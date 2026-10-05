//! The subscriber's end of one log buffer.
//!
//! An image is what a subscription gets when `ON_AVAILABLE_IMAGE` says a
//! publication it matches now exists. It names the **publisher's** log file,
//! and the subscriber maps the same pages the producer writes — so for IPC the
//! payload never passes through the driver, and the only thing the two
//! processes share through it is the flow-control counter this type's reader
//! position feeds.
//!
//! Mirrors the read loop of `aeron-client/src/main/c/aeron_image.c:246-325`.
//!
//! # One reader position, two writers' expectations
//!
//! The driver reads this image's subscriber-position counter every duty cycle to
//! work out `min_sub_pos`, which is what raises the publisher's window limit
//! (`aeron_ipc_publication.c:296-313`). So a subscriber that maps the file and
//! reads it but never advances the counter does not merely lag — it eventually
//! **blocks the publisher**, which is the one failure in this slice that looks
//! like someone else's bug.
//!
//! The counter is advanced **after** the handler has returned, never before:
//! the position means "everything below this has been consumed", and a handler
//! that has not run yet has not consumed anything.
//!
//! # What is not here
//!
//! Reassembly. A fragment is delivered as it lies in the term, flags and all,
//! exactly as the reference's `aeron_image_poll` hands it to a fragment
//! handler; putting fragments back into messages is the fragment assembler's
//! job and a separate layer.

use std::io;
use std::path::Path;

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly};
use deepmsg_core::logbuffer::frame::Frame;
use deepmsg_core::logbuffer::position::{self, Position};
use deepmsg_core::logbuffer::scan::{Scanner, Step};
use deepmsg_core::logbuffer::{descriptor, frame};

use crate::fragment_assembler::Action;
use crate::log_buffer::LogBuffer;

/// One fragment, as it lies in the term buffer.
///
/// A window onto the mapped term, not a copy: reading the payload allocates
/// nothing, which is the whole point of the shared-memory path.
pub struct Fragment<'a> {
    frame: Frame<'a, ReadOnly>,
    position: i64,
}

impl<'a> Fragment<'a> {
    /// A fragment over one frame.
    ///
    /// The scanner inside [`Image::poll`] is what builds these in production;
    /// this exists for the tests of the pieces that consume fragments and need
    /// frames of their own — the assembler's tests write them into a term the
    /// test owns.
    #[cfg(test)]
    pub(crate) const fn new(frame: Frame<'a, ReadOnly>, position: i64) -> Self {
        Self { frame, position }
    }

    /// Where this fragment begins in the stream.
    pub const fn position(&self) -> i64 {
        self.position
    }

    /// How many payload bytes it carries.
    pub fn payload_length(&self) -> usize {
        self.frame.payload_length().unwrap_or(0)
    }

    /// The `BEGIN`/`END`/`EOS`/`REVOKED` bits.
    pub fn flags(&self) -> Option<u8> {
        self.frame.flags()
    }

    /// Whether this fragment is a whole message.
    pub fn is_unfragmented(&self) -> bool {
        self.frame.is_unfragmented()
    }

    /// The session whose publication wrote it. This is the key the fragment
    /// assembler reassembles by: one stream can be carried by two publications
    /// at once, and their fragments must not be assembled together.
    pub fn session_id(&self) -> Option<i32> {
        self.frame.session_id()
    }

    /// The stream it belongs to.
    pub fn stream_id(&self) -> Option<i32> {
        self.frame.stream_id()
    }

    /// Where it begins in its term.
    pub fn term_offset(&self) -> Option<i32> {
        self.frame.term_offset()
    }

    /// The length of the frame itself, header included.
    pub fn frame_length(&self) -> Option<i32> {
        self.frame.frame_length()
    }

    /// Where the **next** fragment of this message would begin
    /// (`aeron_header_next_term_offset`,
    /// `aeron-client/src/main/c/aeron_subscription.c:587-593`).
    ///
    /// That is the continuity test the assembler makes: a fragment whose term
    /// offset is not this is a fragment whose predecessor is missing, and the
    /// message it was part of can never be completed.
    pub fn next_term_offset(&self) -> Option<i32> {
        let term_offset = self.term_offset()?;
        let length = self.frame_length()?;

        // Checked, because this is arithmetic on a field another process
        // wrote: a frame whose length does not add up is not a fragment this
        // build can place, and saying so is better than wrapping into a
        // plausible offset.
        let end = term_offset.checked_add(length)?;

        end.checked_add(descriptor::FRAME_ALIGNMENT - 1)
            .map(|value| value & !(descriptor::FRAME_ALIGNMENT - 1))
    }

    /// Copy the payload out, which is what a handler almost always does.
    ///
    /// `None` if `dst` is not exactly [`Fragment::payload_length`] bytes, or if
    /// the frame is not complete — which a delivered fragment always is.
    pub fn copy_payload(&self, dst: &mut [u8]) -> Option<()> {
        self.frame.copy_payload(dst)
    }
}

impl std::fmt::Debug for Fragment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fragment")
            .field("position", &self.position)
            .field("payload_length", &self.payload_length())
            .field("flags", &self.flags())
            .finish()
    }
}

/// An image: a publication's log buffer, read from the subscriber's side.
pub struct Image {
    /// The **publication's** registration id. Not this subscriber's.
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    /// The counter this image's reader advances.
    subscriber_position_id: i32,
    /// Where this subscriber started reading — the position the driver wrote
    /// into the counter when it linked the subscription
    /// (`aeron_driver_conductor.c:3547-3575`), read back here rather than taken
    /// from the message that announced the image.
    ///
    /// `Image.joinPosition()` (`Image.java:214`);
    /// `aeron_image_constants_t.join_position` (`aeronc.h:2140`).
    join_position: i64,
    /// Where the stream comes from, as the driver described it: `host:port` for
    /// a network publication and `"aeron:ipc"` for an IPC one
    /// (`Image.sourceIdentity()`, `Image.java:154`;
    /// `aeron_image_constants_t.source_identity`, `aeronc.h:2130`).
    ///
    /// It arrives in `ON_AVAILABLE_IMAGE`, whose tail carries it after the log
    /// path (`on_available_image`, `aeron_driver_conductor.c:1327-1336`).
    source_identity: String,
    log: LogBuffer,
    /// How far this reader has consumed.
    position: Position,
}

impl Image {
    /// Map the log buffer `ON_AVAILABLE_IMAGE` named.
    ///
    /// `join_position` is where this subscription started reading; the driver
    /// has already written it into the counter, and this reads it back rather
    /// than taking the caller's word for it.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file is not there or does not describe a usable
    /// log. The reference maps images read-write; this maps them **read-only**,
    /// because a subscriber has no business writing into the publisher's
    /// buffer, and a read-only `MAP_SHARED` mapping sees the writer's stores
    /// just the same.
    pub fn open(
        path: &Path,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        subscriber_position_id: i32,
        join_position: i64,
        source_identity: String,
    ) -> io::Result<Self> {
        Ok(Self {
            registration_id,
            session_id,
            stream_id,
            subscriber_position_id,
            join_position,
            source_identity,
            log: LogBuffer::open(path, false)?,
            position: Position::from_raw(join_position),
        })
    }

    /// The **publication's** registration id — not this subscriber's
    /// (`aeron_image_t.correlation_id`, `aeron_image.h:28`; `Image.correlationId()`,
    /// `Image.java:184`).
    ///
    /// It is what [`crate::client::Client::reject_image`] takes, and what a
    /// [`PublicationErrorEvent`](crate::publication_error::PublicationErrorEvent)
    /// about this image's stream will name — which is why the two ids must not
    /// be confused: handing the subscriber's id to a rejection names nothing.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The publication's session id.
    pub const fn session_id(&self) -> i32 {
        self.session_id
    }

    /// The publication's stream id.
    pub const fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /// The counter this reader must advance, and that the publisher's window
    /// limit is computed from.
    pub const fn subscriber_position_id(&self) -> i32 {
        self.subscriber_position_id
    }

    /// The mapped log.
    pub const fn log(&self) -> &LogBuffer {
        &self.log
    }

    /// How far this **reader** has consumed — not how far the publisher has
    /// written (`aeron_image_position`, `aeron_image.c:735-738`;
    /// `Image.position()`, `Image.java:224`).
    ///
    /// An image that has read nothing reports nothing, however much is in the
    /// term. The reference rejects an image at this position and not at the
    /// publisher's for that reason: what travels to the far end is where the
    /// stream was cut off, which is where its reader stopped.
    pub const fn position(&self) -> i64 {
        self.position.raw()
    }

    /// Set the reader position without reading anything — how a subscriber
    /// joins a stream that is already running.
    pub const fn set_position(&mut self, position: i64) {
        self.position = Position::from_raw(position);
    }

    /// Where this subscriber joined the stream
    /// (`Image.joinPosition()`, `Image.java:214`;
    /// `aeron_image_constants_t.join_position`, `aeronc.h:2140`).
    pub const fn join_position(&self) -> i64 {
        self.join_position
    }

    /// Where the stream comes from as the driver described it — `host:port` for
    /// a network publication, `"aeron:ipc"` for an IPC one
    /// (`Image.sourceIdentity()`, `Image.java:154`).
    ///
    /// Two images of the same stream from different sources differ here, which
    /// is what it is for.
    pub fn source_identity(&self) -> &str {
        &self.source_identity
    }

    /// The MTU the publication was created with (`Image.mtuLength()`,
    /// `Image.java:164`; `LogBufferDescriptor.mtuLength`,
    /// `LogBufferDescriptor.java:527`).
    ///
    /// `None` only when the metadata block cannot be read, which it always can
    /// for a log this type opened.
    pub fn mtu_length(&self) -> Option<i32> {
        self.metadata()?.load_i32(descriptor::MTU_LENGTH_OFFSET)
    }

    /// The position the stream reached when the publisher signalled end of
    /// stream — [`END_OF_STREAM_OPEN`](deepmsg_core::logbuffer::descriptor::END_OF_STREAM_OPEN)
    /// until then (`Image.endOfStreamPosition()`, `Image.java:265`;
    /// `aeron_image_end_of_stream_position`, `aeron_image.c:195-208`).
    pub fn end_of_stream_position(&self) -> Option<i64> {
        self.metadata()?
            .load_i64(descriptor::END_OF_STREAM_POSITION_OFFSET)
    }

    /// Whether this reader has reached the end of the stream
    /// (`Image.isEndOfStream()`, `Image.java:249`).
    ///
    /// The reference compares the **subscriber position counter** against the
    /// metadata; this compares the reader's own position against it, which is
    /// the same number — this client writes that counter after every poll. Both
    /// are false while the stream is open, because the open marker is
    /// `INT64_MAX` and no position reaches it.
    pub fn is_end_of_stream(&self) -> Option<bool> {
        Some(self.position.raw() >= self.end_of_stream_position()?)
    }

    /// How many transports the driver has seen active within the image liveness
    /// timeout; zero for an IPC image, which has no transports
    /// (`Image.activeTransportCount()`, `Image.java:283`).
    pub fn active_transport_count(&self) -> Option<i32> {
        self.metadata()?
            .load_i32(descriptor::ACTIVE_TRANSPORT_COUNT_OFFSET)
    }

    /// Whether the publication behind this image has been revoked
    /// (`Image.isPublicationRevoked()`, `Image.java:298`;
    /// `aeron_image_is_publication_revoked`, `aeron_image.c:231-247`).
    pub fn is_publication_revoked(&self) -> Option<bool> {
        self.metadata()?
            .load_u8(descriptor::IS_PUBLICATION_REVOKED_OFFSET)
            .map(|value| value != 0)
    }

    /// The log's metadata block, which is where the four state questions above
    /// are answered (`crate::log_buffer`).
    fn metadata(&self) -> Option<AtomicBuffer<'_>> {
        self.log.metadata()
    }

    /// Read up to `fragment_limit` fragments, handing each to `handler`.
    ///
    /// Returns how many were delivered. The reader position advances past
    /// exactly those fragments, and the caller publishes it to the counter
    /// afterwards — see [`crate::Client::poll_image`], which does.
    ///
    /// A `fragment_limit` of zero reads nothing, matching the reference. The
    /// caller must pass a positive limit or make no progress.
    pub fn poll<F>(&mut self, fragment_limit: usize, mut handler: F) -> usize
    where
        F: FnMut(&Fragment<'_>),
    {
        let geometry = self.log.geometry();
        let term_length = geometry.term_length as usize;
        let mut fragments = 0;

        // **One term per call.** The partition index is fixed here, at the
        // entry, and the scan below stops at that term's end; the advance into
        // the next term happens on the *next* call, where the position already
        // names it (`aeron_image.c:266-273` does the same, and the Java client
        // with it). A poll that crossed terms would deliver more fragments than
        // the reference's for the same state, which is visible to any caller
        // that throttles by counting them — `docs/compat.md` has the row, and
        // the test is `a_poll_reads_one_term_at_a_time`.
        let term_begin = self.position.term_begin(geometry.bits_to_shift);
        let term_end = Position::from_raw(term_begin.raw() + geometry.term_length as i64);

        let Some(term) = self.log.term(term_begin.index(geometry.bits_to_shift)) else {
            return fragments;
        };
        let offset = (self.position.raw() - term_begin.raw()) as usize;

        let mut scanner = Scanner::at(&term, term_length, offset);
        let mut next = self.position;

        loop {
            match scanner.advance() {
                Step::Data {
                    offset,
                    frame_length,
                } => {
                    handler(&Fragment {
                        frame: Frame::new(&term, offset),
                        position: next.raw(),
                    });
                    fragments += 1;

                    let aligned = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
                    next = Position::from_raw(next.raw() + i64::from(aligned));

                    if fragments >= fragment_limit {
                        break;
                    }
                }
                // A padding frame covers the bytes it covers, and the scan
                // steps over it (`aeron_image.c:375-379`, and Java's
                // `Image.java:357-359`) — which is the only handling that reads
                // **both** shapes of padding correctly.
                //
                // A producer's tail padding reaches exactly to the term's end,
                // so stepping over it ends the term anyway and this is what the
                // old `next = term_end` did. A **repaired hole** does not: an
                // image that fills a gap because its channel said
                // `reliable=false` leaves a padding frame in the middle of a
                // term, with frames the reader has not seen on the other side
                // of it. Jumping to the term's end threw those away — silently,
                // and only ever on an unreliable stream, which is why nothing
                // noticed until one was read end to end.
                Step::Padding { frame_length, .. } => {
                    let aligned = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
                    next = Position::from_raw(next.raw() + i64::from(aligned));
                }
                // The scan reached the term's end because the previous
                // frame ended exactly on the boundary, with no padding
                // frame to mark it. Not an error, and not "no data": the
                // term is simply done.
                Step::End => {
                    next = term_end;
                    break;
                }
                // A claimed-but-unpublished frame, or a length no writer
                // could have produced. Both stop the scan where they are:
                // the position does not move, and the caller comes back.
                Step::NotReady { .. } | Step::Malformed { .. } => break,
            }
        }

        if next.raw() <= self.position.raw() {
            // No progress. Either the term is not ready or it is empty;
            // either way the caller comes back.
            return fragments;
        }

        self.position = next;

        fragments
    }

    /// The same scan, with the caller answering for each fragment.
    ///
    /// The answer decides where the reader's position ends up, which is the
    /// whole of what the controlled face adds to [`Image::poll`]:
    ///
    /// * **`Abort`** — the fragment is not consumed, so the position does not
    ///   reach it and the same fragment arrives again next time. The reference
    ///   walks its offset back by the frame it had already stepped over
    ///   (`aeron_image.c`, the `AERON_ACTION_ABORT` arm); this walks `next`
    ///   back, which is the same statement about a position.
    /// * **`Break`** — consumed, and the scan stops here.
    /// * **`Commit`** — consumed, and the position is **published now**, so a
    ///   later refusal cannot take it back. The reference publishes inside the
    ///   loop for the same reason, and that is what makes `Commit` different
    ///   from `Continue` rather than merely more eager: what it buys is a
    ///   watermark that survives a mistake further on.
    /// * **`Continue`** — consumed; the position is published once, at the end.
    ///
    /// `publish` is called at each commit and once at the end when the scan
    /// moved. It is a `dyn` call, and that is deliberate: commits are rare — a
    /// poller that reads a control response refuses far more than it takes —
    /// and the alternative is threading a counter handle through this type,
    /// which is the arrangement [`crate::Client`] already owns.
    pub(crate) fn controlled_poll<H>(
        &mut self,
        fragment_limit: usize,
        handler: &mut H,
        publish: &mut dyn FnMut(i64),
    ) -> usize
    where
        H: ControlledFragments,
    {
        let geometry = self.log.geometry();
        let term_length = geometry.term_length as usize;

        let term_begin = self.position.term_begin(geometry.bits_to_shift);
        let term_end = Position::from_raw(term_begin.raw() + geometry.term_length as i64);

        let Some(term) = self.log.term(term_begin.index(geometry.bits_to_shift)) else {
            return 0;
        };
        let offset = (self.position.raw() - term_begin.raw()) as usize;

        let mut scanner = Scanner::at(&term, term_length, offset);
        let mut ledger = PositionLedger::new(self.position);
        let mut fragments = 0;

        loop {
            match scanner.advance() {
                Step::Data {
                    offset,
                    frame_length,
                } => {
                    let action = handler.on_fragment(&Fragment {
                        frame: Frame::new(&term, offset),
                        position: ledger.next.raw(),
                    });

                    if !action.consumes() {
                        ledger.refuse();
                        break;
                    }

                    let aligned = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
                    ledger.consume(i64::from(aligned));
                    fragments += 1;

                    if action.publishes_now() {
                        publish(ledger.commit());
                    }

                    if action.stops() || fragments >= fragment_limit {
                        break;
                    }
                }
                Step::Padding { frame_length, .. } => {
                    // A padding frame moves the position without being a
                    // fragment, so it never spends the budget — which is what
                    // keeps a repaired hole from costing a reader a message.
                    let aligned = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
                    ledger.consume(i64::from(aligned));
                }
                Step::End => {
                    ledger.next = term_end;
                    break;
                }
                Step::NotReady { .. } | Step::Malformed { .. } => break,
            }
        }

        if let Some(position) = ledger.settled() {
            publish(position);
        }

        if ledger.committed.raw() > self.position.raw() {
            self.position = ledger.committed;
        }

        fragments
    }

    /// The term buffer regions, for a test or a tool that wants to look at the
    /// raw bytes rather than scan them.
    pub fn term(&self, partition: usize) -> Option<AtomicBuffer<'_>> {
        self.log.term(partition)
    }
}

impl std::fmt::Debug for Image {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Image")
            .field("registration_id", &self.registration_id)
            .field("session_id", &self.session_id)
            .field("stream_id", &self.stream_id)
            .field("subscriber_position_id", &self.subscriber_position_id)
            .field("position", &self.position.raw())
            .finish()
    }
}

/// Where a controlled scan's position stands, as the caller's answers move it.
///
/// Split out from the scan because this is the part with rules in it — the
/// reference's four actions and what each does to a reader's position
/// (`aeron_image.c`, the action arms of `aeron_image_controlled_poll`) — and
/// the part that can be checked without a term buffer to read. The scan owns
/// the frames; this owns the arithmetic.
#[derive(Clone, Copy, Debug)]
struct PositionLedger {
    /// Where the scan has read up to.
    next: Position,
    /// The last point that was published, and the point a refusal falls back
    /// to. It starts where the reader started: a refusal with nothing committed
    /// before it leaves the reader where it was.
    committed: Position,
}

impl PositionLedger {
    const fn new(start: Position) -> Self {
        Self {
            next: start,
            committed: start,
        }
    }

    /// A consumed fragment moves the scan on by its aligned length.
    fn consume(&mut self, aligned: i64) {
        self.next = Position::from_raw(self.next.raw() + aligned);
    }

    /// `Commit`: this point is published now and a later refusal cannot pass it.
    fn commit(&mut self) -> i64 {
        self.committed = self.next;
        self.next.raw()
    }

    /// `Abort`: the fragment is not consumed, and the position falls back to
    /// the last committed point rather than to where the scan began.
    fn refuse(&mut self) {
        self.next = self.committed;
    }

    /// Where the scan ended up, if it moved past the last thing published.
    fn settled(&mut self) -> Option<i64> {
        if self.next.raw() > self.committed.raw() {
            self.committed = self.next;
            return Some(self.next.raw());
        }
        None
    }
}

/// What a controlled scan calls for each fragment, and what it answers.
///
/// Fragment-shaped rather than message-shaped, because that is the level this
/// scan reads at: the reassembly from fragments to messages happens one layer
/// up, and [`crate::Client`]'s controlled poll puts it there.
pub trait ControlledFragments {
    /// Say what the scan should do with this fragment.
    fn on_fragment(&mut self, fragment: &Fragment<'_>) -> Action;
}

/// The frame bits a caller is most likely to test, re-exported so a handler
/// does not have to reach into `deepmsg_core`.
pub use frame::{FLAG_BEGIN, FLAG_END, FLAG_UNFRAGMENTED};

#[cfg(test)]
mod ledger_tests {
    use super::{Position, PositionLedger};

    fn at(raw: i64) -> PositionLedger {
        PositionLedger::new(Position::from_raw(raw))
    }

    /// The four answers, as positions. This is the table the plan calls the
    /// acceptance for this slice, and it is here rather than in a scan because
    /// a scan needs a term buffer and this needs only the arithmetic.
    #[test]
    fn the_four_actions_move_the_position_where_the_reference_moves_it() {
        // `Continue`: consumed, and published once at the end.
        let mut ledger = at(100);
        ledger.consume(64);
        ledger.consume(64);
        assert_eq!(
            Some(228),
            ledger.settled(),
            "the end publishes what was read"
        );
        assert_eq!(None, ledger.settled(), "and only once");

        // `Break`: the same, because a break consumes what it stopped on.
        let mut ledger = at(100);
        ledger.consume(64);
        assert_eq!(Some(164), ledger.settled());

        // `Commit`: published there and then, so the end has nothing left.
        let mut ledger = at(100);
        ledger.consume(64);
        assert_eq!(164, ledger.commit());
        assert_eq!(None, ledger.settled(), "the end adds nothing to a commit");
    }

    /// What `Commit` buys over `Continue`: a point a later refusal cannot take
    /// back. Without this the two actions would be the same action.
    ///
    /// The difference is in what was **published**, not in what is left to
    /// publish — after the refusal neither ledger has anything more to say, and
    /// only one of them has already said 164.
    #[test]
    fn a_committed_point_survives_a_refusal_after_it() {
        let mut published = Vec::new();

        let mut committed = at(100);
        committed.consume(64);
        published.push(committed.commit());
        committed.consume(128);
        committed.refuse();
        if let Some(position) = committed.settled() {
            published.push(position);
        }
        assert_eq!(vec![164], published, "the refused fragment is given back");

        // The same reads with no commit among them publish nothing at all.
        let mut uncommitted = at(100);
        uncommitted.consume(64);
        uncommitted.consume(128);
        uncommitted.refuse();
        assert_eq!(
            None,
            uncommitted.settled(),
            "nothing was committed, nothing moved"
        );
    }

    /// A refusal as the first answer leaves the reader exactly where it was —
    /// a poll that reads a message it will not take has not read anything.
    #[test]
    fn a_refusal_with_nothing_committed_moves_nothing() {
        let mut ledger = at(100);
        ledger.refuse();
        assert_eq!(None, ledger.settled());
    }
}
