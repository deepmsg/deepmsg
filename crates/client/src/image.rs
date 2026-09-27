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
    ) -> io::Result<Self> {
        Ok(Self {
            registration_id,
            session_id,
            stream_id,
            subscriber_position_id,
            log: LogBuffer::open(path, false)?,
            position: Position::from_raw(join_position),
        })
    }

    /// The publication's registration id.
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

    /// How far this reader has consumed.
    pub const fn position(&self) -> i64 {
        self.position.raw()
    }

    /// Set the reader position without reading anything — how a subscriber
    /// joins a stream that is already running.
    pub const fn set_position(&mut self, position: i64) {
        self.position = Position::from_raw(position);
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

        while fragments < fragment_limit {
            let term_begin = self.position.term_begin(geometry.bits_to_shift);
            let term_end = Position::from_raw(term_begin.raw() + geometry.term_length as i64);

            let Some(term) = self.log.term(term_begin.index(geometry.bits_to_shift)) else {
                break;
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
                    // A padding frame means the producer ran out of term, so
                    // the rest of this term is consumed whatever it contains.
                    Step::Padding { .. } => {
                        next = term_end;
                        break;
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
                // either way looping again would spin.
                break;
            }

            self.position = next;
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

/// The frame bits a caller is most likely to test, re-exported so a handler
/// does not have to reach into `deepmsg_core`.
pub use frame::{FLAG_BEGIN, FLAG_END, FLAG_UNFRAGMENTED};
