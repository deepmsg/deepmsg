//! The producer's end of one log buffer.
//!
//! A client that has sent `ADD_PUBLICATION` and received `ON_PUBLICATION_READY`
//! holds a path and a counter id. It maps the file, reads the shape from the
//! metadata the driver wrote, and offers by appending frames — for an IPC
//! channel the subscriber is reading the very same pages, and nothing goes
//! through the driver.
//!
//! Mirrors `aeron-client/src/main/c/aeron_publication.c:459-540`.
//!
//! # What a producer does not do
//!
//! It does not create the log file, does not write `is_connected`, does not
//! write `end_of_stream_position`, and does not write the `pub-pos` counter —
//! all of those are the driver's (`aeron_ipc_publication.c:278-325`). The only
//! things a producer owns are the current term's tail counter, by get-and-add,
//! and the bytes of the frames it writes into.
//!
//! # The window is a position, not a remainder
//!
//! The limit counter holds an **absolute position** and the test is
//! `position < limit` (`aeron_publication.c:498`). Reading it as "bytes left"
//! gives a plausible answer that is wrong by the whole stream length.

use std::io;
use std::path::Path;

use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};
use deepmsg_core::logbuffer::append::{Appended, Appender};
use deepmsg_core::logbuffer::frame::Frame;
use deepmsg_core::logbuffer::{descriptor, position};

use crate::log_buffer::LogBuffer;

/// A publication: a log buffer this client writes into.
pub struct Publication {
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    position_limit_counter_id: i32,
    channel_status_indicator_id: i32,
    log: LogBuffer,
}

impl Publication {
    /// Map the log buffer `ON_PUBLICATION_READY` named.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file is not there or does not describe a usable
    /// log. It is there by then: the driver creates it before sending the
    /// response.
    pub fn open(
        path: &Path,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        position_limit_counter_id: i32,
        channel_status_indicator_id: i32,
    ) -> io::Result<Self> {
        Ok(Self {
            registration_id,
            session_id,
            stream_id,
            position_limit_counter_id,
            channel_status_indicator_id,
            log: LogBuffer::open(path, true)?,
        })
    }

    /// The id the driver keys this publication by.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The session id the driver allocated. Random per publication unless the
    /// channel asked for one, and echoed in every frame.
    pub const fn session_id(&self) -> i32 {
        self.session_id
    }

    /// The stream id.
    pub const fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /// The counter holding this publication's window limit. The driver is its
    /// only writer; a producer reads it before every offer.
    pub const fn position_limit_counter_id(&self) -> i32 {
        self.position_limit_counter_id
    }

    /// The channel-status counter, or
    /// [`deepmsg_cnc::command::CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`].
    pub const fn channel_status_indicator_id(&self) -> i32 {
        self.channel_status_indicator_id
    }

    /// The mapped log.
    pub const fn log(&self) -> &LogBuffer {
        &self.log
    }

    /// Whether the driver says a subscriber is attached.
    ///
    /// The byte is the driver's, in the log's own metadata
    /// (`LogBufferDescriptor.IS_CONNECTED_OFFSET`), and it is the same byte for
    /// both kinds of publication — the append view reads it, but an
    /// **exclusive** publication has no append view and still has the byte.
    pub fn is_connected(&self) -> Option<bool> {
        self.log().is_connected()
    }

    /// Append `payload` as one frame, if `position_limit` allows it.
    ///
    /// The limit is passed in rather than read here because the counter lives
    /// in the CnC file, which this type does not own — see
    /// [`crate::Client::offer`], which reads it and calls this.
    ///
    /// # Errors
    ///
    /// See [`Appended`]. Two outcomes are "try again" rather than "no":
    /// `EndOfLog` (the log rotated and the caller retries into the new term)
    /// and `MidRotation` (another producer is rotating right now, so the caller
    /// retries once it has settled).
    pub fn offer(&self, position_limit: i64, payload: &[u8]) -> Appended {
        let Some(appender) = self.appender() else {
            return Appended::Malformed;
        };

        appender.append(self.session_id, self.stream_id, position_limit, payload)
    }

    /// The largest payload one frame can carry on this log.
    pub fn max_payload_length(&self) -> Option<usize> {
        self.appender()
            .map(|appender| appender.max_payload_length())
    }

    /// A fresh appender over the current term.
    ///
    /// Built per offer rather than held: an `Appender` borrows the mapping, so
    /// storing one would make this type self-referential. The cost is two
    /// metadata reads, which is what the reference's own offer does anyway —
    /// it re-reads `active_term_count` per call rather than caching it.
    fn appender(&self) -> Option<Appender<'_>> {
        let metadata = self.log.file().region_mut(
            self.log.geometry().metadata_offset,
            descriptor::METADATA_LENGTH,
        )?;

        // The partition comes from `active_term_count`, which is why it has to
        // be read before the tail: after a rotation the count and the tail
        // disagree for a moment, and the count is the one that is right.
        let term_count = metadata.load_i32(descriptor::ACTIVE_TERM_COUNT_OFFSET)?;
        let partition = position::index_by_term_count(term_count);
        let term = self.log.term_mut(partition)?;

        Appender::new(metadata, term)
    }
}

impl std::fmt::Debug for Publication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publication")
            .field("registration_id", &self.registration_id)
            .field("session_id", &self.session_id)
            .field("stream_id", &self.stream_id)
            .field("position_limit_counter_id", &self.position_limit_counter_id)
            .field("log", &self.log)
            .finish()
    }
}

/// An **exclusive** publication: one producer, and the offset is its own.
///
/// Mirrors `aeron-client/src/main/c/aeron_exclusive_publication.c`. The type is
/// separate from [`Publication`] because the two differ in a way the compiler
/// should keep apart: a concurrent producer takes its place in the term from a
/// claim, an exclusive one keeps it
/// (`publication->term_offset`, `aeron_exclusive_publication.c:558`), and only
/// the exclusive one has `try_claim` / `append_padding` / `offer_block`.
///
/// # The offset is a cache, and the log is the authority
///
/// [`ExclusivePublication::open`] seeds the pair from the log's **current
/// tail** (`:451-459`, which reads `active_term_count` and the tail counter it
/// names), not from zero — a publication that resumes a log starts where the
/// log is. From then on the pair is moved only when the log rotates, and the
/// rotation is the log's: an [`Appended::EndOfLog`] means the append rotated
/// it, and this re-reads rather than computes.
pub struct ExclusivePublication {
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    position_limit_counter_id: i32,
    channel_status_indicator_id: i32,
    log: LogBuffer,
    /// The term the producer believes it is in, and where in it — the two
    /// numbers the reference keeps on the publication object.
    ///
    /// `Cell` because the reference's are plain fields moved by a `&self`
    /// method too, and because an exclusive producer has exactly one caller:
    /// the interior mutability here is the "one producer" contract, not a
    /// concession to sharing.
    term_id: std::cell::Cell<i32>,
    term_offset: std::cell::Cell<i32>,
}

impl ExclusivePublication {
    /// Map the log buffer `ON_EXCLUSIVE_PUBLICATION_READY` named, and take the
    /// position it is already at.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file is not there or does not describe a usable
    /// log, which the driver would have failed on first.
    pub fn open(
        path: &Path,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        position_limit_counter_id: i32,
        channel_status_indicator_id: i32,
    ) -> io::Result<Self> {
        let publication = Self {
            registration_id,
            session_id,
            stream_id,
            position_limit_counter_id,
            channel_status_indicator_id,
            log: LogBuffer::open(path, true)?,
            term_id: std::cell::Cell::new(0),
            term_offset: std::cell::Cell::new(0),
        };

        publication.seed_from_log();

        Ok(publication)
    }

    /// The id the driver keys this publication by.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The session it publishes under, as the driver named it.
    pub const fn session_id(&self) -> i32 {
        self.session_id
    }

    /// The stream it publishes on.
    pub const fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /// The counter whose value is the window's upper bound.
    pub const fn position_limit_counter_id(&self) -> i32 {
        self.position_limit_counter_id
    }

    /// The counter whose value is this channel's status.
    pub const fn channel_status_indicator_id(&self) -> i32 {
        self.channel_status_indicator_id
    }

    /// The mapped log.
    pub const fn log(&self) -> &LogBuffer {
        &self.log
    }

    /// The term this producer believes it is in.
    pub fn term_id(&self) -> i32 {
        self.term_id.get()
    }

    /// Where in that term it believes it is.
    pub fn term_offset(&self) -> i32 {
        self.term_offset.get()
    }

    /// Whether the driver says a subscriber is attached.
    ///
    /// Read from the log's metadata, as the concurrent one is: the byte is the
    /// driver's and both kinds of publication have it.
    pub fn is_connected(&self) -> Option<bool> {
        self.log().is_connected()
    }

    /// The largest payload one frame can carry on this log.
    pub fn max_payload_length(&self) -> Option<usize> {
        self.appender()
            .map(|appender| appender.max_payload_length())
    }

    /// Append `payload` as one frame, if `position_limit` allows it.
    ///
    /// # Errors
    ///
    /// See [`Appended`]. [`Appended::EndOfLog`] is the one that moves this
    /// publication: the append rotated the log, so the cached pair is re-read
    /// before returning and the caller's retry lands in the new term.
    pub fn offer(&self, position_limit: i64, payload: &[u8]) -> Appended {
        let Some(appender) = self.appender() else {
            return Appended::Malformed;
        };

        let outcome = appender.append_exclusive(
            self.session_id,
            self.stream_id,
            position_limit,
            self.term_id.get(),
            self.term_offset.get(),
            payload,
        );

        match outcome {
            Appended::EndOfLog => self.seed_from_log(),
            Appended::Ok { position, .. } => self.advance_to(position),
            _ => {}
        }

        outcome
    }

    /// Claim a frame of `length` **payload** bytes and hand it back unwritten.
    ///
    /// The caller writes into [`Claim::frame`] and commits it with
    /// [`Frame::publish`].
    ///
    /// # Errors
    ///
    /// See [`Appended`]. `EndOfLog` moves this publication's position, as it
    /// does for [`Self::offer`].
    pub fn try_claim(&self, position_limit: i64, length: usize) -> Result<Claim<'_>, Appended> {
        // The frame starts where this publication says it does; the claim
        // answers with where it **ends**, which is what moves the cache.
        let offset = usize::try_from(self.term_offset.get()).unwrap_or(0);

        {
            let Some(appender) = self.appender() else {
                return Err(Appended::Malformed);
            };

            match appender.try_claim_exclusive(
                self.session_id,
                self.stream_id,
                position_limit,
                self.term_id.get(),
                self.term_offset.get(),
                length,
            ) {
                Ok(position) => self.advance_to(position),
                Err(error) => {
                    if error == Appended::EndOfLog {
                        self.seed_from_log();
                    }

                    return Err(error);
                }
            }
        }

        // A **second** view of the same term, because the frame outlives the
        // call and the appender's view does not. The reference has two here as
        // well: its claim returns a pointer into a term it has already moved
        // the tail of.
        let Some(partition) = self.term_partition() else {
            return Err(Appended::Malformed);
        };

        let Some(term) = self.log.term_mut(partition) else {
            return Err(Appended::Malformed);
        };

        Ok(Claim { term, offset })
    }

    /// The partition `active_term_count` names.
    fn term_partition(&self) -> Option<usize> {
        let metadata = self.log.file().region_mut(
            self.log.geometry().metadata_offset,
            descriptor::METADATA_LENGTH,
        )?;

        metadata
            .load_i32(descriptor::ACTIVE_TERM_COUNT_OFFSET)
            .map(position::index_by_term_count)
    }

    /// Append a padding frame of `length` **payload** bytes.
    ///
    /// # Errors
    ///
    /// See [`Appended`].
    pub fn append_padding(&self, position_limit: i64, length: usize) -> Appended {
        let Some(appender) = self.appender() else {
            return Appended::Malformed;
        };

        let outcome = appender.append_padding_exclusive(
            self.session_id,
            self.stream_id,
            position_limit,
            self.term_id.get(),
            self.term_offset.get(),
            length,
        );

        match outcome {
            Appended::EndOfLog => self.seed_from_log(),
            Appended::Ok { position, .. } => self.advance_to(position),
            _ => {}
        }

        outcome
    }

    /// Append a frame this caller built elsewhere
    /// (`aeron_exclusive_publication_offer_block`, `:810-895`).
    ///
    /// # Errors
    ///
    /// See [`Appended`]. A block that does not fit the term, or whose header
    /// does not name this stream at this offset, is refused.
    pub fn offer_block(&self, position_limit: i64, block: &[u8]) -> Appended {
        let Some(appender) = self.appender() else {
            return Appended::Malformed;
        };

        let outcome = appender.append_block_exclusive(
            self.session_id,
            self.stream_id,
            position_limit,
            self.term_id.get(),
            self.term_offset.get(),
            block,
        );

        match outcome {
            Appended::EndOfLog => self.seed_from_log(),
            Appended::Ok { position, .. } => self.advance_to(position),
            _ => {}
        }

        outcome
    }

    /// Move the cached pair to where an append left the log
    /// (`aeron_exclusive_publication_new_position`,
    /// `aeron_exclusive_publication.h:99-108`: `term_offset = resulting_offset`
    /// and the position follows from it).
    ///
    /// The **end** position, not the frame's: what the next append needs is
    /// where this one stopped. A publication that kept the frame's offset would
    /// write every message over the first — which is not a subtle failure, it
    /// is a stream of one message that looks like it is being sent N times.
    fn advance_to(&self, position: deepmsg_core::logbuffer::position::Position) {
        let Some(term_length) = self.appender().map(|appender| appender.term_length()) else {
            return;
        };
        let Some(bits) = position::bits_to_shift(term_length) else {
            return;
        };
        let Some(initial_term_id) = self.appender().map(|appender| appender.initial_term_id())
        else {
            return;
        };

        self.term_id.set(position.term_id(bits, initial_term_id));
        self.term_offset.set(position.term_offset(bits));
    }

    /// Re-read the cached pair from the log's current tail
    /// (`aeron_exclusive_publication.c:451-459`).
    ///
    /// The raw offset, not the saturating one: a tail that has run to the end
    /// of a term says so, and zero and `term_length` are different answers.
    fn seed_from_log(&self) {
        let Some(appender) = self.appender() else {
            return;
        };

        let Some(tail) = appender.current_tail() else {
            return;
        };

        self.term_id.set(tail.term_id());

        #[allow(clippy::cast_possible_truncation)] // a term offset
        self.term_offset.set(tail.raw_term_offset() as i32);
    }

    /// A fresh appender over the current term, as [`Publication`] builds one
    /// and for the same reason.
    fn appender(&self) -> Option<Appender<'_>> {
        let metadata = self.log.file().region_mut(
            self.log.geometry().metadata_offset,
            descriptor::METADATA_LENGTH,
        )?;

        let term_count = metadata.load_i32(descriptor::ACTIVE_TERM_COUNT_OFFSET)?;
        let partition = position::index_by_term_count(term_count);
        let term = self.log.term_mut(partition)?;

        Appender::new(metadata, term)
    }
}

impl std::fmt::Debug for ExclusivePublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExclusivePublication")
            .field("registration_id", &self.registration_id)
            .field("session_id", &self.session_id)
            .field("stream_id", &self.stream_id)
            .field("term_id", &self.term_id)
            .field("term_offset", &self.term_offset)
            .field("log", &self.log)
            .finish()
    }
}

/// A frame an exclusive publication has claimed and not yet published
/// (`aeron_buffer_claim_t`, `aeron_exclusive_publication.h:36-41`, which the
/// claim fills with a header pointer, a payload pointer and a length).
///
/// It **owns** the term view rather than borrowing the publication's: a
/// [`Frame`] borrows the buffer it is built over, and a publication cannot hand
/// out a borrow of a mapping it builds per call. Two views of one mapping is
/// what the reference has too — the claim's pointers are into a term whose tail
/// it has already moved.
pub struct Claim<'a> {
    term: AtomicBuffer<'a, ReadWrite>,
    offset: usize,
}

impl Claim<'_> {
    /// The frame, ready to be written into and published.
    ///
    /// Its length reads **negative** until [`Frame::publish`] is called, which
    /// is what keeps a reader from taking a frame mid-write
    /// (`aeron_exclusive_publication.c:41`).
    pub fn frame(&self) -> Frame<'_, ReadWrite> {
        Frame::new(&self.term, self.offset)
    }

    /// Where in the term the claimed frame starts.
    pub const fn offset(&self) -> usize {
        self.offset
    }
}
