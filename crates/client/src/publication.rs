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
use deepmsg_core::logbuffer::position::Position;
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
    /// Set by [`Publication::revoke_on_close`], read by the removal that gives
    /// this publication back.
    ///
    /// It is the whole difference between the two endings the reference offers:
    /// a quiet close, and one that tells every reader the stream is over
    /// (`ExclusivePublication.revokeOnClose`,
    /// `ExclusivePublication.java:155-158`, and the flag its conductor reads on
    /// the way out at `ClientConductor.java:700-713`).
    revoke_on_close: bool,
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
            revoke_on_close: false,
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

    /// Claim `length` **payload** bytes and write into them without copying.
    ///
    /// The one thing a publication with more than one producer could not do:
    /// [`Publication::offer`] copies the payload into the term, where a claim
    /// hands back a window onto it — Java's `ConcurrentPublication.tryClaim`
    /// (`aeron-client/src/main/java/io/aeron/ConcurrentPublication.java:312`),
    /// and `aeron_publication_try_claim`
    /// (`aeron-client/src/main/c/aeron_publication.c:634-685`) behind it.
    ///
    /// Where [`ExclusivePublication::try_claim`] takes the offset this
    /// publication has been keeping, this one has none to keep: whichever
    /// producer gets there first takes the space, and the claim is what says
    /// where. The window is written to and committed with
    /// [`Claim::frame`]`().publish(..)`, exactly as the exclusive one is.
    ///
    /// # Errors
    ///
    /// See [`Appended`]. `MidRotation` and `EndOfLog` both mean "try again":
    /// another producer is rotating, and the claim landed in the term it is
    /// leaving.
    pub fn try_claim(&self, position_limit: i64, length: usize) -> Result<Claim<'_>, Appended> {
        // Scoped so the appender's borrow of the log ends before the window's
        // begins — they are the same mapping, taken mutably.
        let offset = {
            let Some(appender) = self.appender() else {
                return Err(Appended::Malformed);
            };

            appender.try_claim_shared(self.session_id, self.stream_id, position_limit, length)?
        };

        let Some(partition) = self.log.active_term_partition() else {
            return Err(Appended::Malformed);
        };

        let Some(term) = self.log.term_mut(partition) else {
            return Err(Appended::Malformed);
        };

        Ok(Claim { term, offset })
    }

    /// The largest payload one frame can carry on this log.
    /// The term this stream began at.
    ///
    /// `Publication.initialTermId()` (`Publication.java:378`) — the id the
    /// driver picked when the publication was created, which a caller needs to
    /// place a position in a term rather than merely to count bytes.
    pub fn initial_term_id(&self) -> i32 {
        self.log.geometry().initial_term_id
    }

    /// How many bytes one term of this publication's log buffer is.
    ///
    /// `Publication.termBufferLength()` (`Publication.java:383`).
    pub fn term_buffer_length(&self) -> i32 {
        self.log.geometry().term_length
    }

    /// `log2(term length)`, for a caller doing its own position arithmetic.
    ///
    /// `Publication.positionBitsToShift()` (`Publication.java:390`); the
    /// reference's archive reads it six times, alongside
    /// [`Publication::initial_term_id`] and
    /// [`Publication::term_buffer_length`], to size the frames it writes.
    pub fn position_bits_to_shift(&self) -> u32 {
        self.log.geometry().bits_to_shift
    }

    pub fn max_payload_length(&self) -> Option<usize> {
        self.appender()
            .map(|appender| appender.max_payload_length())
    }

    /// Where this producer has got to in the stream
    /// (`Publication.position`, `Publication.java:367-378`).
    ///
    /// Read from the **active term's raw tail** every time rather than kept,
    /// which is what the reference does and the reason it matters here: a
    /// publication with more than one producer can be moved by another of them
    /// between two calls, so a position this one remembered would be a number
    /// it could not sign for. `None` for a log whose metadata cannot be read.
    ///
    /// [`Publication::offer`] hands back the same number after writing a frame,
    /// which is cheaper for a caller that has just written one; this is for a
    /// caller that has not.
    pub fn position(&self) -> Option<i64> {
        let tail = self.appender()?.current_tail()?;
        let geometry = self.log.geometry();

        Some(
            Position::new(
                tail.term_id(),
                tail.term_offset(geometry.term_length),
                geometry.bits_to_shift,
                geometry.initial_term_id,
            )
            .raw(),
        )
    }

    /// How much room is left before this publication is back-pressured
    /// (`Publication.availableWindow`, `Publication.java:409-413`;
    /// `ConcurrentPublication.java:73-80`).
    ///
    /// The limit is passed in for the reason [`Publication::offer`] gives: the
    /// counter lives in the CnC file, which this type does not own. What comes
    /// back is `limit - position` — positive while there is room — and the
    /// reference's `CLOSED` (-1) for a closed publication is [`None`] here,
    /// because a publication this client has given back is one
    /// [`crate::Client::available_window`] cannot find.
    ///
    /// A positive answer is a **guide**, as the reference calls it: the limit
    /// moves under the caller, so an offer made on the strength of it can still
    /// come back `BackPressured`.
    pub fn available_window(&self, position_limit: i64) -> Option<i64> {
        Some(position_limit - self.position()?)
    }

    /// Mark this publication to be revoked when it is given back
    /// (`ExclusivePublication.revokeOnClose`, `ExclusivePublication.java:155-158`).
    ///
    /// Giving it back is [`crate::Client::remove_publication`] here, which is
    /// this crate's close: the reference's `close()` and this build's removal
    /// are the same event, and neither sends the revoke until it happens. So a
    /// caller that knows it wants a loud ending can say so now and close later,
    /// which is what the archive does (`ControlSession.java:168-169` marks it
    /// and closes on the next line).
    pub const fn revoke_on_close(&mut self) {
        self.revoke_on_close = true;
    }

    /// Whether it has been marked.
    pub const fn is_revoke_on_close(&self) -> bool {
        self.revoke_on_close
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
            .field("revoke_on_close", &self.revoke_on_close)
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
    /// Which of the log's three terms this producer is in — the reference's
    /// `active_partition_index` (`aeron_exclusive_publication.h:62`), kept on the
    /// object and moved only by a rotation.
    ///
    /// It is a cache of the log's answer to a question the log is the authority
    /// on, so the question is still asked — [`ExclusivePublication::seed_from_log`]
    /// reads the log's `active_term_count`, and that runs when the log rotates
    /// under this producer. What caching buys is that it is asked once per
    /// rotation instead of once per append.
    ///
    /// A stale partition cannot be written through: every path that appends asks
    /// [`Appender::try_claim_exclusive`]'s own first question (`term_is_current`)
    /// before it touches the term, and that is exactly the question "has this
    /// cache missed a rotation". The one read-only use, `max_payload_length`,
    /// does not depend on the partition at all.
    partition: std::cell::Cell<usize>,
    /// As [`Publication::revoke_on_close`], which is the whole of what the two
    /// kinds share about endings: the flag is not a property of having one
    /// producer.
    revoke_on_close: bool,
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
            // Whatever is here is overwritten by the seed below, which is the
            // read of the log that answers the question properly.
            partition: std::cell::Cell::new(0),
            revoke_on_close: false,
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
    /// The term this stream began at.
    ///
    /// `Publication.initialTermId()` (`Publication.java:378`) — the id the
    /// driver picked when the publication was created, which a caller needs to
    /// place a position in a term rather than merely to count bytes.
    pub fn initial_term_id(&self) -> i32 {
        self.log.geometry().initial_term_id
    }

    /// How many bytes one term of this publication's log buffer is.
    ///
    /// `Publication.termBufferLength()` (`Publication.java:383`).
    pub fn term_buffer_length(&self) -> i32 {
        self.log.geometry().term_length
    }

    /// `log2(term length)`, for a caller doing its own position arithmetic.
    ///
    /// `Publication.positionBitsToShift()` (`Publication.java:390`); the
    /// reference's archive reads it six times, alongside
    /// [`Publication::initial_term_id`] and
    /// [`Publication::term_buffer_length`], to size the frames it writes.
    pub fn position_bits_to_shift(&self) -> u32 {
        self.log.geometry().bits_to_shift
    }

    pub fn max_payload_length(&self) -> Option<usize> {
        self.appender()
            .map(|appender| appender.max_payload_length())
    }

    /// Where this producer has got to in the stream
    /// (`ExclusivePublication.position`, `ExclusivePublication.java:437-445`).
    ///
    /// The pair this publication keeps, rather than the log's tail — which is
    /// the one place the two kinds of publication answer this question
    /// differently, and for the reason they are different types: an exclusive
    /// publication is the only writer of its log, so what it remembers cannot
    /// have been moved by anybody else. `Some` always, then; the [`Option`] is
    /// the shape every publication shares.
    pub fn position(&self) -> Option<i64> {
        let geometry = self.log.geometry();

        Some(
            Position::new(
                self.term_id.get(),
                self.term_offset.get(),
                geometry.bits_to_shift,
                geometry.initial_term_id,
            )
            .raw(),
        )
    }

    /// How much room is left before this publication is back-pressured
    /// (`ExclusivePublication.availableWindow`, `ExclusivePublication.java:195-203`).
    ///
    /// As [`Publication::available_window`]: the limit comes in from the CnC
    /// file and the answer is `limit - position`.
    pub fn available_window(&self, position_limit: i64) -> Option<i64> {
        Some(position_limit - self.position()?)
    }

    /// Mark this publication to be revoked when it is given back. See
    /// [`Publication::revoke_on_close`], which is the same flag.
    pub const fn revoke_on_close(&mut self) {
        self.revoke_on_close = true;
    }

    /// Whether it has been marked.
    pub const fn is_revoke_on_close(&self) -> bool {
        self.revoke_on_close
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
            // As in `try_claim`: both "try again" answers say this publication's
            // cached term is not the log's current one, and re-seeding is how the
            // cache hears the log's answer.
            Appended::EndOfLog | Appended::MidRotation => self.seed_from_log(),
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
                    // Both of the "try again" answers mean this publication's
                    // cached term is not the log's current one, and the cache is
                    // what the next call is judged against: a term count that has
                    // moved on is `EndOfLog`, a cached term id that names a term
                    // the log has left behind is `MidRotation`. Re-seeding reads
                    // the log's own answer, which is what the reference does on
                    // *every* call (`aeron_publication.c:483-493` reads the
                    // active term count, that partition's tail and the term id
                    // from the metadata each time) — and without it a
                    // `MidRotation` is returned for ever, because nothing else
                    // moves the cache.
                    if matches!(error, Appended::EndOfLog | Appended::MidRotation) {
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
        let Some(term) = self.log.term_mut(self.partition.get()) else {
            return Err(Appended::Malformed);
        };

        Ok(Claim { term, offset })
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
            // As in `try_claim`: both "try again" answers say this publication's
            // cached term is not the log's current one, and re-seeding is how the
            // cache hears the log's answer.
            Appended::EndOfLog | Appended::MidRotation => self.seed_from_log(),
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
            // As in `try_claim`: both "try again" answers say this publication's
            // cached term is not the log's current one, and re-seeding is how the
            // cache hears the log's answer.
            Appended::EndOfLog | Appended::MidRotation => self.seed_from_log(),
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
    ///
    /// The two numbers that turn a position into the cached pair — `log2` of
    /// the term length, and the initial term id — are the log's **shape**, not
    /// anything an append moves, so they come off the mapping's own geometry
    /// ([`LogBuffer::geometry`], read once when the file was mapped) rather than
    /// out of an [`Appender`]. Building one to ask it for them costs a metadata
    /// region, a load of `active_term_count`, a term view and three further
    /// metadata fields (`Appender::new`'s `TERM_LENGTH`, `INITIAL_TERM_ID` and
    /// `MTU_LENGTH`), on the path of **every** successful append. The reference
    /// keeps both of them on the publication object instead
    /// (`aeron_exclusive_publication.h:45-47`: `position_bits_to_shift`,
    /// `initial_term_id`, `term_buffer_length`) and re-reads nothing to move the
    /// pair after an append.
    ///
    /// This is total where the appender was fallible: a geometry that could not
    /// be read is a log that never mapped ([`LogBuffer::open`]), so a caller
    /// that has one has the other, and the pair is always moved.
    fn advance_to(&self, position: deepmsg_core::logbuffer::position::Position) {
        let geometry = self.log.geometry();

        self.term_id
            .set(position.term_id(geometry.bits_to_shift, geometry.initial_term_id));
        self.term_offset
            .set(position.term_offset(geometry.bits_to_shift));
    }

    /// Re-read the cached pair from the log's current tail
    /// (`aeron_exclusive_publication.c:451-459`).
    ///
    /// The raw offset, not the saturating one: a tail that has run to the end
    /// of a term says so, and zero and `term_length` are different answers.
    fn seed_from_log(&self) {
        // The log's answer, taken before anything reads the partition this
        // publication has cached: `active_term_count` is what says which term is
        // the current one, and after a rotation the count and a tail can
        // disagree for a moment — the count is the one that is right.
        let Some(partition) = self.log.active_term_partition() else {
            return;
        };

        self.partition.set(partition);

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
    /// and for the same reason — **except** for where the partition comes from.
    ///
    /// [`Publication`] re-reads the log's `active_term_count` on every call
    /// because a shared publication is not the log's only writer and cannot
    /// assume its cache is still current. An exclusive one *is* the only writer,
    /// which is the whole of what "exclusive" means, so the reference keeps
    /// `active_partition_index` on the object and hands it to the appender —
    /// `TermAppender.claim` is given `termBuffers[activePartitionIndex]` and the
    /// tail offset that goes with it, with nothing left to ask the log. This is
    /// that, and the log is asked again only in
    /// [`ExclusivePublication::seed_from_log`], which is where a rotation is
    /// noticed.
    fn appender(&self) -> Option<Appender<'_>> {
        let metadata = self.log.file().region_mut(
            self.log.geometry().metadata_offset,
            descriptor::METADATA_LENGTH,
        )?;

        let term = self.log.term_mut(self.partition.get())?;

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
