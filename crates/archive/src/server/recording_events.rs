//! The recording events publication (P2-13): `RecordingStarted`,
//! `RecordingProgress` and `RecordingStopped` on the channel the deployment
//! named.
//!
//! The reference's `io.aeron.archive.RecordingEventsProxy` — one `Publication`
//! over `aeron.archive.recording.events.channel`, one stream for every recording,
//! with the events of all of them interleaved and told apart by `recordingId`.
//! The reader is the client's own `RecordingEventsAdapter`, which is also what
//! the reference's benchmark harness uses as a **pacemaker**
//! (`LiveRecordingMessageTransceiver.java:158-190`: it consumes only what the
//! archive has recorded, and the events are how it hears that).
//!
//! # One publication, and who may hold it
//!
//! The reference builds it on the conductor (`ArchiveConductor.java:224-225`)
//! and hands the **same object** to every `RecordingSession`
//! (`RecordingSession.java:44`, `:62`). That works there because Aeron's
//! `Publication` may be written from any thread as long as one thread drives the
//! driver; this build's client has no such discipline, so the publication is
//! owned by **whatever runs the sessions** — the recorder's own half, which P2-9a
//! made a seam for ([`super::recorder::RecorderHalf`]) — and a session borrows it
//! for its turn.
//!
//! # The encoding is separable, and separate
//!
//! [`encode_started`], [`encode_progress`] and [`encode_stopped`] are free
//! functions over a buffer: the bytes are the contract — the client's own
//! `RecordingEventsAdapter` reads them — and a contract that can only be reached
//! through a live publication is a contract nobody tests. [`RecordingEventsProxy`]
//! is then the thin part: offer what was built, and answer the ways that can go
//! (`RecordingEventsProxy.checkResult`).
//!
//! # Three sends, and a seam in front of them
//!
//! [`RecordingEvents`] is a **trait**, the twin of
//! [`RecordingImage`](super::recording_session::RecordingImage) and of
//! [`crate::server::replay_session::Publication`], and for the same reason: what
//! the session needs is three sends, and the publication is one answer to them
//! while a unit test's is another. The three decisions that can be wrong —
//! whether a progress event is sent again, whether a dropped one is retried,
//! what a fatal answer does to the session — are then testable without a mapped
//! log, which is the whole of what a driver would otherwise be needed for.
//!
//! # What is not the reference's, and says so
//!
//! - **`offer` for all three.** The reference claims into the term for `progress`
//!   and `stopped` (`tryClaim`) and encodes in place; this encodes into the
//!   proxy's own buffer and offers it. The frame on the wire is the same; the
//!   difference is one copy per event.
//! - ★ **`Publication.CLOSED` is a state this build's client cannot reach**, and
//!   that is a difference between the two ownership models rather than a missing
//!   case. The reference's `checkResult` throws on it as well as on
//!   `MAX_POSITION_EXCEEDED`, because its `Publication` is **reference-counted**:
//!   `Client.close()` can close one while a caller still holds it. Here a
//!   publication leaves the client by **move** — `take_exclusive_publication`
//!   removes it from the list (`crates/client/src/client.rs:1205`) and
//!   `forget_exclusive_publication` drops what is left (`:2776`) — so there is no
//!   moment at which a caller holds a publication the client has closed. What a
//!   publication with no usable log can still answer is
//!   `Appended::Malformed` (`crates/client/src/publication.rs:565-567`), and for
//!   a publication that cannot be closed that is the right answer.
//!   [`EventsError`] therefore names only what can actually happen, rather than a
//!   variant nothing can produce.

use std::fmt;

use deepmsg_client::publication::ExclusivePublication;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_codec::archive::WriteBuf;
use deepmsg_codec::archive::message_header_codec;
use deepmsg_codec::archive::recording_progress_codec::RecordingProgressEncoder;
use deepmsg_codec::archive::recording_started_codec::RecordingStartedEncoder;
use deepmsg_codec::archive::recording_stopped_codec::RecordingStoppedEncoder;
use deepmsg_core::buffer::ReadWrite;
use deepmsg_core::logbuffer::append::Appended;

use crate::server::replay_session::position_limit;

/// `MessageHeaderEncoder.ENCODED_LENGTH`: the eight bytes in front of every
/// message's body, which is where the body starts and what the frame's length
/// adds up.
const HEADER_LENGTH: usize = message_header_codec::ENCODED_LENGTH;

/// `RecordingEventsProxy.SEND_ATTEMPTS` (`:31`): how many times an event is
/// offered before it is given up on. Not a retry loop over back pressure — the
/// third attempt is the last, and the next event will have a new position.
const SEND_ATTEMPTS: usize = 3;

/// `new ExpandableArrayBuffer(1024)` (`:35`): one buffer per proxy, sized for the
/// largest event — `RecordingStarted` with its two strings.
pub const BUFFER_LENGTH: usize = 1024;

/// The one way publishing an event is fatal, in the reference's own words
/// (`RecordingEventsProxy.checkResult`).
///
/// `Publication.CLOSED` is the reference's other fatal answer and is **not
/// here**: this build's client cannot report it (see the module note). A variant
/// nothing can produce would be a claim, not a case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventsError {
    /// `Publication.MAX_POSITION_EXCEEDED` — the publication's position reached
    /// the stream's maximum, so no event will ever go out on it again.
    Exhausted {
        /// `publication.termBufferLength()`, which the reference's message
        /// carries (`:139-142`).
        term_length: i32,
    },
}

impl fmt::Display for EventsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // The reference's composition, character for character (`:140-142`).
            Self::Exhausted { term_length } => write!(
                f,
                "recording events publication at max position, term-length={term_length}"
            ),
        }
    }
}

/// `RecordingEventsProxy.started`'s message (`:53-66`), into `buffer`.
///
/// `channel` and `source_identity` are the **recorded subscription's** — the
/// reference passes `image.subscription().streamId()` for the stream and
/// `image.sourceIdentity()` for the identity, which is why they are arguments
/// rather than read off the events channel.
#[must_use]
pub fn encode_started(
    buffer: &mut [u8],
    recording_id: i64,
    start_position: i64,
    session_id: i32,
    stream_id: i32,
    channel: &str,
    source_identity: &str,
) -> usize {
    let encoder = RecordingStartedEncoder::default().wrap(WriteBuf::new(buffer), HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut encoder = header.parent().expect("the encoder just wrapped");

    encoder
        .recording_id(recording_id)
        .start_position(start_position)
        .session_id(session_id)
        .stream_id(stream_id)
        .channel(channel.as_bytes())
        .source_identity(source_identity.as_bytes());

    HEADER_LENGTH + encoder.encoded_length()
}

/// `RecordingEventsProxy.progress`'s message (`:75-88`), into `buffer`.
#[must_use]
pub fn encode_progress(
    buffer: &mut [u8],
    recording_id: i64,
    start_position: i64,
    position: i64,
) -> usize {
    let encoder = RecordingProgressEncoder::default().wrap(WriteBuf::new(buffer), HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut encoder = header.parent().expect("the encoder just wrapped");

    encoder
        .recording_id(recording_id)
        .start_position(start_position)
        .position(position);

    HEADER_LENGTH + encoder.encoded_length()
}

/// `RecordingEventsProxy.stopped`'s message (`:97-116`), into `buffer`.
#[must_use]
pub fn encode_stopped(
    buffer: &mut [u8],
    recording_id: i64,
    start_position: i64,
    stop_position: i64,
) -> usize {
    let encoder = RecordingStoppedEncoder::default().wrap(WriteBuf::new(buffer), HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut encoder = header.parent().expect("the encoder just wrapped");

    encoder
        .recording_id(recording_id)
        .start_position(start_position)
        .stop_position(stop_position);

    HEADER_LENGTH + encoder.encoded_length()
}

/// The recording events, as a recording session sees them — the three sends of
/// `io.aeron.archive.RecordingEventsProxy` (`:28-128`).
///
/// See the module note for why this is a trait. [`RecordingEventsProxy`] is the
/// real answer and [`FakeEvents`] a test's.
pub trait RecordingEvents {
    /// `RecordingEventsProxy.started` (`:53-73`).
    ///
    /// # Errors
    ///
    /// [`EventsError::Exhausted`] when the position is finished, which the
    /// reference throws rather than swallowing (`:139-142`).
    #[allow(clippy::too_many_arguments)] // one per field of the message, as the reference's own signature has
    fn started(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        source_identity: &str,
    ) -> Result<(), EventsError>;

    /// `RecordingEventsProxy.progress` (`:75-95`), which answers whether the
    /// event went out.
    ///
    /// ★ **The caller is what makes that answer matter**: the reference only
    /// advances `progressEventPosition` when this returns true
    /// (`RecordingSession.java:249-254`), so a progress event that was dropped is
    /// sent again rather than skipped.
    ///
    /// # Errors
    ///
    /// [`EventsError`] as [`RecordingEvents::started`].
    fn progress(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        position: i64,
    ) -> Result<bool, EventsError>;

    /// `RecordingEventsProxy.stopped` (`:97-127`).
    ///
    /// # Errors
    ///
    /// [`EventsError`] as [`RecordingEvents::started`].
    fn stopped(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        stop_position: i64,
    ) -> Result<(), EventsError>;
}

/// What one offer answered, in the three shapes a caller acts on
/// (`RecordingEventsProxy`'s `started` and `progress`).
///
/// The reference branches on the answer three different ways: `started` and
/// `stopped` break on a write **or** on `NOT_CONNECTED` and retry otherwise
/// (`:64-71`, `:100-113`), while `progress` sends **once** and answers `true`
/// only for a write (`:81-92`). Naming the three shapes is what keeps the two
/// from being written as one loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Offered {
    /// The bytes are in the term.
    Sent,
    /// Nobody is subscribed, which the reference does **not** retry: an archive
    /// with no reader is not an archive in trouble (`:68`, `:107`).
    NotConnected,
    /// The window is used up or the log is mid-rotation: worth another attempt
    /// (`Appended::EndOfLog`, `crates/core/src/logbuffer/append.rs:47-56`).
    TryAgain,
}

/// `RecordingEventsProxy.checkResult` (`:129-144`) over one offer's answer, and
/// the two answers it does not act on.
///
/// The fatal one is [`EventsError::Exhausted`], and it is fatal **immediately**:
/// the reference calls `checkResult` inside its two attempt loops and lets the
/// throw out, so a publication at its maximum is not retried three times.
fn classify(appended: Appended, term_length: i32) -> Result<Offered, EventsError> {
    match appended {
        Appended::Ok { .. } => Ok(Offered::Sent),
        Appended::NotConnected => Ok(Offered::NotConnected),
        Appended::MaxPositionExceeded => Err(EventsError::Exhausted { term_length }),
        _ => Ok(Offered::TryAgain),
    }
}

/// The events publication, and the buffer one event is built in.
///
/// Held by whatever runs the sessions — see the module note. Every method takes
/// the counters region, because the window a publication may write to is the
/// driver's `position_limit` counter and not something the publication knows
/// ([`position_limit`], the same helper the replay path uses).
pub struct RecordingEventsProxy {
    publication: ExclusivePublication,
    buffer: Vec<u8>,
}

impl RecordingEventsProxy {
    /// The publication the conductor has already built and taken out of its
    /// client.
    #[must_use]
    pub fn new(publication: ExclusivePublication) -> Self {
        Self {
            publication,
            buffer: vec![0; BUFFER_LENGTH],
        }
    }

    /// The publication's own session id, which is what tells one archive's
    /// events from another's on a shared channel.
    #[must_use]
    pub fn session_id(&self) -> i32 {
        self.publication.session_id()
    }

    /// One offer, its answer classified.
    ///
    /// An **associated** function rather than a method: the payload is a slice
    /// of `self.buffer` while the publication is written to, and the two fields
    /// have to be borrowed apart for that to be possible at all.
    fn attempt(
        publication: &ExclusivePublication,
        payload: &[u8],
        counters: &CountersReader<'_, ReadWrite>,
    ) -> Result<Offered, EventsError> {
        let limit = position_limit(publication, counters);

        classify(
            publication.offer(limit, payload),
            publication.term_buffer_length(),
        )
    }

    /// What one attempt's answer means to `progress` (`:81-92`).
    ///
    /// ★ **Only a write is a `true`.** The reference's `progress` answers `false`
    /// both for a window that was used up and for a publication nobody is
    /// subscribed to, and the session's `progressEventPosition` is what that
    /// `false` protects: it stays where it was, and the next turn offers the same
    /// event again. A `true` here for an event that did not go out loses that
    /// position for good — nothing after it names it again.
    ///
    /// A function of its own, and not a line inside `progress`, because it is the
    /// half of that method which can be wrong: the other half needs a mapped log
    /// to reach at all.
    fn progress_answer(offered: Offered) -> bool {
        matches!(offered, Offered::Sent)
    }
}

/// The publication, and how big the buffer is rather than its contents: the
/// buffer is a kilobyte of the last event and nothing a reader wants.
impl fmt::Debug for RecordingEventsProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordingEventsProxy")
            .field("publication", &self.publication)
            .field("buffer_length", &self.buffer.len())
            .finish()
    }
}

impl RecordingEvents for RecordingEventsProxy {
    fn started(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        source_identity: &str,
    ) -> Result<(), EventsError> {
        let length = encode_started(
            &mut self.buffer,
            recording_id,
            start_position,
            session_id,
            stream_id,
            channel,
            source_identity,
        );

        // The reference's `do`/`while (--attempts > 0)` (`:63-72`): a write or
        // `NOT_CONNECTED` ends it, anything else is tried again, and a
        // publication at its maximum is thrown out of on the first attempt.
        for _ in 0..SEND_ATTEMPTS {
            match Self::attempt(&self.publication, &self.buffer[..length], counters)? {
                Offered::Sent | Offered::NotConnected => break,
                Offered::TryAgain => {}
            }
        }

        Ok(())
    }

    fn progress(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        position: i64,
    ) -> Result<bool, EventsError> {
        let length = encode_progress(&mut self.buffer, recording_id, start_position, position);

        // ★ **One attempt, and the answer is the caller's.** The reference's
        // `progress` has no `SEND_ATTEMPTS` loop (`:79-92`), unlike the two
        // below it: a claim that did not land is a `false` the session acts on.
        Ok(Self::progress_answer(Self::attempt(
            &self.publication,
            &self.buffer[..length],
            counters,
        )?))
    }

    fn stopped(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        stop_position: i64,
    ) -> Result<(), EventsError> {
        let length = encode_stopped(
            &mut self.buffer,
            recording_id,
            start_position,
            stop_position,
        );

        for _ in 0..SEND_ATTEMPTS {
            match Self::attempt(&self.publication, &self.buffer[..length], counters)? {
                Offered::Sent | Offered::NotConnected => break,
                Offered::TryAgain => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_codec::archive::ReadBuf;
    use deepmsg_codec::archive::message_header_codec::MessageHeaderDecoder;
    use deepmsg_codec::archive::recording_progress_codec::{
        self as progress, RecordingProgressDecoder,
    };
    use deepmsg_codec::archive::recording_started_codec::{
        self as started, RecordingStartedDecoder,
    };
    use deepmsg_codec::archive::recording_stopped_codec::{
        self as stopped, RecordingStoppedDecoder,
    };
    use deepmsg_core::logbuffer::position::Position;

    /// The body, decoded. The generated `wrap` takes the acting block length and
    /// version rather than reading them, which is what SBE means by them: a
    /// reader written against an older schema still reads a newer message.
    fn body(buffer: &[u8]) -> (u16, u16, u16) {
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(buffer), 0);
        (
            header.template_id(),
            header.block_length(),
            header.version(),
        )
    }

    /// ★ **The wire layout, read straight out of the buffer** rather than through
    /// the generated decoder's var-data accessors, whose `&'a self` signature
    /// ties the returned slice to the *buffer's* lifetime rather than the
    /// decoder's. This is also the stronger statement: what is pinned is
    /// `u32 length` then the bytes, in that order, which is what the reference's
    /// `RecordingStartedEncoder.channel(byte[])` writes.
    fn var_string(buffer: &[u8], at: usize) -> (&[u8], usize) {
        let length =
            u32::from_le_bytes(buffer[at..at + 4].try_into().expect("four bytes")) as usize;
        let end = at + 4 + length;
        (&buffer[at + 4..end], end)
    }

    /// ★ The `started` event, field by field: the header a reader dispatches on,
    /// the fixed block, and the two strings that are the only reason this message
    /// needs a 1024-byte buffer.
    #[test]
    fn a_started_event_encodes_every_field() {
        let mut buffer = [0_u8; BUFFER_LENGTH];
        let length = encode_started(
            &mut buffer,
            7,
            1024,
            42,
            1001,
            "aeron:udp?endpoint=localhost:9000",
            "aeron:udp?endpoint=localhost:8000",
        );

        let (template_id, block_length, version) = body(&buffer);
        assert_eq!(started::SBE_TEMPLATE_ID, template_id);
        assert_eq!(started::SBE_BLOCK_LENGTH, block_length);
        assert_eq!(started::SBE_SCHEMA_VERSION, version);

        let decoder = RecordingStartedDecoder::default().wrap(
            ReadBuf::new(&buffer),
            HEADER_LENGTH,
            started::SBE_BLOCK_LENGTH,
            started::SBE_SCHEMA_VERSION,
        );
        assert_eq!(7, decoder.recording_id());
        assert_eq!(1024, decoder.start_position());
        assert_eq!(42, decoder.session_id());
        assert_eq!(1001, decoder.stream_id());

        let (channel, after_channel) = var_string(&buffer, HEADER_LENGTH + 24);
        let (identity, end) = var_string(&buffer, after_channel);
        assert_eq!(b"aeron:udp?endpoint=localhost:9000", channel);
        assert_eq!(b"aeron:udp?endpoint=localhost:8000", identity);
        assert_eq!(length, end, "and nothing after them");
    }

    #[test]
    fn a_progress_event_encodes_every_field() {
        let mut buffer = [0_u8; BUFFER_LENGTH];
        let length = encode_progress(&mut buffer, 7, 1024, 8192);

        let (template_id, block_length, _) = body(&buffer);
        assert_eq!(progress::SBE_TEMPLATE_ID, template_id);
        assert_eq!(progress::SBE_BLOCK_LENGTH, block_length);
        assert_eq!(HEADER_LENGTH + 24, length, "no variable part");

        let decoder = RecordingProgressDecoder::default().wrap(
            ReadBuf::new(&buffer),
            HEADER_LENGTH,
            progress::SBE_BLOCK_LENGTH,
            progress::SBE_SCHEMA_VERSION,
        );
        assert_eq!(7, decoder.recording_id());
        assert_eq!(1024, decoder.start_position());
        assert_eq!(8192, decoder.position());
    }

    #[test]
    fn a_stopped_event_encodes_every_field() {
        let mut buffer = [0_u8; BUFFER_LENGTH];
        let length = encode_stopped(&mut buffer, 7, 1024, 4096);

        let (template_id, block_length, _) = body(&buffer);
        assert_eq!(stopped::SBE_TEMPLATE_ID, template_id);
        assert_eq!(stopped::SBE_BLOCK_LENGTH, block_length);
        assert_eq!(HEADER_LENGTH + 24, length);

        let decoder = RecordingStoppedDecoder::default().wrap(
            ReadBuf::new(&buffer),
            HEADER_LENGTH,
            stopped::SBE_BLOCK_LENGTH,
            stopped::SBE_SCHEMA_VERSION,
        );
        assert_eq!(7, decoder.recording_id());
        assert_eq!(1024, decoder.start_position());
        assert_eq!(4096, decoder.stop_position());
    }

    /// The three templates a reader dispatches on, which the reference's
    /// `RecordingEventsAdapter` switches over. A body encoded under the wrong id
    /// is a body nobody reads, so the ids are asserted against the schema's own
    /// numbers rather than against each other.
    #[test]
    fn the_three_events_are_the_schemas_own_templates() {
        assert_eq!(101, started::SBE_TEMPLATE_ID, "RecordingStarted");
        assert_eq!(102, progress::SBE_TEMPLATE_ID, "RecordingProgress");
        assert_eq!(103, stopped::SBE_TEMPLATE_ID, "RecordingStopped");
    }

    /// Every answer an offer can give, in the shape the caller acts on.
    ///
    /// ★ The two that matter are the ones a single loop would get wrong:
    /// `NotConnected` **ends** `started`'s attempts (`:66-69`, no reader is not
    /// trouble) while a used-up window is retried, and the maximum is neither —
    /// it is the throw (`:134-143`) and it happens on the first attempt.
    #[test]
    fn an_offers_answer_classifies_into_the_three_shapes() {
        let term_length = 64 * 1024;

        let sent = Appended::Ok {
            position: Position::from_raw(1024),
            term_offset: 0,
        };
        assert_eq!(Ok(Offered::Sent), classify(sent, term_length));
        assert_eq!(
            Ok(Offered::NotConnected),
            classify(Appended::NotConnected, term_length)
        );

        for try_again in [
            Appended::BackPressured,
            Appended::EndOfLog,
            Appended::MidRotation,
        ] {
            assert_eq!(
                Ok(Offered::TryAgain),
                classify(try_again, term_length),
                "{try_again:?} is a window that will open again"
            );
        }

        assert_eq!(
            Err(EventsError::Exhausted { term_length }),
            classify(Appended::MaxPositionExceeded, term_length),
            "the one answer that is not an attempt at all"
        );
    }

    /// ★ `progress` says the event went out for a write and for nothing else.
    ///
    /// The mutation this pins is `Ok(true)`: it is what the first cut of this
    /// file did, it compiles, it reads plausibly, and it quietly loses every
    /// progress event a slow reader caused to be dropped — the session advances
    /// its mark past a position nobody was told about
    /// (`RecordingSession.java:249-254`).
    #[test]
    fn progress_answers_yes_only_for_a_write() {
        assert!(RecordingEventsProxy::progress_answer(Offered::Sent));

        assert!(
            !RecordingEventsProxy::progress_answer(Offered::NotConnected),
            "an archive with no reader is not an archive that reported"
        );
        assert!(
            !RecordingEventsProxy::progress_answer(Offered::TryAgain),
            "a window that is used up now is one to offer into again"
        );
    }

    /// The message the archive dies with is the reference's, character for
    /// character (`RecordingEventsProxy:140-142`) — term length and all.
    #[test]
    fn the_exhausted_message_is_the_references() {
        assert_eq!(
            "recording events publication at max position, term-length=65536",
            EventsError::Exhausted {
                term_length: 64 * 1024
            }
            .to_string()
        );
    }
}
/// A [`RecordingEvents`] a test drives: it answers what it is told to, and
/// counts what it was asked.
///
/// The twin of [`FakeImage`](super::recording_session::FakeImage), and there for
/// the same reason — a session's three landing points are about *when* an event
/// goes out and what the answer does to the session's own state, and neither
/// question needs a mapped log to answer.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct FakeEvents {
    /// What `progress` answers: `true` is a write, `false` is a dropped event
    /// the session has to send again.
    pub(crate) sent: bool,
    /// What all three answer when it is set: the publication is finished.
    pub(crate) exhausted: bool,
    /// What the session asked for, in order.
    pub(crate) calls: Vec<EventCall>,
}

/// One of the three sends, with the fields a test asserts on.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventCall {
    Started {
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: String,
        source_identity: String,
    },
    Progress {
        recording_id: i64,
        position: i64,
    },
    Stopped {
        recording_id: i64,
        stop_position: i64,
    },
}

#[cfg(test)]
impl FakeEvents {
    pub(crate) fn new() -> Self {
        Self {
            sent: true,
            exhausted: false,
            calls: Vec::new(),
        }
    }

    /// The publication is finished, which is the answer the reference throws on
    /// (`:134-143`).
    fn exhaust(&mut self) -> EventsError {
        self.exhausted = true;

        EventsError::Exhausted {
            term_length: 64 * 1024,
        }
    }
}

#[cfg(test)]
impl RecordingEvents for FakeEvents {
    fn started(
        &mut self,
        _counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        source_identity: &str,
    ) -> Result<(), EventsError> {
        if self.exhausted {
            return Err(self.exhaust());
        }

        self.calls.push(EventCall::Started {
            recording_id,
            start_position,
            session_id,
            stream_id,
            channel: channel.to_owned(),
            source_identity: source_identity.to_owned(),
        });

        Ok(())
    }

    fn progress(
        &mut self,
        _counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        _start_position: i64,
        position: i64,
    ) -> Result<bool, EventsError> {
        if self.exhausted {
            return Err(self.exhaust());
        }

        self.calls.push(EventCall::Progress {
            recording_id,
            position,
        });

        Ok(self.sent)
    }

    fn stopped(
        &mut self,
        _counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        _start_position: i64,
        stop_position: i64,
    ) -> Result<(), EventsError> {
        if self.exhausted {
            return Err(self.exhaust());
        }

        self.calls.push(EventCall::Stopped {
            recording_id,
            stop_position,
        });

        Ok(())
    }
}
