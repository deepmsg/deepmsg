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
//! through a live publication is a contract nobody tests. [`RecordingEvents`] is
//! then the thin part: offer what was built, and answer the two ways that can go
//! (`RecordingEventsProxy.checkResult`).
//!
//! # What is not the reference's, and says so
//!
//! - **`offer` for all three.** The reference claims into the term for `progress`
//!   and `stopped` (`tryClaim`) and encodes in place; this encodes into the
//!   proxy's own buffer and offers it. The frame on the wire is the same; the
//!   difference is one copy per event.
//! - ★ **`Publication.CLOSED` has no counterpart here.** The reference's
//!   `checkResult` throws on it as well as on `MAX_POSITION_EXCEEDED`, but this
//!   build's client cannot report it: a publication whose log is gone offers
//!   `Appended::Malformed` (`crates/client/src/publication.rs:565-567`), and the
//!   replay path has the same gap. [`EventsError`] therefore names only what can
//!   actually happen, rather than a variant nothing can produce.

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

/// The events publication, and the buffer one event is built in.
///
/// Held by whatever runs the sessions — see the module note. Every method takes
/// the counters region, because the window a publication may write to is the
/// driver's `position_limit` counter and not something the publication knows
/// ([`position_limit`], the same helper the replay path uses).
pub struct RecordingEvents {
    publication: ExclusivePublication,
    buffer: Vec<u8>,
}

impl RecordingEvents {
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

    /// `RecordingEventsProxy.started` (`:53-73`).
    ///
    /// # Errors
    ///
    /// [`EventsError`] when the position is exhausted, which the reference
    /// throws rather than swallowing (`:139-142`).
    #[allow(clippy::too_many_arguments)] // one per field of the message, as the reference's own signature has
    pub fn started(
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

        Self::offer(&self.publication, &self.buffer[..length], counters)
    }

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
    pub fn progress(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        recording_id: i64,
        start_position: i64,
        position: i64,
    ) -> Result<bool, EventsError> {
        let length = encode_progress(&mut self.buffer, recording_id, start_position, position);
        Self::offer(&self.publication, &self.buffer[..length], counters)?;

        Ok(true)
    }

    /// `RecordingEventsProxy.stopped` (`:97-127`).
    ///
    /// # Errors
    ///
    /// [`EventsError`] as [`RecordingEvents::started`].
    pub fn stopped(
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
        Self::offer(&self.publication, &self.buffer[..length], counters)
    }

    /// The offer every event makes, with the reference's attempt count and its
    /// two "stop" answers.
    ///
    /// `Ok(())` covers both a sent event and `NOT_CONNECTED`: the reference stops
    /// trying on the latter rather than counting it as a failure (`:66-69`),
    /// because an archive with no reader is not an archive in trouble.
    /// `BackPressured` and the two rotation answers are the ones worth another
    /// attempt.
    ///
    /// An **associated** function rather than a method: the buffer an event was
    /// built in is borrowed from `self` while the offer happens, and the two
    /// fields have to be borrowed apart for that to be possible at all.
    fn offer(
        publication: &ExclusivePublication,
        payload: &[u8],
        counters: &CountersReader<'_, ReadWrite>,
    ) -> Result<(), EventsError> {
        let limit = position_limit(publication, counters);

        for _ in 0..SEND_ATTEMPTS {
            match publication.offer(limit, payload) {
                Appended::Ok { .. } | Appended::NotConnected => return Ok(()),
                Appended::MaxPositionExceeded => {
                    return Err(EventsError::Exhausted {
                        term_length: publication.term_buffer_length(),
                    });
                }
                _ => {}
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
}
