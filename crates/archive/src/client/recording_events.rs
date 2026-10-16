//! Reading the recording events: the client's half of P2-13.
//!
//! The archive publishes `RecordingStarted`, `RecordingProgress` and
//! `RecordingStopped` on `aeron.archive.recording.events.channel`
//! ([`crate::server::recording_events`]); this is the other end of that channel —
//! `io.aeron.archive.client.RecordingEventsAdapter`, one subscription polled and
//! three templates dispatched to a listener.
//!
//! # Why it is here and not in the benchmark
//!
//! Its only caller today is the benchmark's live-recording client, which reads the
//! events as a **pacemaker**: `LiveRecordingMessageTransceiver.receive` refuses to
//! take a message the archive has not reported recording yet
//! (`LiveRecordingMessageTransceiver.java:155-183`). But the class it ports is the
//! reference's *client* class, beside the decoders and the archive proxy, and a
//! second caller — anything that wants to watch recordings rather than make them —
//! would want it here. Putting it under `bench/` would have claimed the opposite.
//!
//! # The three templates, and the fourth case
//!
//! The reference's `switch` has no `default`: a template it does not know is
//! consumed and the poll carries on. So is a message this reader cannot read **at
//! all** — too short for a header, or another schema's — which the reference turns
//! into an `ArchiveException` out of the poll. Both are here: the second is
//! [`EventsReadError`], reported by the poll rather than thrown out of a fragment
//! handler, which is how every other poller in this crate reports the same thing.

use std::fmt;

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, Message};
use deepmsg_codec::archive::ReadBuf;
use deepmsg_codec::archive::message_header_codec::ENCODED_LENGTH;
use deepmsg_codec::archive::recording_progress_codec::RecordingProgressDecoder;
use deepmsg_codec::archive::recording_started_codec::RecordingStartedDecoder;
use deepmsg_codec::archive::recording_stopped_codec::RecordingStoppedDecoder;

use crate::client::poller::sbe_header;
use deepmsg_codec::archive::SBE_SCHEMA_ID;

/// The three template ids this reads, from the schema.
mod template {
    pub use deepmsg_codec::archive::recording_progress_codec::SBE_TEMPLATE_ID as PROGRESS;
    pub use deepmsg_codec::archive::recording_started_codec::SBE_TEMPLATE_ID as STARTED;
    pub use deepmsg_codec::archive::recording_stopped_codec::SBE_TEMPLATE_ID as STOPPED;
}

/// What a recording did, told one event at a time
/// (`io.aeron.archive.client.RecordingEventsListener`).
///
/// The three methods are the reference's three, with its arguments in its order —
/// which is what a caller that ports a listener can be checked against.
pub trait RecordingEventsListener {
    /// `onStart`: a recording began.
    fn on_start(
        &mut self,
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        source_identity: &str,
    );

    /// `onProgress`: how far it has got.
    fn on_progress(&mut self, recording_id: i64, start_position: i64, position: i64);

    /// `onStop`: it ended, and where.
    fn on_stop(&mut self, recording_id: i64, start_position: i64, stop_position: i64);
}

/// Why an events poll could not read what it found
/// (`RecordingEventsAdapter.onFragment`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventsReadError {
    /// A message shorter than a message header, which cannot name a schema. The
    /// reference reads the header anyway and its buffer throws.
    ShortMessage {
        /// How long the message was.
        length: usize,
    },
    /// The message belongs to another schema (`:56-60`), which the reference turns
    /// into `ArchiveException("expected schemaId=…, actual=…")`.
    UnexpectedSchemaId {
        /// The archive schema's id.
        expected: u16,
        /// What the message carried.
        actual: u16,
    },
}

impl fmt::Display for EventsReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShortMessage { length } => write!(
                f,
                "recording event of {length} bytes is shorter than a message header"
            ),
            // The reference's composition, character for character.
            Self::UnexpectedSchemaId { expected, actual } => {
                write!(f, "expected schemaId={expected}, actual={actual}")
            }
        }
    }
}

impl std::error::Error for EventsReadError {}

/// One subscription's events, handed to a [`RecordingEventsListener`].
///
/// Holds the listener **for the length of a poll**, which is the Rust shape of the
/// reference's adapter holding one for its lifetime: `on_message` is called by the
/// client's own scan and has nowhere else to put an argument.
pub struct RecordingEventsAdapter<'a> {
    listener: &'a mut dyn RecordingEventsListener,
    subscription: i64,
    fragment_limit: usize,
    /// Why this poll met a message it could not read, if it did. The scan carries on
    /// either way; the poll reports it afterwards, as every other poller here does.
    error: Option<EventsReadError>,
}

impl<'a> RecordingEventsAdapter<'a> {
    /// An adapter for one subscription.
    #[must_use]
    pub const fn new(
        listener: &'a mut dyn RecordingEventsListener,
        subscription: i64,
        fragment_limit: usize,
    ) -> Self {
        Self {
            listener,
            subscription,
            fragment_limit,
            error: None,
        }
    }

    /// Poll for events, and hand each one to the listener.
    ///
    /// # Errors
    ///
    /// [`EventsReadError`] when a message the scan met could not be read — the
    /// events that *could* be read have already been delivered, which is the
    /// reference's behaviour too: it throws out of the fragment handler, so the
    /// events before the bad one have been dispatched by then.
    pub fn poll(&mut self, client: &mut Client) -> Result<usize, EventsReadError> {
        self.error = None;

        let polled =
            client.poll_subscription_controlled(self.subscription, self.fragment_limit, self);

        match self.error.take() {
            Some(error) => Err(error),
            None => Ok(polled),
        }
    }
}

impl deepmsg_client::fragment_assembler::ControlledHandler for RecordingEventsAdapter<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        match dispatch(message.payload, self.listener) {
            Ok(_) => Action::Continue,
            Err(error) => {
                // The reference throws here; a handler that cannot throw puts the
                // error aside and stops the scan, which is what the pollers in
                // `poller.rs` do with the same problem.
                self.error = Some(error);
                Action::Break
            }
        }
    }
}

/// Read one message and hand it to the listener, answering whether it was one of
/// the three events.
///
/// A free function because it is the half that can be tested with bytes and no
/// driver: [`RecordingEventsAdapter::on_message`] is this plus "what the scan does
/// with it". The three decoders are the schema's own, so what is pinned by feeding
/// it the bytes [`crate::server::recording_events`] writes is the *contract* rather
/// than one implementation agreeing with itself.
///
/// # Errors
///
/// [`EventsReadError`] as [`RecordingEventsAdapter::poll`].
pub fn dispatch(
    payload: &[u8],
    listener: &mut dyn RecordingEventsListener,
) -> Result<bool, EventsReadError> {
    let Some(header) = sbe_header(payload) else {
        return Err(EventsReadError::ShortMessage {
            length: payload.len(),
        });
    };

    let schema_id = header.schema_id();
    if schema_id != SBE_SCHEMA_ID {
        return Err(EventsReadError::UnexpectedSchemaId {
            expected: SBE_SCHEMA_ID,
            actual: schema_id,
        });
    }

    // The header's own acting block length and version, which is what SBE means by
    // them: a reader written against an older schema still reads a newer message.
    let block_length = header.block_length();
    let version = header.version();

    match header.template_id() {
        template::STARTED => {
            let mut decoder = RecordingStartedDecoder::default().wrap(
                ReadBuf::new(payload),
                ENCODED_LENGTH,
                block_length,
                version,
            );

            // The two strings are read **last**, and that order is not a
            // preference: `channel_slice` takes `&'a self` for the *buffer's*
            // lifetime (`recording_started_codec.rs:270-273`), so the first call to
            // it freezes the decoder. Coordinates first, slices after, and every
            // fixed field in between.
            let channel_at = decoder.channel_decoder();
            let identity_at = decoder.source_identity_decoder();

            let (recording_id, start_position, session_id, stream_id) = (
                decoder.recording_id(),
                decoder.start_position(),
                decoder.session_id(),
                decoder.stream_id(),
            );

            let channel = decoder.channel_slice(channel_at);
            let source_identity = decoder.source_identity_slice(identity_at);

            listener.on_start(
                recording_id,
                start_position,
                session_id,
                stream_id,
                &String::from_utf8_lossy(channel),
                &String::from_utf8_lossy(source_identity),
            );

            Ok(true)
        }
        template::PROGRESS => {
            let decoder = RecordingProgressDecoder::default().wrap(
                ReadBuf::new(payload),
                ENCODED_LENGTH,
                block_length,
                version,
            );

            listener.on_progress(
                decoder.recording_id(),
                decoder.start_position(),
                decoder.position(),
            );

            Ok(true)
        }
        template::STOPPED => {
            let decoder = RecordingStoppedDecoder::default().wrap(
                ReadBuf::new(payload),
                ENCODED_LENGTH,
                block_length,
                version,
            );

            listener.on_stop(
                decoder.recording_id(),
                decoder.start_position(),
                decoder.stop_position(),
            );

            Ok(true)
        }
        // "do nothing": a template this reader does not know is consumed and the
        // poll carries on, which is the reference's missing `default` arm.
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::server::recording_events::{encode_progress, encode_started, encode_stopped};

    /// One event, as a listener saw it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Seen {
        Start {
            recording_id: i64,
            start_position: i64,
            session_id: i32,
            stream_id: i32,
            channel: String,
            source_identity: String,
        },
        Progress {
            recording_id: i64,
            start_position: i64,
            position: i64,
        },
        Stop {
            recording_id: i64,
            start_position: i64,
            stop_position: i64,
        },
    }

    /// A listener that remembers.
    #[derive(Default)]
    struct Remembering(Vec<Seen>);

    impl RecordingEventsListener for Remembering {
        fn on_start(
            &mut self,
            recording_id: i64,
            start_position: i64,
            session_id: i32,
            stream_id: i32,
            channel: &str,
            source_identity: &str,
        ) {
            self.0.push(Seen::Start {
                recording_id,
                start_position,
                session_id,
                stream_id,
                channel: channel.to_owned(),
                source_identity: source_identity.to_owned(),
            });
        }

        fn on_progress(&mut self, recording_id: i64, start_position: i64, position: i64) {
            self.0.push(Seen::Progress {
                recording_id,
                start_position,
                position,
            });
        }

        fn on_stop(&mut self, recording_id: i64, start_position: i64, stop_position: i64) {
            self.0.push(Seen::Stop {
                recording_id,
                start_position,
                stop_position,
            });
        }
    }

    /// ★ **The three events the archive sends, read back by the client's own
    /// reader.** The bytes come from [`crate::server::recording_events`], which is
    /// what puts them on the wire, so this is the encoding and the decoding checked
    /// against each other rather than each against itself — the same claim
    /// `tests/interop/recording_events_reference.rs` makes with the reference's own
    /// adapter on the other side of a driver.
    #[test]
    fn the_three_events_decode_into_the_three_callbacks() {
        let mut listener = Remembering::default();
        let mut buffer = [0_u8; crate::server::recording_events::BUFFER_LENGTH];

        let length = encode_started(
            &mut buffer,
            7,
            1024,
            42,
            1001,
            "aeron:udp?endpoint=localhost:9000",
            "aeron:udp?endpoint=localhost:8000",
        );
        assert_eq!(Ok(true), dispatch(&buffer[..length], &mut listener));

        let length = encode_progress(&mut buffer, 7, 1024, 8192);
        assert_eq!(Ok(true), dispatch(&buffer[..length], &mut listener));

        let length = encode_stopped(&mut buffer, 7, 1024, 4096);
        assert_eq!(Ok(true), dispatch(&buffer[..length], &mut listener));

        assert_eq!(
            vec![
                Seen::Start {
                    recording_id: 7,
                    start_position: 1024,
                    session_id: 42,
                    stream_id: 1001,
                    channel: "aeron:udp?endpoint=localhost:9000".to_owned(),
                    source_identity: "aeron:udp?endpoint=localhost:8000".to_owned(),
                },
                Seen::Progress {
                    recording_id: 7,
                    start_position: 1024,
                    position: 8192,
                },
                Seen::Stop {
                    recording_id: 7,
                    start_position: 1024,
                    stop_position: 4096,
                },
            ],
            listener.0
        );
    }

    /// A template this reader does not know is **not** an event, and not an error
    /// either: the reference's `switch` has no `default`, so the message is consumed
    /// and the poll carries on.
    #[test]
    fn another_template_is_consumed_rather_than_read() {
        let mut listener = Remembering::default();
        let mut buffer = [0_u8; crate::server::recording_events::BUFFER_LENGTH];

        let length = encode_progress(&mut buffer, 7, 1024, 8192);
        // `RecordingProgress`'s id changed to one this reader does not read — which
        // is exactly what another archive's traffic on the same channel looks like.
        // The message header is `blockLength`, `templateId`, `schemaId`, `version`,
        // so the id is at 2.
        buffer[2..4].copy_from_slice(&9999_u16.to_le_bytes());

        assert_eq!(Ok(false), dispatch(&buffer[..length], &mut listener));
        assert!(
            listener.0.is_empty(),
            "a template nobody reads is a template nobody is told about"
        );
    }

    /// A message that is not one of the archive's at all is an error with the
    /// reference's own words, and a message too short to have a header is the other.
    #[test]
    fn a_foreign_schema_and_a_short_message_are_errors() {
        let mut listener = Remembering::default();
        let mut buffer = [0_u8; crate::server::recording_events::BUFFER_LENGTH];

        let length = encode_progress(&mut buffer, 7, 1024, 8192);
        buffer[4..6].copy_from_slice(&1234_u16.to_le_bytes());

        assert_eq!(
            Err(EventsReadError::UnexpectedSchemaId {
                expected: SBE_SCHEMA_ID,
                actual: 1234
            }),
            dispatch(&buffer[..length], &mut listener)
        );
        assert_eq!(
            "expected schemaId=101, actual=1234",
            EventsReadError::UnexpectedSchemaId {
                expected: SBE_SCHEMA_ID,
                actual: 1234
            }
            .to_string()
        );

        assert_eq!(
            Err(EventsReadError::ShortMessage { length: 4 }),
            dispatch(&buffer[..4], &mut listener)
        );
        assert!(listener.0.is_empty());
    }

    /// The three ids and the three versions are the schema's, read from the schema
    /// rather than from each other: a regenerated codec that moved one would
    /// otherwise have this reader agree with itself about a contract it no longer
    /// holds.
    #[test]
    fn the_reader_reads_the_schemas_own_templates() {
        assert_eq!(101, template::STARTED, "RecordingStarted");
        assert_eq!(102, template::PROGRESS, "RecordingProgress");
        assert_eq!(103, template::STOPPED, "RecordingStopped");
        assert_eq!(
            14,
            deepmsg_codec::archive::SBE_SCHEMA_VERSION,
            "the schema's acting version: the three events are read with the \
             header's, and a regenerated codec that moved it would be read as a \
             different layout"
        );
    }
}
