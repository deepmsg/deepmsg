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

use deepmsg_core::logbuffer::append::{Appended, Appender};
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
    pub fn is_connected(&self) -> Option<bool> {
        self.appender().and_then(|appender| appender.is_connected())
    }

    /// Append `payload` as one frame, if `position_limit` allows it.
    ///
    /// The limit is passed in rather than read here because the counter lives
    /// in the CnC file, which this type does not own — see
    /// [`crate::Client::offer`], which reads it and calls this.
    ///
    /// # Errors
    ///
    /// See [`Appended`]. `EndOfLog` is not a failure: the log rotated and the
    /// caller retries into the new term.
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
