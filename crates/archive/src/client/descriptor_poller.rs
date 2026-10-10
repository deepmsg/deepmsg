//! The archive client's recording-descriptor poller: reading a listing
//! (`aeron_archive_recording_descriptor_poller.c`, 351 lines).
//!
//! A listing is not one answer. The archive sends a **stream** of
//! `RecordingDescriptor` (22) messages — one per recording, as many as fitted in
//! the page the request asked for — and then either a `RECORDING_UNKNOWN` (with
//! the request's correlation id) when there are no more, or simply nothing more
//! once the count the caller asked for has arrived. The poller is what turns
//! that stream into calls to a consumer, and it is why this is a poller and not
//! a wait: there is no single answer to wait for.
//!
//! # Two ways a listing ends, and they are different
//!
//! * **The consumer has seen `record_count` descriptors.** The request named how
//!   many it wanted, so when that many have gone to the consumer the listing is
//!   over — the archive owes nothing more and sends nothing more. This is the
//!   `--remaining_record_count == 0` arm (`:306-311`).
//! * **The archive said `RECORDING_UNKNOWN`** with this request's correlation id
//!   (`:194-200`) — there were fewer than asked for, and this is how it says so.
//!
//! Getting the first one wrong is a listing that waits for a message the archive
//! has no reason to send. That is the S3 slice's "the cursor is an id, not a page
//! number" seen from the other end.
//!
//! # The other two templates are handled here too, and differently
//!
//! A listing's poll reads the **same subscription** as every other operation, so
//! the same three templates arrive on it. A `ControlResponse` is read only for
//! its code — an `ERROR` for this request fails the listing, and an `ERROR` for
//! somebody else's goes to the error handler. A `RecordingSignalEvent` is
//! dispatched to the signal handler, because a listing can take long enough for
//! a recording to start and stop while it runs.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::{
    ControlResponseDecoder, SBE_BLOCK_LENGTH as CONTROL_RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_descriptor_codec::{
    RecordingDescriptorDecoder, SBE_BLOCK_LENGTH as RECORDING_DESCRIPTOR_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_signal_event_codec::{
    RecordingSignalEventDecoder, SBE_BLOCK_LENGTH as RECORDING_SIGNAL_EVENT_BLOCK_LENGTH,
};

use crate::client::poller::{ControlResponseError, holds, sbe_header};

/// `AERON_ARCHIVE_RECORDING_DESCRIPTOR_POLLER_FRAGMENT_LIMIT_DEFAULT` (`:21`).
pub const FRAGMENT_LIMIT_DEFAULT: usize = 10;

/// `AERON_ARCHIVE_CLIENT_RECORDING_DESCRIPTOR_SBE_TEMPLATE_ID`.
const RECORDING_DESCRIPTOR_TEMPLATE_ID: u16 = 22;
/// `…_CONTROL_RESPONSE_SBE_TEMPLATE_ID`.
const CONTROL_RESPONSE_TEMPLATE_ID: u16 = 1;
/// `…_RECORDING_SIGNAL_EVENT_SBE_TEMPLATE_ID`.
const RECORDING_SIGNAL_EVENT_TEMPLATE_ID: u16 = 24;

/// `RECORDING_UNKNOWN` (2), which is how a listing that ran short ends.
const RECORDING_UNKNOWN: ControlResponseCode = ControlResponseCode::RECORDING_UNKNOWN;

/// One recording, as the archive describes it
/// (`aeron_archive_recording_descriptor_t`).
///
/// The strings are `String` rather than the reference's three allocations and a
/// length each: the descriptor is built per message and handed to a consumer
/// that must not keep it, so owning them is the whole of what the C does with
/// `aeron_alloc`/`aeron_free` around the call (`:250-305`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingDescriptor {
    /// The session the listing was made on — **the poller's**, not the
    /// message's, which is what the reference copies (`:295-296`).
    pub control_session_id: i64,
    /// The request this descriptor belongs to, likewise.
    pub correlation_id: i64,
    /// The recording.
    pub recording_id: i64,
    /// When it started, in milliseconds.
    pub start_timestamp: i64,
    /// When it stopped, or `-1` if it has not.
    pub stop_timestamp: i64,
    /// The first position recorded.
    pub start_position: i64,
    /// The last, or the current one for a live recording.
    pub stop_position: i64,
    /// The term the recording's log began in.
    pub initial_term_id: i32,
    /// How long each segment file is.
    pub segment_file_length: i32,
    /// The term buffer length the recording was made with.
    pub term_buffer_length: i32,
    /// Its MTU.
    pub mtu_length: i32,
    /// The session of the publication that was recorded.
    pub session_id: i32,
    /// The stream.
    pub stream_id: i32,
    /// The channel with the parameters stripped off — what a caller matches on.
    pub stripped_channel: String,
    /// The channel as it was given.
    pub original_channel: String,
    /// Who published it, as the image named itself.
    pub source_identity: String,
}

/// What a listing does with each descriptor it reads.
///
/// A plain function pointer, like [`crate::client::archive::ErrorHandler`]: the
/// reference's consumer gets a `clientd` and this one cannot, so a consumer with
/// state uses a `static`. The descriptor is borrowed — a consumer must copy what
/// it wants to keep, which is what the reference's `aeron_free` after the call
/// says in C.
pub type RecordingDescriptorConsumer = fn(&RecordingDescriptor);

/// Reads a listing, one descriptor at a time.
#[derive(Debug)]
pub struct RecordingDescriptorPoller {
    /// The subscription every operation reads.
    subscription: i64,
    /// The session whose descriptors are this client's.
    control_session_id: i64,
    /// How many messages one poll may read.
    fragment_limit: usize,
    /// The request being listed. Set by [`Self::reset`].
    correlation_id: i64,
    /// How many more descriptors the caller is waiting for (`:61`).
    remaining_record_count: i32,
    /// Where each one goes.
    consumer: Option<RecordingDescriptorConsumer>,
    /// Where recording signals go while a listing is running.
    signal_handler: crate::client::archive::RecordingSignalHandler,
    /// The listing is over (`:62`) — either the count ran out or the archive
    /// said there are no more.
    is_dispatch_complete: bool,
    /// A message could not be read. See [`ControlResponseError`].
    error_on_message: bool,
}

impl RecordingDescriptorPoller {
    /// A poller over `subscription`, configured for `control_session_id`
    /// (`aeron_archive_recording_descriptor_poller_create`, `:32-74`).
    #[must_use]
    pub fn new(
        subscription: i64,
        control_session_id: i64,
        fragment_limit: usize,
        signal_handler: crate::client::archive::RecordingSignalHandler,
    ) -> Self {
        Self {
            subscription,
            control_session_id,
            fragment_limit,
            correlation_id: -1,
            remaining_record_count: 0,
            consumer: None,
            signal_handler,
            is_dispatch_complete: false,
            error_on_message: false,
        }
    }

    /// Arm it for one listing (`aeron_archive_recording_descriptor_poller_reset`,
    /// `:86-100`).
    pub fn reset(
        &mut self,
        correlation_id: i64,
        record_count: i32,
        consumer: RecordingDescriptorConsumer,
    ) {
        self.error_on_message = false;
        self.correlation_id = correlation_id;
        self.remaining_record_count = record_count;
        self.consumer = Some(consumer);
        self.is_dispatch_complete = false;
    }

    /// Whether the listing is over (`:62`).
    #[must_use]
    pub const fn is_dispatch_complete(&self) -> bool {
        self.is_dispatch_complete
    }

    /// The request this poller is listing for.
    #[must_use]
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// How many more descriptors the caller is waiting for (`:61`).
    ///
    /// The listing's deadline is re-armed whenever this changes, so a caller
    /// that drives the poll itself needs it.
    #[must_use]
    pub const fn remaining_record_count(&self) -> i32 {
        self.remaining_record_count
    }

    /// The subscription descriptors arrive on.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// Read up to `fragment_limit` messages (`:101-119`).
    ///
    /// # Errors
    ///
    /// [`ControlResponseError`] when a message on the subscription was
    /// malformed.
    pub fn poll(&mut self, client: &mut Client) -> Result<usize, ControlResponseError> {
        if self.is_dispatch_complete {
            self.is_dispatch_complete = false;
        }

        let subscription = self.subscription;
        let fragment_limit = self.fragment_limit;

        let fragments = client.poll_subscription_controlled(subscription, fragment_limit, self);

        if self.error_on_message {
            return Err(ControlResponseError::MalformedMessage);
        }

        Ok(fragments)
    }
}

impl ControlledHandler for RecordingDescriptorPoller {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        if self.is_dispatch_complete {
            return Action::Abort;
        }

        let payload = message.payload;

        let Some(header) = sbe_header(payload) else {
            self.error_on_message = true;
            return Action::Break;
        };

        if header.schema_id() != deepmsg_codec::archive::SBE_SCHEMA_ID {
            self.error_on_message = true;
            return Action::Break;
        }

        match header.template_id() {
            CONTROL_RESPONSE_TEMPLATE_ID => {
                if !holds(payload, CONTROL_RESPONSE_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let response = ControlResponseDecoder::default().header(header, 0);

                if response.control_session_id() != self.control_session_id {
                    return Action::Continue;
                }

                let correlation_id = response.correlation_id();

                // **"There are no more"**: the archive answering a listing that
                // ran short. It is not an error and it is not a descriptor — it
                // is the end (`:194-200`).
                if RECORDING_UNKNOWN == response.code() && correlation_id == self.correlation_id {
                    self.is_dispatch_complete = true;

                    return Action::Break;
                }

                if ControlResponseCode::ERROR == response.code()
                    && correlation_id == self.correlation_id
                {
                    // The one failure this poller raises, and it is raised the
                    // way the reference raises it: a flag the poll turns into an
                    // error, because a handler cannot return one.
                    self.error_on_message = true;

                    return Action::Break;
                }

                // Somebody else's refusal is consumed here and the listing goes
                // on. The reference hands it to the error handler
                // (`:217-227`); this poller has none — the handler lives on the
                // `Archive` — so it is dropped rather than dispatched, which is
                // recorded rather than hidden behind a null check.
                Action::Continue
            }
            RECORDING_DESCRIPTOR_TEMPLATE_ID => {
                if !holds(payload, RECORDING_DESCRIPTOR_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let mut descriptor = RecordingDescriptorDecoder::default().header(header, 0);

                if descriptor.control_session_id() != self.control_session_id
                    || descriptor.correlation_id() != self.correlation_id
                {
                    return Action::Continue;
                }

                // The three variable fields, **in the schema's declaration
                // order** — the accessors are a cursor that advances, so a
                // later read of an earlier field is a different field's bytes.
                let stripped = descriptor.stripped_channel_decoder();
                let original = descriptor.original_channel_decoder();
                let identity = descriptor.source_identity_decoder();

                let recording = RecordingDescriptor {
                    control_session_id: self.control_session_id,
                    correlation_id: self.correlation_id,
                    recording_id: descriptor.recording_id(),
                    start_timestamp: descriptor.start_timestamp(),
                    stop_timestamp: descriptor.stop_timestamp(),
                    start_position: descriptor.start_position(),
                    stop_position: descriptor.stop_position(),
                    initial_term_id: descriptor.initial_term_id(),
                    segment_file_length: descriptor.segment_file_length(),
                    term_buffer_length: descriptor.term_buffer_length(),
                    mtu_length: descriptor.mtu_length(),
                    session_id: descriptor.session_id(),
                    stream_id: descriptor.stream_id(),
                    stripped_channel: String::from_utf8_lossy(
                        descriptor.stripped_channel_slice(stripped),
                    )
                    .into_owned(),
                    original_channel: String::from_utf8_lossy(
                        descriptor.original_channel_slice(original),
                    )
                    .into_owned(),
                    source_identity: String::from_utf8_lossy(
                        descriptor.source_identity_slice(identity),
                    )
                    .into_owned(),
                };

                if let Some(consumer) = self.consumer {
                    consumer(&recording);
                }

                self.remaining_record_count -= 1;
                if 0 == self.remaining_record_count {
                    // The caller asked for exactly this many, so the archive
                    // owes nothing more and will not send anything more.
                    self.is_dispatch_complete = true;

                    return Action::Break;
                }

                Action::Continue
            }
            RECORDING_SIGNAL_EVENT_TEMPLATE_ID => {
                if !holds(payload, RECORDING_SIGNAL_EVENT_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let signal = RecordingSignalEventDecoder::default().header(header, 0);

                if signal.control_session_id() == self.control_session_id {
                    (self.signal_handler)(&crate::client::archive::RecordingSignal {
                        control_session_id: signal.control_session_id(),
                        recording_id: signal.recording_id(),
                        subscription_id: signal.subscription_id(),
                        position: signal.position(),
                        signal: signal.signal(),
                    });
                }

                Action::Continue
            }
            // A template this poller does not read: consumed, and the listing
            // goes on.
            _ => Action::Continue,
        }
    }
}
