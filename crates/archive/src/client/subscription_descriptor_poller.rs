//! The archive client's recording-**subscription** descriptor poller: reading a
//! listing of the archive's subscriptions
//! (`aeron_archive_recording_subscription_descriptor_poller.c`, 315 lines).
//!
//! [`crate::client::descriptor_poller`]'s twin, and the differences are the
//! whole of what is worth saying here:
//!
//! * What is listed is a **subscription** rather than a recording, so the
//!   descriptor is three fields and no positions, and what the archive answers
//!   when it has none is `SUBSCRIPTION_UNKNOWN` (3) rather than
//!   `RECORDING_UNKNOWN` (2).
//! * The count is `remaining_subscription_count`, which is what a caller asked
//!   `listRecordingSubscriptions` for — so the deadline in
//!   `aeron_archive_poll_for_subscription_descriptors` (`:2201-2262`) re-arms the
//!   same way the descriptor listing's does.
//!
//! Everything else — one slot per poll, the three templates, the deviation
//! recorded in [`ControlResponsePoller`](crate::client::poller::ControlResponsePoller)
//! about `error_on_message` being sticky — is the twin's, and is documented
//! there rather than twice.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::{
    ControlResponseDecoder, SBE_BLOCK_LENGTH as CONTROL_RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_signal_event_codec::{
    RecordingSignalEventDecoder, SBE_BLOCK_LENGTH as RECORDING_SIGNAL_EVENT_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_subscription_descriptor_codec::{
    RecordingSubscriptionDescriptorDecoder,
    SBE_BLOCK_LENGTH as SUBSCRIPTION_DESCRIPTOR_BLOCK_LENGTH,
};

use crate::client::poller::{ControlResponseError, holds, sbe_header};

/// `AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_POLLER_FRAGMENT_LIMIT_DEFAULT`
/// (`:24` of the poller's header, not of the client).
pub const FRAGMENT_LIMIT_DEFAULT: usize = 10;

/// `…_RECORDING_SUBSCRIPTION_DESCRIPTOR_SBE_TEMPLATE_ID`.
const RECORDING_SUBSCRIPTION_DESCRIPTOR_TEMPLATE_ID: u16 = 23;
/// `…_CONTROL_RESPONSE_SBE_TEMPLATE_ID`.
const CONTROL_RESPONSE_TEMPLATE_ID: u16 = 1;
/// `…_RECORDING_SIGNAL_EVENT_SBE_TEMPLATE_ID`.
const RECORDING_SIGNAL_EVENT_TEMPLATE_ID: u16 = 24;

/// `SUBSCRIPTION_UNKNOWN` (3), which is how a listing that ran short ends.
const SUBSCRIPTION_UNKNOWN: ControlResponseCode = ControlResponseCode::SUBSCRIPTION_UNKNOWN;

/// One subscription, as the archive describes it
/// (`aeron_archive_recording_subscription_descriptor_t`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingSubscriptionDescriptor {
    /// The session the listing was made on — **the poller's**, not the
    /// message's, which is what the reference copies.
    pub control_session_id: i64,
    /// The request this descriptor belongs to, likewise.
    pub correlation_id: i64,
    /// The subscription the archive opened to record this channel and stream.
    pub subscription_id: i64,
    /// The stream it was made on.
    pub stream_id: i32,
    /// The channel with the parameters stripped off.
    pub stripped_channel: String,
}

/// What a listing does with each descriptor it reads.
///
/// A plain function pointer, for the reason
/// [`RecordingDescriptorConsumer`](crate::client::descriptor_poller::RecordingDescriptorConsumer)
/// is one.
pub type RecordingSubscriptionDescriptorConsumer = fn(&RecordingSubscriptionDescriptor);

/// Reads a listing of the archive's subscriptions, one descriptor at a time.
#[derive(Debug)]
pub struct RecordingSubscriptionDescriptorPoller {
    /// The subscription every operation reads.
    subscription: i64,
    /// The session whose descriptors are this client's.
    control_session_id: i64,
    /// How many messages one poll may read.
    fragment_limit: usize,
    /// The request being listed. Set by [`Self::reset`].
    correlation_id: i64,
    /// How many more descriptors the caller is waiting for.
    remaining_subscription_count: i32,
    /// Where each one goes.
    consumer: Option<RecordingSubscriptionDescriptorConsumer>,
    /// Where recording signals go while a listing is running.
    signal_handler: crate::client::archive::RecordingSignalHandler,
    /// The listing is over — either the count ran out or the archive said there
    /// are no more.
    is_dispatch_complete: bool,
    /// A message could not be read. See [`ControlResponseError`].
    error_on_message: bool,
}

impl RecordingSubscriptionDescriptorPoller {
    /// A poller over `subscription`, configured for `control_session_id`
    /// (`aeron_archive_recording_subscription_descriptor_poller_create`,
    /// `:38-77`).
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
            remaining_subscription_count: 0,
            consumer: None,
            signal_handler,
            is_dispatch_complete: false,
            error_on_message: false,
        }
    }

    /// Arm it for one listing
    /// (`aeron_archive_recording_subscription_descriptor_poller_reset`, `:80-93`).
    ///
    /// The reference's reset does not clear `is_dispatch_complete`, and this one
    /// does. It is not observable: [`Self::poll`] clears it before it reads
    /// anything, and the reference's own caller polls before it looks
    /// (`aeron_archive_poll_for_subscription_descriptors`, `:2236-2244`).
    pub fn reset(
        &mut self,
        correlation_id: i64,
        subscription_count: i32,
        consumer: RecordingSubscriptionDescriptorConsumer,
    ) {
        self.error_on_message = false;
        self.correlation_id = correlation_id;
        self.remaining_subscription_count = subscription_count;
        self.consumer = Some(consumer);
        self.is_dispatch_complete = false;
    }

    /// Whether the listing is over.
    #[must_use]
    pub const fn is_dispatch_complete(&self) -> bool {
        self.is_dispatch_complete
    }

    /// The request this poller is listing for.
    #[must_use]
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// How many more descriptors the caller is waiting for.
    ///
    /// The listing's deadline is re-armed whenever this changes, so a caller
    /// that drives the poll itself needs it.
    #[must_use]
    pub const fn remaining_subscription_count(&self) -> i32 {
        self.remaining_subscription_count
    }

    /// The subscription descriptors arrive on.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// Read up to `fragment_limit` messages (`:95-119`).
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

impl ControlledHandler for RecordingSubscriptionDescriptorPoller {
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
                // ran short.
                if SUBSCRIPTION_UNKNOWN == response.code() && correlation_id == self.correlation_id
                {
                    self.is_dispatch_complete = true;

                    return Action::Break;
                }

                if ControlResponseCode::ERROR == response.code()
                    && correlation_id == self.correlation_id
                {
                    self.error_on_message = true;

                    return Action::Break;
                }

                Action::Continue
            }
            RECORDING_SUBSCRIPTION_DESCRIPTOR_TEMPLATE_ID => {
                if !holds(payload, SUBSCRIPTION_DESCRIPTOR_BLOCK_LENGTH) {
                    self.error_on_message = true;
                    return Action::Break;
                }

                let mut descriptor =
                    RecordingSubscriptionDescriptorDecoder::default().header(header, 0);

                if descriptor.control_session_id() != self.control_session_id
                    || descriptor.correlation_id() != self.correlation_id
                {
                    return Action::Continue;
                }

                // The one variable field, read **there and then**: the accessor
                // is a cursor, so it is read before anything else advances it.
                let stripped = descriptor.stripped_channel_decoder();

                let subscription = RecordingSubscriptionDescriptor {
                    control_session_id: self.control_session_id,
                    correlation_id: self.correlation_id,
                    subscription_id: descriptor.subscription_id(),
                    stream_id: descriptor.stream_id(),
                    stripped_channel: String::from_utf8_lossy(
                        descriptor.stripped_channel_slice(stripped),
                    )
                    .into_owned(),
                };

                if let Some(consumer) = self.consumer {
                    consumer(&subscription);
                }

                self.remaining_subscription_count -= 1;
                if 0 == self.remaining_subscription_count {
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
            _ => Action::Continue,
        }
    }
}
