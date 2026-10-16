//! The client end of a **live recording** measurement: send, and read the replay
//! paced by what the archive reports recording.
//!
//! Mirrors `benchmarks-aeron/.../LiveRecordingMessageTransceiver.java`. Its
//! neighbour [`LiveReplayTransceiver`](crate::loadtest::transport::live_replay::LiveReplayTransceiver)
//! is the same shape over an *existing* recording; this one records the stream it is
//! sending into, and the difference is the pacemaker:
//!
//! * it subscribes to the archive's **recording events**
//!   ([`crate::client::recording_events`]), which is what the recording channel is
//!   for;
//! * every `receive` first asks whether there is anything new to read — the two
//!   positions it compares are the archive's recorded position and how much of it
//!   this end has consumed — and returns without reading when they agree;
//! * and the read itself **stops** at that boundary rather than running to whatever
//!   the stream has.
//!
//! The read is over the **subscription** rather than over one image as the
//! reference's is (`:161`, `image.controlledPoll`): this build's controlled door,
//! which is the one that can leave a message unconsumed, is a subscription's
//! (`Client::poll_subscription_controlled`). One subscription carries one image for
//! the whole of a run — the replay of one recording — so the two read the same
//! messages.
//!
//! # ★ What the two positions are, and what the reference's arithmetic does
//!
//! `recording_position` is the archive's: `RecordingProgress.position`, the byte the
//! recording has been written up to. `recording_position_consumed` starts at
//! `NULL_POSITION` and is set to the recording's **start position** by the first
//! progress event (`:207-211`), so the first `receive` of a run compares `-1` with
//! `-1`, reads no events, and returns: **that is the pacemaker**, and it is what
//! makes the client wait for the archive rather than for the stream.
//!
//! Then each message it does read advances `recording_position_consumed` by
//! `align(length, FRAME_ALIGNMENT)` (`:187`) — where `length` is the **payload** the
//! handler was given (`Image.java:610-611` passes `length - HEADER_LENGTH`, and
//! `DataHeaderFlyweight.HEADER_LENGTH` is 32), while the archive's position counts
//! the **frame**: `HEADER_LENGTH + payload`, aligned. The two therefore advance at
//! different rates, and after the first event the equality this end tests for is not
//! reached again — the read is no longer cut short, and the client takes whatever the
//! stream has.
//!
//! **This build copies that arithmetic rather than correcting it** (`:187` is the
//! line, and the divergence is the reference's), because the two arms of the
//! archive-vs-Java batch have to pace the same way for their numbers to be
//! comparable. It is written down here because a reader of the numbers should know
//! that the fine-grained pacing is not what it looks like — and it is on the list of
//! things to measure rather than assume.

use std::time::Instant;

use deepmsg_archive::client::archive::{
    Archive as ArchiveClient, Handlers, channel_with_session_id,
};
use deepmsg_archive::client::recording_events::{
    EventsReadError, RecordingEventsAdapter, RecordingEventsListener,
};
use deepmsg_archive::client::{ArchiveContext, NoCredentials};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::fragment_assembler::{Action, Message};
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::position::align_up;

use crate::loadtest::config::{Configuration, IdleStrategy};
use crate::loadtest::recorder::Recorder;
use crate::loadtest::transceiver::{Clock, MessageTransceiver, SystemClock, TransceiverError};
use crate::loadtest::transport::echo::payload_fields;
use crate::loadtest::transport::sender::{Kind, MessageSender, PublicationOutbound};
use crate::loadtest::transport::util::{
    ChannelSettings, MIN_MESSAGE_LENGTH, await_connected, images, window_limit,
};

/// `Aeron.NULL_VALUE`, which is where both positions start
/// (`LiveRecordingMessageTransceiver:48`, `:210`): a recording that has not been
/// reported on yet is one this end has neither read nor been told about.
const NULL_POSITION: i64 = -1;

/// The transceiver a run uses when the archive records the stream it sends into
/// (`LiveRecordingMessageTransceiver`).
pub struct LiveRecordingTransceiver {
    settings: ChannelSettings,
    client: Client,
    archive: ArchiveClient,
    /// Where a batch is sent: the channel the node reads, and the one recorded.
    publication: i64,
    /// The replay of that recording, which is what the client reads.
    subscription: i64,
    /// The archive's recording events, which is the pacemaker.
    events_subscription: i64,
    /// The channel that pacemaker is on, out of the context the run was given.
    events_channel: Option<String>,
    /// And its stream.
    events_stream: i32,
    /// The recording this run is making, once the archive reports a counter.
    recording_id: i64,
    /// Where the archive says the recording has got to.
    recording_position: i64,
    /// How much of it this end has taken.
    recording_position_consumed: i64,
    window: i64,
    sender: MessageSender,
}

impl LiveRecordingTransceiver {
    /// Connect to the driver and the archive the settings name.
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the driver or the archive is not where the settings
    /// say, or when either refuses this client.
    pub fn new(
        settings: ChannelSettings,
        idle: IdleStrategy,
        archive_context: &ArchiveContext,
    ) -> Result<Self, TransceiverError> {
        let mut client =
            Client::connect(&settings.directory).map_err(|error| TransceiverError::Failed {
                action: "connect to the driver",
                message: error.to_string(),
            })?;

        let archive = ArchiveClient::connect(
            archive_context,
            &mut client,
            &mut NoCredentials,
            Handlers::default(),
        )
        .map_err(|error| TransceiverError::Failed {
            action: "connect to the archive",
            message: error.to_string(),
        })?;

        let sender = MessageSender::new(
            if settings.use_try_claim {
                Kind::Claim
            } else {
                Kind::Offer
            },
            idle,
            settings.receiver_count,
        );

        Ok(Self {
            settings,
            client,
            archive,
            publication: 0,
            subscription: 0,
            events_subscription: 0,
            events_channel: archive_context.recording_events_channel.clone(),
            events_stream: archive_context.recording_events_stream_id,
            recording_id: NULL_POSITION,
            recording_position: NULL_POSITION,
            recording_position_consumed: NULL_POSITION,
            window: 0,
            sender,
        })
    }

    /// The recording this run is making, once [`Self::init`] has found it.
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Where the archive says the recording has got to.
    #[must_use]
    pub const fn recording_position(&self) -> i64 {
        self.recording_position
    }

    /// How much of it this end has taken.
    #[must_use]
    pub const fn recording_position_consumed(&self) -> i64 {
        self.recording_position_consumed
    }

    /// Read whatever the events channel has, which is where both positions come
    /// from.
    ///
    /// # Errors
    ///
    /// [`EventsReadError`] when a message on the events channel could not be read.
    fn poll_events(&mut self) -> Result<usize, EventsReadError> {
        let subscription = self.events_subscription;
        let fragment_limit = self.settings.fragment_limit;
        let recording_id = self.recording_id;

        // The two positions are borrowed by the listener for the length of the poll,
        // and the client is borrowed by the poll: separate fields, so they go apart.
        let mut listener = Pacemaker {
            recording_id,
            position: &mut self.recording_position,
            consumed: &mut self.recording_position_consumed,
        };

        RecordingEventsAdapter::new(&mut listener, subscription, fragment_limit)
            .poll(&mut self.client)
    }

    /// Ask the archive for the counter of the recording of this publication, waiting
    /// for it to appear.
    ///
    /// The same wait the archive node makes, for the same reason and on the same
    /// counter — and, like that one, with a deadline the reference does not have.
    fn await_recording_id(&mut self) -> Result<i64, TransceiverError> {
        let deadline = Instant::now() + self.settings.connection_timeout;
        let archive_id = self.archive.archive_id();
        let session_id = self
            .client
            .exclusive_publication(self.publication)
            .map(|publication| publication.session_id())
            .ok_or_else(|| TransceiverError::Failed {
                action: "read the publication session id",
                message: "the driver answered with a publication this client does not hold"
                    .to_owned(),
            })?;
        let client = &mut self.client;

        loop {
            client.poll();

            if let Some(recording_id) = client.counters_reader().and_then(|counters| {
                deepmsg_archive::server::recording_pos::find_counter_id_by_session(
                    &counters, session_id, archive_id,
                )
                .and_then(|counter_id| {
                    counters
                        .key(counter_id)
                        .and_then(|key| deepmsg_archive::server::recording_pos::parse_key(&key))
                })
                .map(|identity| identity.recording_id)
            }) {
                return Ok(recording_id);
            }

            if Instant::now() >= deadline {
                return Err(TransceiverError::Failed {
                    action: "wait for the recording to start",
                    message: "no recording position counter appeared for this publication"
                        .to_owned(),
                });
            }

            std::thread::yield_now();
        }
    }
}

impl<C: Clock> MessageTransceiver<C> for LiveRecordingTransceiver {
    fn init(&mut self, configuration: &Configuration) -> Result<(), TransceiverError> {
        let message_length = usize::try_from(configuration.message_length()).unwrap_or(usize::MAX);
        if message_length < MIN_MESSAGE_LENGTH {
            return Err(TransceiverError::Failed {
                action: "init",
                message: format!("Message length must be at least {MIN_MESSAGE_LENGTH}"),
            });
        }

        let destination_channel = self.settings.destination_channel.clone();
        let destination_stream = self.settings.destination_stream;
        let source_channel = self.settings.source_channel.clone();
        let source_stream = self.settings.source_stream;

        self.publication = self
            .client
            .add_exclusive_publication(&destination_channel, destination_stream, DEFAULT_TIMEOUT)
            .map_err(|error| TransceiverError::Failed {
                action: "add the publication",
                message: error.to_string(),
            })?;

        self.subscription = self
            .client
            .add_subscription(&source_channel, source_stream, DEFAULT_TIMEOUT)
            .map_err(|error| TransceiverError::Failed {
                action: "add the subscription",
                message: error.to_string(),
            })?;

        // ★ The pacemaker's channel. Without it this end has no way to be told what
        // the archive has recorded, which is the whole of what this transceiver does
        // differently from a live reader — so a deployment that has not named one is
        // refused here rather than read from at full speed.
        let events_channel =
            self.events_channel
                .clone()
                .ok_or_else(|| TransceiverError::Failed {
                    action: "init",
                    message:
                        "a live-recording run has to name the archive's recording events channel \
                          (aeron.archive.recording.events.channel): without it there is nothing to \
                          pace the read against"
                            .to_owned(),
                })?;
        let events_stream = self.events_stream;

        self.events_subscription = self
            .client
            .add_subscription(&events_channel, events_stream, DEFAULT_TIMEOUT)
            .map_err(|error| TransceiverError::Failed {
                action: "add the recording events subscription",
                message: error.to_string(),
            })?;

        let (publication, subscription, events) = (
            self.publication,
            self.subscription,
            self.events_subscription,
        );
        await_connected(
            || {
                self.client.poll();

                publication_connected(&self.client, publication)
                    && images(&self.client, subscription) > 0
                    && subscription_connected(&self.client, events)
            },
            self.settings.connection_timeout,
            &SystemClock,
        )
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the node, the replay and the events",
            message: error.to_string(),
        })?;

        let session_id = self
            .client
            .exclusive_publication(publication)
            .map(|publication| publication.session_id())
            .ok_or_else(|| TransceiverError::Failed {
                action: "read the publication session id",
                message: "the driver answered with a publication this client does not hold"
                    .to_owned(),
            })?;

        let channel =
            channel_with_session_id(&destination_channel, session_id).map_err(|error| {
                TransceiverError::Failed {
                    action: "name the publication in the recorded channel",
                    message: error.to_string(),
                }
            })?;

        self.archive
            .start_recording(
                &mut self.client,
                &channel,
                destination_stream,
                true,
                // `autoStop`, the reference's `true` (`:132-135`).
                true,
            )
            .map_err(|error| TransceiverError::Failed {
                action: "start the recording",
                message: error.to_string(),
            })?;

        self.recording_id = self.await_recording_id()?;

        self.window = window_limit(&self.client, self.publication);

        Ok(())
    }

    fn destroy(&mut self) -> Result<(), TransceiverError> {
        self.client.close();

        Ok(())
    }

    fn send(
        &mut self,
        number_of_messages: usize,
        message_length: usize,
        timestamp: i64,
        checksum: i64,
        _recorder: &mut Recorder<C>,
    ) -> usize {
        let registration_id = self.publication;
        let window = &mut self.window;
        let client: &Client = &self.client;
        let sender = &mut self.sender;

        let mut outbound = PublicationOutbound {
            client,
            publication: client.exclusive_publication(registration_id),
            registration_id,
            window,
        };

        sender.send(
            &mut outbound,
            number_of_messages,
            message_length,
            timestamp,
            checksum,
        )
    }

    fn receive(&mut self, recorder: &mut Recorder<C>) {
        // The pacemaker, as the reference writes it (`:155-160`): with nothing new
        // recorded there is nothing to read, and the events are what says so.
        if self.recording_position_consumed == self.recording_position {
            let _ = self.poll_events();

            if self.recording_position_consumed == self.recording_position {
                return;
            }
        }

        let subscription = self.subscription;
        let fragment_limit = self.settings.fragment_limit;
        let position = self.recording_position;
        let consumed = &mut self.recording_position_consumed;

        self.client.poll_subscription_controlled(
            subscription,
            fragment_limit,
            &mut |message: Message<'_>| {
                if *consumed == position {
                    // `ABORT` and not a break: the message is left **unconsumed**, so
                    // the next turn finds it again once the archive has recorded it.
                    // Consuming it here would drop it, and the run would be missing
                    // messages that nothing ever sends again.
                    return Action::Abort;
                }

                if let Some((timestamp, checksum)) = payload_fields(message.payload) {
                    recorder.on_message_received(timestamp, checksum);
                }

                *consumed += i64::from(align_up(
                    i32::try_from(message.payload.len()).unwrap_or(i32::MAX),
                    FRAME_ALIGNMENT,
                ));

                Action::Continue
            },
        );
    }
}

/// The two positions, as the events listener the poll drives.
///
/// A struct of borrows rather than a method on the transceiver because the poll
/// holds the client mutably while the listener runs — the positions and the client
/// are separate fields, so they are borrowed apart.
struct Pacemaker<'a> {
    recording_id: i64,
    position: &'a mut i64,
    consumed: &'a mut i64,
}

impl RecordingEventsListener for Pacemaker<'_> {
    fn on_start(
        &mut self,
        _recording_id: i64,
        _start_position: i64,
        _session_id: i32,
        _stream_id: i32,
        _channel: &str,
        _source_identity: &str,
    ) {
        // The reference's listener says nothing about a recording starting
        // (`:201-212`): what a client can read is what **progress** reports.
    }

    fn on_progress(&mut self, recording_id: i64, start_position: i64, position: i64) {
        if recording_id != self.recording_id {
            // Another recording on the same channel, which this end is not reading.
            return;
        }

        if NULL_POSITION == *self.consumed {
            // Where the recording *began*, which is where this end starts from
            // (`:208-211`). Until an event says, both positions are `NULL_POSITION`
            // and — being equal — the read is not attempted at all.
            *self.consumed = start_position;
        }

        *self.position = position;
    }

    fn on_stop(&mut self, recording_id: i64, _start_position: i64, stop_position: i64) {
        if recording_id == self.recording_id {
            *self.position = stop_position;
        }
    }
}

/// Whether a publication is connected, which the reference asks first (`:124-131`).
fn publication_connected(client: &Client, publication: i64) -> bool {
    client
        .exclusive_publication(publication)
        .is_some_and(|publication| publication.is_connected().unwrap_or(false))
}

/// Whether a subscription is connected, which the reference asks of two of them.
fn subscription_connected(client: &Client, subscription: i64) -> bool {
    client
        .subscription(subscription)
        .is_some_and(deepmsg_client::subscription::Subscription::is_connected)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ★ **The pacemaker, both halves.** Before an event both positions are
    /// `NULL_POSITION` and the read is not attempted; the first progress for *this*
    /// recording moves both, and one for another recording moves neither.
    #[test]
    fn the_first_progress_starts_the_recording_this_end_is_reading() {
        let (mut position, mut consumed) = (NULL_POSITION, NULL_POSITION);
        let mut pacemaker = Pacemaker {
            recording_id: 7,
            position: &mut position,
            consumed: &mut consumed,
        };

        pacemaker.on_progress(9, 4096, 8192);
        assert_eq!(
            (NULL_POSITION, NULL_POSITION),
            (*pacemaker.position, *pacemaker.consumed),
            "another recording's progress is not this end's to act on"
        );

        pacemaker.on_progress(7, 4096, 8192);
        assert_eq!(
            (8192, 4096),
            (*pacemaker.position, *pacemaker.consumed),
            "the archive's position is where it says, and this end starts at the \\
             recording's beginning rather than at zero"
        );

        pacemaker.on_progress(7, 4096, 16384);
        assert_eq!(
            4096, *pacemaker.consumed,
            "a later event moves where the recording is, not where this end has read to"
        );

        pacemaker.on_stop(7, 4096, 32768);
        assert_eq!(
            32768, *pacemaker.position,
            "and a stop is the last position"
        );
    }

    /// ★ The arithmetic the increment is made with is the reference's, and it is the
    /// **payload** aligned rather than the frame — see the module note for why that
    /// is copied rather than corrected, and for what it does to the pacing.
    #[test]
    fn a_consumed_message_advances_by_its_aligned_payload() {
        for (payload, expected) in [(40, 64), (32, 32), (1, 32), (64, 64)] {
            assert_eq!(
                expected,
                align_up(payload, FRAME_ALIGNMENT),
                "{payload} bytes of payload advance the consumed position by the \\
                 reference's expression (`:187`)"
            );
        }

        assert_eq!(
            32,
            deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT,
            "and the alignment is the log's frame alignment, not a byte count"
        );
    }
}
