//! The client end of an archive measurement: send to a node, read the **recording**
//! back.
//!
//! Mirrors `benchmarks-aeron/.../LiveReplayMessageTransceiver.java`. Its neighbour
//! [`EchoTransceiver`](crate::loadtest::transport::echo::EchoTransceiver) reads what a
//! node answers; this one reads a **replay** — the archive's own copy of what the node
//! forwarded — so the round trip it measures is a byte that went to disk and came back.
//!
//! What it does at `init` is the whole of the shape, and it is the reference's order
//! (`:91-119`):
//!
//! 1. an exclusive publication on `destination_channel`, which is what the node reads;
//! 2. ★ **the last recording of `record_channel`**, found by asking the archive
//!    (`AeronUtil.findLastRecordingId:290-317`) — so the run measures the recording the
//!    node of *this* run is making, which is why the node starts before the client;
//! 3. a full replay of it onto `source_channel`, and a subscription on that channel
//!    naming the replay's **session id** — the same `addSessionId` the node used, and
//!    what makes this subscription read this replay rather than any other stream on the
//!    channel;
//! 4. the image that subscription formed, which is what [`Self::receive`] reads.
//!
//! # The waits, and which of them the reference does not have
//!
//! `findLastRecordingId` polls in a `do { … } while (0 == foundCount)` and
//! `replayFullRecording` retries a failed `startReplay` for ever: both are "the world
//! will become ready", written without a bound because the reference starts the node
//! and the archive in the same process. This one is given the connection timeout, the
//! same way [`ArchiveNode`](crate::loadtest::transport::archive_node::ArchiveNode)'s
//! wait for the recording is — a node that never came up is a message rather than a
//! client that stops answering.

use std::time::Instant;

use deepmsg_archive::client::archive::{Archive as ArchiveClient, Handlers};
use deepmsg_archive::client::proxy::ReplayParams;
use deepmsg_archive::client::{ArchiveContext, NoCredentials};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::fragment_assembler::FragmentAssembler;

use crate::loadtest::config::{Configuration, IdleStrategy};
use crate::loadtest::recorder::Recorder;
use crate::loadtest::transceiver::{Clock, MessageTransceiver, SystemClock, TransceiverError};
use crate::loadtest::transport::echo::{fragment_fields, payload_fields};
use crate::loadtest::transport::sender::{Kind, MessageSender, PublicationOutbound};
use crate::loadtest::transport::util::{
    ChannelSettings, MIN_MESSAGE_LENGTH, await_connected, images, window_limit,
};

/// The transceiver a run uses when the far end is an archive node
/// (`LiveReplayMessageTransceiver`).
pub struct LiveReplayTransceiver {
    settings: ChannelSettings,
    client: Client,
    archive: ArchiveClient,
    /// Where a batch is sent: the channel the node reads.
    publication: i64,
    /// Where the replay arrives.
    subscription: i64,
    /// The image of that replay, which is what [`Self::receive`] polls.
    image: i64,
    /// The recording found at `init`, which is the one this run reads back.
    last_recording_id: i64,
    /// The publication's window, held between batches so that a batch does not
    /// re-read the counter it was told about.
    window: i64,
    sender: MessageSender,
    assembler: FragmentAssembler,
}

impl LiveReplayTransceiver {
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
            image: 0,
            last_recording_id: 0,
            window: 0,
            sender,
            assembler: FragmentAssembler::new(),
        })
    }

    /// The recording this run is reading back, once [`Self::init`] has found it.
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.last_recording_id
    }

    /// The publication a batch is sent into, once [`Self::init`] has made it.
    #[must_use]
    pub const fn publication(&self) -> i64 {
        self.publication
    }

    /// The image the replay arrives on, once [`Self::init`] has found it.
    #[must_use]
    pub const fn image(&self) -> i64 {
        self.image
    }

    /// Ask the archive for the last recording of the recorded channel, waiting for
    /// there to be one.
    fn await_last_recording_id(&mut self) -> Result<i64, TransceiverError> {
        let deadline = Instant::now() + self.settings.connection_timeout;
        let channel = self.settings.record_channel.clone();
        let stream = self.settings.record_stream;

        loop {
            self.client.poll();

            let mut last = None;
            let found = self
                .archive
                .list_recordings_for_uri(
                    &mut self.client,
                    0,
                    1,
                    &channel,
                    stream,
                    &mut |descriptor| last = Some(descriptor.recording_id),
                )
                .map_err(|error| TransceiverError::Failed {
                    action: "list the recordings of the recorded channel",
                    message: error.to_string(),
                })?;

            if found > 0 {
                if let Some(recording_id) = last {
                    return Ok(recording_id);
                }
            }

            if Instant::now() >= deadline {
                return Err(TransceiverError::Failed {
                    action: "find the recording the node is making",
                    message: format!(
                        "no recording of {channel} stream {stream} appeared within {:?}",
                        self.settings.connection_timeout
                    ),
                });
            }

            std::thread::yield_now();
        }
    }
}

impl<C: Clock> MessageTransceiver<C> for LiveReplayTransceiver {
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

        let publication = self.publication;
        await_connected(
            || {
                self.client.poll();

                self.client
                    .exclusive_publication(publication)
                    .is_some_and(|publication| publication.is_connected().unwrap_or(false))
            },
            self.settings.connection_timeout,
            &SystemClock,
        )
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the node to read the publication",
            message: error.to_string(),
        })?;

        // ★ The recording the node is making, which is what this run measures. The
        // node starts before the client for this reason and no other.
        self.last_recording_id = self.await_last_recording_id()?;

        // ★ Start the replay **and subscribe to it in one step**, because the
        // session id that subscription has to name is the replay's low 32 bits
        // — `(replayId << 32) | replayPublication.sessionId()` — and truncating
        // it is the whole of what makes this a subscription to *this* replay
        // (`ArchiveConductor.java:956`, `AeronArchive.java:890-891`). Doing it
        // in two steps here is how this went wrong once: `i32::try_from` on an
        // id that is by construction out of range yielded `i32::MAX`, the
        // subscription named a session nobody had, and the run died on
        // "Failed to connect within timeout" against **both** archives.
        self.subscription = self
            .archive
            .replay(
                &mut self.client,
                self.last_recording_id,
                &source_channel,
                source_stream,
                &ReplayParams {
                    // `replayFullRecording`: from the beginning, all of it
                    // (`AeronUtil.java:363-377`).
                    position: 0,
                    length: i64::MAX,
                    ..ReplayParams::default()
                },
            )
            .map_err(|error| TransceiverError::Failed {
                action: "start the replay and subscribe to it",
                message: error.to_string(),
            })?;

        let subscription = self.subscription;
        await_connected(
            || {
                self.client.poll();

                images(&self.client, subscription) > 0
            },
            self.settings.connection_timeout,
            &SystemClock,
        )
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the replay to arrive",
            message: error.to_string(),
        })?;

        self.image = self
            .client
            .subscription(subscription)
            .and_then(|subscription| {
                subscription
                    .images()
                    .first()
                    .map(deepmsg_client::image::Image::registration_id)
            })
            .ok_or_else(|| TransceiverError::Failed {
                action: "take the replay's image",
                message: "the subscription reported an image and then had none".to_owned(),
            })?;

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
        let (subscription, image) = (self.subscription, self.image);
        let fragment_limit = self.settings.fragment_limit;

        // The duty cycle first, as on the echo end and for the same reason: it is what
        // refreshes the heartbeat and what takes the driver's answers off the queue.
        self.client.poll();

        let mut fragments = 0;
        self.client
            .poll_image(subscription, image, fragment_limit, |fragment| {
                fragments += 1;

                if fragment.is_unfragmented() {
                    if let Some((timestamp, checksum)) = fragment_fields(fragment) {
                        recorder.on_message_received(timestamp, checksum);
                    }

                    return;
                }

                self.assembler.push(fragment, &mut |message| {
                    if let Some((timestamp, checksum)) = payload_fields(message.payload) {
                        recorder.on_message_received(timestamp, checksum);
                    }
                });
            });

        // `None` because the subscription itself is gone is as closed as an image
        // that has been withdrawn: either way there is nothing left to read.
        let closed = self
            .client
            .subscription(subscription)
            .and_then(|subscription| subscription.image(image))
            .is_none_or(deepmsg_client::image::Image::is_closed);

        match replay_step(fragments, closed) {
            Replay::Read | Replay::Nothing => {}
            // The reference's `IllegalStateException` (`:137-143`), which ends the
            // run: a replay whose image went away is not a replay this end can read,
            // and waiting for the rest of it would be reading a stream nobody is
            // writing.
            Replay::Gone => panic!("the replay's image closed unexpectedly"),
        }
    }
}

/// What one turn of reading the replay was.
///
/// ★ The one decision in this file that can be wrong without a driver, and the shape
/// every other one in this crate takes: the two numbers are handed in and the answer
/// is a value. The reference reads it as "nothing came and the image is closed" →
/// throw (`:137-143`); *nothing came* on its own is not an answer at all, it is the
/// turn simply having had nothing to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replay {
    /// Fragments were read.
    Read,
    /// Nothing was read, and the image is still there.
    Nothing,
    /// Nothing was read, and the image has gone.
    Gone,
}

/// Which of the three a turn is (`LiveReplayMessageTransceiver.receive`, `:137-143`).
fn replay_step(fragments: usize, closed: bool) -> Replay {
    if fragments > 0 {
        return Replay::Read;
    }

    if closed {
        return Replay::Gone;
    }

    Replay::Nothing
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ★ The three branches, and the one that is not the same as the other two: a
    /// turn with nothing to read is an ordinary turn, and a turn with nothing to read
    /// **and no image** ends the run.
    #[test]
    fn a_replay_is_only_gone_when_nothing_came_and_the_image_closed() {
        assert_eq!(Replay::Read, replay_step(1, false));
        assert_eq!(
            Replay::Read,
            replay_step(1, true),
            "an image that closed after delivering is read first, as the reference's \
             own order has it"
        );

        assert_eq!(Replay::Nothing, replay_step(0, false));
        assert_eq!(Replay::Gone, replay_step(0, true));
    }
}
