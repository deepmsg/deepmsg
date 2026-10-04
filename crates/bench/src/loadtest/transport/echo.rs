//! The echo a run measures against: publish to a node, read the replies back.
//!
//! Mirrors `benchmarks-aeron/.../EchoMessageTransceiver.java`. A run's client
//! publishes on one channel and subscribes on another, and the far end — the
//! reference's `EchoNode`, or the one in [`super::node`] — sends every message
//! back the way it came. So a message's round trip is the whole stack: the
//! client, the driver at both ends, and the node.
//!
//! UDP and IPC are the same code with a different channel string; what differs
//! is what the message crosses, which is the point of having both.

use std::path::{Path, PathBuf};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::publication::ExclusivePublication;
use deepmsg_core::buffer::ReadWrite;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::frame::{DATA_HEADER_LENGTH, Frame};

use crate::loadtest::config::{Configuration, IdleStrategy};
use crate::loadtest::recorder::Recorder;
use crate::loadtest::transceiver::{Clock, MessageTransceiver, SystemClock, TransceiverError};
use crate::loadtest::transport::sender::{Kind, MessageSender, Outbound, Published};
use crate::loadtest::transport::util::{
    ChannelSettings, MIN_MESSAGE_LENGTH, TIMESTAMP_OFFSET, await_connected,
};

/// A client that publishes to a node and reads what comes back.
pub struct EchoTransceiver {
    settings: ChannelSettings,
    logs_directory: PathBuf,
    client: Client,
    publication: i64,
    subscription: i64,
    sender: MessageSender,
    /// The publication's window, cached between messages.
    ///
    /// The plan's G1, and not an optimisation. A client can `offer_exclusive`
    /// and have the limit read for it, but there is no client-level claim: the
    /// only claim is on the publication itself, and it takes the limit as an
    /// argument. So a claiming sender reads the counter — and reads it again
    /// only when the log says there is no room, which is what the reference's
    /// client does inside its own append.
    window: i64,
}

impl EchoTransceiver {
    /// Connect to the driver the settings name.
    ///
    /// No media driver is launched here. The reference can start one in-process
    /// when `io.aeron.benchmarks.aeron.embedded.media.driver` is set, and a run
    /// against an external one — which is every run this port is for — leaves it
    /// false and goes through `aeron.dir` instead.
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the driver's CnC file is not where the settings
    /// say, or is not one this build can read.
    pub fn new(
        settings: ChannelSettings,
        idle: IdleStrategy,
        logs_directory: PathBuf,
    ) -> Result<Self, TransceiverError> {
        let client =
            Client::connect(&settings.directory).map_err(|error| TransceiverError::Failed {
                action: "connect to the driver",
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
            logs_directory,
            client,
            publication: 0,
            subscription: 0,
            sender,
            window: 0,
        })
    }

    /// Where the reference writes this end's diagnostics.
    ///
    /// Nothing is written there yet: `AeronUtil.dumpAeronStats` copies the CnC
    /// file's version, its process id and every counter into
    /// `logs/echo-client-aeron-stat.txt`, and a run that wants that corroboration
    /// has the reference's own file to read until this writes one.
    #[must_use]
    pub fn logs_directory(&self) -> &Path {
        &self.logs_directory
    }
}

impl<C: Clock> MessageTransceiver<C> for EchoTransceiver {
    fn init(&mut self, configuration: &Configuration) -> Result<(), TransceiverError> {
        let message_length = usize::try_from(configuration.message_length()).unwrap_or(usize::MAX);
        if message_length < MIN_MESSAGE_LENGTH {
            return Err(TransceiverError::Failed {
                action: "init",
                message: format!("Message length must be at least {MIN_MESSAGE_LENGTH}"),
            });
        }

        let (destination_channel, destination_stream) = (
            self.settings.destination_channel.clone(),
            self.settings.destination_stream,
        );
        let (source_channel, source_stream) = (
            self.settings.source_channel.clone(),
            self.settings.source_stream,
        );

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

        let (publication, subscription, receiver_count) = (
            self.publication,
            self.subscription,
            usize::try_from(self.settings.receiver_count.max(0)).unwrap_or(1),
        );
        let client = &mut self.client;

        await_connected(
            || {
                // The driver tells the client about images and about a
                // publication becoming connected through responses, and `poll`
                // takes one of those at a time.
                client.poll();

                ready(client, publication, subscription, receiver_count)
            },
            self.settings.connection_timeout,
            &SystemClock,
        )
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the node",
            message: error.to_string(),
        })?;

        // Whatever the wait confirmed, so that the first claim does not have to
        // read the counter to find it out again.
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
        let mut outbound = PublicationOutbound {
            client: &self.client,
            registration_id,
            window: &mut self.window,
        };

        self.sender.send(
            &mut outbound,
            number_of_messages,
            message_length,
            timestamp,
            checksum,
        )
    }

    fn receive(&mut self, recorder: &mut Recorder<C>) {
        let fragment_limit = self.settings.fragment_limit;
        let subscription = self.subscription;

        // The client's own duty cycle, and not optional: it is what refreshes the
        // heartbeat the driver reaps an idle client by, and what takes the
        // driver's answers off the queue — the counter events, the image
        // lifecycle. Polling only the subscription reads messages while telling
        // the driver nothing, and a driver that is watching says so by dropping
        // the client, which takes the far end's image with it.
        self.client.poll();

        self.client
            .poll_subscription(subscription, fragment_limit, |message| {
                let payload = message.payload;

                if payload.len() < MIN_MESSAGE_LENGTH {
                    return;
                }

                if let (Some(timestamp), Some(checksum)) = (
                    i64_at(payload, TIMESTAMP_OFFSET),
                    i64_at(payload, payload.len() - 8),
                ) {
                    recorder.on_message_received(timestamp, checksum);
                }
            });
    }
}

/// The eight bytes at `offset`, little-endian.
///
/// `None` when they are not all there. The offset comes from a message's own
/// length, so a message shorter than the fields it claims to carry is a message
/// this reads nothing out of rather than a panic.
pub(crate) fn i64_at(payload: &[u8], offset: usize) -> Option<i64> {
    payload
        .get(offset..offset.checked_add(8)?)?
        .try_into()
        .ok()
        .map(i64::from_le_bytes)
}

/// The four bytes at `offset`, little-endian. See [`i64_at`].
pub(crate) fn i32_at(payload: &[u8], offset: usize) -> Option<i32> {
    payload
        .get(offset..offset.checked_add(4)?)?
        .try_into()
        .ok()
        .map(i32::from_le_bytes)
}

/// Whether the two ends have found each other.
///
/// `EchoMessageTransceiver.init`'s condition. The window is asked about as its
/// *limit*, where the reference asks for `availableWindow` — limit less position.
/// At this point the publication has never sent anything, so its position is
/// zero and the two agree; a run that asked later could not use this.
fn ready(client: &Client, publication: i64, subscription: i64, receiver_count: usize) -> bool {
    let connected = client
        .exclusive_publication(publication)
        .and_then(|publication| publication.is_connected())
        == Some(true);

    let images = client
        .subscription(subscription)
        .map_or(0, |subscription| subscription.images().len());

    connected && images == receiver_count && window_limit(client, publication) > 0
}

/// The publication's position limit, which is what its window opens to.
fn window_limit(client: &Client, publication: i64) -> i64 {
    let Some(counter_id) = client
        .exclusive_publication(publication)
        .map(|publication| publication.position_limit_counter_id())
    else {
        return 0;
    };

    client
        .counters_reader()
        .and_then(|counters| counters.value(counter_id))
        .unwrap_or(0)
}

/// The publication a sender writes into.
struct PublicationOutbound<'a> {
    client: &'a Client,
    registration_id: i64,
    /// The window, held by the transceiver so that it outlives one batch.
    window: &'a mut i64,
}

/// The exclusive publication a registration id names.
///
/// A free function rather than a method: a method's returned borrow would be
/// tied to the outbound, and the window it is about to write is a field of it.
fn publication_of(client: &Client, registration_id: i64) -> Option<&ExclusivePublication> {
    client.exclusive_publication(registration_id)
}

/// Read a publication's window limit from the driver's counter.
fn read_window(client: &Client, registration_id: i64) -> i64 {
    let Some(counter_id) = publication_of(client, registration_id)
        .map(|publication| publication.position_limit_counter_id())
    else {
        return 0;
    };

    client
        .counters_reader()
        .and_then(|counters| counters.value(counter_id))
        .unwrap_or(0)
}

impl Outbound for PublicationOutbound<'_> {
    fn claim<W: FnMut(&Frame<'_, ReadWrite>)>(&mut self, length: usize, mut write: W) -> Published {
        // A claim takes the limit as an argument and there is no client-level
        // one that would read it, so the limit is kept here and re-read only
        // when the log says the window it describes is used up — which is what
        // the reference's client does inside its own append. Reporting that
        // first answer as back pressure would spend one of the sender's three
        // attempts on a window that had merely moved.
        let Some(publication) = publication_of(self.client, self.registration_id) else {
            // The publication is gone from under the client.
            return Err(Appended::Malformed);
        };

        let mut outcome = publication.try_claim(*self.window, length);

        if matches!(
            outcome,
            Err(Appended::BackPressured | Appended::NotConnected)
        ) {
            *self.window = read_window(self.client, self.registration_id);
            outcome = publication.try_claim(*self.window, length);
        }

        // Bound rather than inlined: a frame borrows the claim it came from.
        let claim = outcome?;
        let frame = claim.frame();
        write(&frame);

        let frame_length = i32::try_from(length + DATA_HEADER_LENGTH).unwrap_or(i32::MAX);

        match frame.publish(frame_length) {
            Some(()) => Ok(()),
            // The frame did not fit where it was claimed, which the appender
            // only finds out on publishing it.
            None => Err(Appended::Malformed),
        }
    }

    fn offer(&mut self, payload: &[u8]) -> Published {
        match self.client.offer_exclusive(self.registration_id, payload) {
            Some(Appended::Ok { .. }) => Ok(()),
            Some(other) => Err(other),
            None => Err(Appended::Malformed),
        }
    }
}
