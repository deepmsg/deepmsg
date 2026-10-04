//! The far end of an echo measurement: read what arrives, send it straight back.
//!
//! Mirrors `benchmarks-aeron/.../EchoNode.java`. A run where the far end is this
//! rather than the reference's node is a run with no Java in it at all, which is
//! what the all-ours grid is for.
//!
//! Two things the node does that are easy to leave out and change the numbers:
//!
//! - **It answers only the messages addressed to it.** A message carries the
//!   index of the receiver it is for, and a node whose index is not that one
//!   says nothing. That is what makes a fan-out's cost a count of receivers
//!   rather than a count of messages.
//! - **It answers with an offer**, never a claim, whatever the client used. The
//!   message is already in a term by the time the node sees it; the node is
//!   publishing it into a different one, and it has no claim to make into the
//!   one it is reading.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;

use crate::loadtest::config::IdleStrategy;
use crate::loadtest::transceiver::{Idle, SystemClock, TransceiverError};
use crate::loadtest::transport::echo::i32_at;
use crate::loadtest::transport::util::{
    ChannelSettings, MIN_MESSAGE_LENGTH, RECEIVER_INDEX_OFFSET, await_connected,
    check_publication_result,
};

/// A node that echoes, until it is told to stop or the client goes away.
pub struct EchoNode {
    settings: ChannelSettings,
    idle: IdleStrategy,
    receiver_index: i32,
    running: Arc<AtomicBool>,
    client: Client,
    publication: i64,
    subscription: i64,
    /// The messages one poll took, kept between polls so that echoing them does
    /// not allocate. See [`EchoNode::run`] for why they are taken out of the
    /// poll rather than answered inside it.
    echoed: Vec<Vec<u8>>,
}

impl EchoNode {
    /// Connect, and create the two resources the node answers through.
    ///
    /// The publication is on the channel a client *reads*, and the subscription
    /// on the one it *writes* — the mirror of the client's own pair.
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the driver is not where the settings say, or
    /// refuses either resource.
    pub fn new(
        settings: ChannelSettings,
        idle: IdleStrategy,
        receiver_index: i32,
    ) -> Result<Self, TransceiverError> {
        let client =
            Client::connect(&settings.directory).map_err(|error| TransceiverError::Failed {
                action: "connect to the driver",
                message: error.to_string(),
            })?;

        let mut node = Self {
            publication: 0,
            subscription: 0,
            settings,
            idle,
            receiver_index,
            running: Arc::new(AtomicBool::new(true)),
            client,
            echoed: Vec::new(),
        };

        node.attach()?;

        Ok(node)
    }

    /// A handle a test or a signal handler can clear to bring the node down.
    ///
    /// The reference's node is stopped by a shutdown hook setting an
    /// `AtomicBoolean`, and it returns from `run` once it has seen that and
    /// found nothing to echo.
    #[must_use]
    pub fn running(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.running)
    }

    /// Create the node's publication and subscription.
    fn attach(&mut self) -> Result<(), TransceiverError> {
        let (source_channel, source_stream) = (
            self.settings.source_channel.clone(),
            self.settings.source_stream,
        );
        let (destination_channel, destination_stream) = (
            self.settings.destination_channel.clone(),
            self.settings.destination_stream,
        );

        self.publication = self
            .client
            .add_exclusive_publication(&source_channel, source_stream, DEFAULT_TIMEOUT)
            .map_err(|error| TransceiverError::Failed {
                action: "add the node's publication",
                message: error.to_string(),
            })?;

        self.subscription = self
            .client
            .add_subscription(&destination_channel, destination_stream, DEFAULT_TIMEOUT)
            .map_err(|error| TransceiverError::Failed {
                action: "add the node's subscription",
                message: error.to_string(),
            })?;

        Ok(())
    }

    /// Echo until the client goes away or [`EchoNode::running`] is cleared.
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the two ends never find each other.
    ///
    /// # Panics
    ///
    /// When the publication is not merely slow — as the reference's throw does.
    pub fn run(&mut self) -> Result<(), TransceiverError> {
        let (publication, subscription) = (self.publication, self.subscription);
        let receiver_index = self.receiver_index;
        let fragment_limit = self.settings.fragment_limit;
        let client = &mut self.client;

        await_connected(
            || {
                client.poll();

                images(client, subscription) > 0 && window_limit(client, publication) > 0
            },
            self.settings.connection_timeout,
            &SystemClock,
        )
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the client",
            message: error.to_string(),
        })?;

        // Taken out of `self` for the loop: the poll borrows the client
        // mutably, and the echoes after it borrow the client again, so the
        // messages have to be somewhere that is neither.
        let mut echoed = std::mem::take(&mut self.echoed);

        loop {
            self.client.poll();

            let mut arrived = 0;
            {
                let client = &mut self.client;

                client.poll_subscription(subscription, fragment_limit, |message| {
                    let payload = message.payload;

                    if payload.len() < MIN_MESSAGE_LENGTH {
                        return;
                    }

                    // Only the messages addressed to this node. One message of a
                    // client's batch names one receiver, so a node that answered
                    // everything would answer for its neighbours too.
                    if i32_at(payload, RECEIVER_INDEX_OFFSET) != Some(receiver_index) {
                        return;
                    }

                    if arrived == echoed.len() {
                        echoed.push(Vec::new());
                    }
                    echoed[arrived].clear();
                    echoed[arrived].extend_from_slice(payload);
                    arrived += 1;
                });
            }

            for payload in &echoed[..arrived] {
                echo(&self.client, publication, payload, &mut self.idle);
            }

            if arrived == 0
                && (!self.running.load(Ordering::Relaxed)
                    || images(&self.client, subscription) == 0)
            {
                break;
            }

            // `IdleStrategy.idle(workCount)`: something happened, so start
            // again; nothing did, so wait.
            if arrived == 0 {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }

        self.echoed = echoed;

        Ok(())
    }

    /// Release both resources.
    pub fn close(&mut self) {
        self.client.close();
    }
}

/// Send one message back, waiting as long as it takes.
///
/// The reference's node loops on a negative offer without a bound — it is the
/// only thing this process has to do, and giving up would drop a message the
/// client is timing.
fn echo(client: &Client, publication: i64, payload: &[u8], idle: &mut IdleStrategy) {
    idle.reset();

    loop {
        match client.offer_exclusive(publication, payload) {
            Some(Appended::Ok { .. }) => return,
            Some(other) => match check_publication_result(other, idle) {
                Ok(_) => {}
                Err(error) => panic!("{error}"),
            },
            None => panic!("the node's publication is gone"),
        }
    }
}

/// How many images the subscription has, which is how many publishers it hears.
fn images(client: &Client, subscription: i64) -> usize {
    client
        .subscription(subscription)
        .map_or(0, |subscription| subscription.images().len())
}

/// The publication's position limit — see `echo::window_limit`, which this
/// repeats because a node and a client are separate connections to the same
/// driver.
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

/// The fields the node reads, re-exported so that a test can check it read the
/// right ones.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadtest::transport::echo::i64_at;
    use crate::loadtest::transport::util::TIMESTAMP_OFFSET;

    #[test]
    fn a_message_names_its_receiver_and_carries_its_timestamp() {
        let mut payload = vec![0_u8; 32];
        payload[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8].copy_from_slice(&1234_i64.to_le_bytes());
        payload[RECEIVER_INDEX_OFFSET..RECEIVER_INDEX_OFFSET + 4]
            .copy_from_slice(&2_i32.to_le_bytes());

        assert_eq!(i64_at(&payload, TIMESTAMP_OFFSET), Some(1234));
        assert_eq!(i32_at(&payload, RECEIVER_INDEX_OFFSET), Some(2));
    }

    /// The offset of a message's checksum comes from its own length, and a
    /// message with nothing at that offset is one this reads nothing out of
    /// rather than one it panics on.
    #[test]
    fn reading_past_the_end_of_a_message_is_none_rather_than_a_panic() {
        let payload = vec![0_u8; MIN_MESSAGE_LENGTH];

        assert_eq!(i64_at(&payload, MIN_MESSAGE_LENGTH), None);
        assert_eq!(i32_at(&payload, MIN_MESSAGE_LENGTH - 1), None);
        assert_eq!(i64_at(&payload, usize::MAX), None);
        assert_eq!(i32_at(&payload, usize::MAX), None);
    }
}
