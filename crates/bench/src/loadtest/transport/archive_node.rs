//! The far end of an archive measurement: **forward** what arrives into a recording.
//!
//! Mirrors `benchmarks-aeron/.../ArchiveNode.java`. Its neighbour
//! [`EchoNode`](crate::loadtest::transport::node::EchoNode) answers the client; this
//! one does **not**. It reads the client's stream and offers every message straight
//! into a *different* channel — the one an archive is recording — and the client reads
//! the recording back as a **replay**. That is the whole shape of the measurement: the
//! reply the client's rig measures a round trip on is a byte the archive wrote to disk
//! and read back.
//!
//! # Three ways it is not an echo node
//!
//! 1. **The two channels' roles are crossed.** The echo node subscribes to what a
//!    client *writes* and publishes on what it *reads*. This one subscribes to what
//!    the client writes — `destination_channel` — and publishes on
//!    **`record_channel`**, which is the channel an archive is recording. What the
//!    client reads is that recording, replayed by the archive.
//! 2. **It starts a recording.** `addSessionId(recordChannel, publication.sessionId())`
//!    is the channel it asks for, which is the reference's own line
//!    (`ArchiveNode.java:86`): a **local source** is recorded by naming exactly the
//!    publication to be recorded rather than by subscribing to a channel and taking
//!    what turns up. That channel is also what
//!    [`AeronArchive.startRecording`](deepmsg_archive::client::archive::Archive::start_recording)
//!    is told, so an archive that is already recording somebody else's stream on the
//!    same channel is not what this records.
//! 3. ★ **It waits for the recording to be running before it forwards anything**
//!    (`:97`, `awaitRecordingStart`). The wait is on a **recording position counter**,
//!    which the archive allocates into the *driver's* counters region when the image
//!    arrives — so this is the one place in the benchmark where the far end reads a
//!    counter the archive wrote, and the reason the reference's own poll has no
//!    timeout at all. This one has one (see [`ArchiveNode::await_recording_id`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use deepmsg_archive::client::archive::{
    Archive as ArchiveClient, Handlers, channel_with_session_id,
};
use deepmsg_archive::client::{ArchiveContext, NoCredentials};
use deepmsg_archive::server::recording_pos::{find_counter_id_by_session, parse_key};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::fragment_assembler::FragmentAssembler;

use crate::loadtest::config::IdleStrategy;
use crate::loadtest::transceiver::{Idle, SystemClock, TransceiverError};
use crate::loadtest::transport::util::{
    ChannelSettings, MIN_MESSAGE_LENGTH, await_connected, images, offer_until_accepted,
    window_limit,
};

/// The archive id to look the recording up under, when the deployment names one.
///
/// `Aeron.NULL_VALUE`, which is what the reference's
/// `RecordingPos.findCounterIdBySession` treats as "do not compare the archive id"
/// (`RecordingPos.java:230`) — and which is also the reference archive's own default
/// id, so a run that never names one is asking the same question either way.
pub const ANY_ARCHIVE_ID: i64 = -1;

/// A node that forwards what it reads into a recorded channel, until it is told to
/// stop or the client goes away.
pub struct ArchiveNode {
    settings: ChannelSettings,
    idle: IdleStrategy,
    running: Arc<AtomicBool>,
    client: Client,
    archive: ArchiveClient,
    /// What it forwards into: the channel an archive is recording.
    publication: i64,
    /// What it reads: the channel the client writes.
    subscription: i64,
    /// The recording the archive made of `publication`, once it is running.
    recording_id: i64,
    /// The **publication's** session id, which is what the recording position
    /// counter is keyed by and what the recording channel names.
    session_id: i32,
    /// The archive id its counters are keyed by, which is what the recording
    /// position counter is looked up with.
    archive_id: i64,
    assembler: FragmentAssembler,
}

impl ArchiveNode {
    /// Connect, and create the two resources the node forwards through.
    ///
    /// The publication is on the **recorded** channel and the subscription on the one
    /// the client writes — see the module note for why that is the opposite way round
    /// from [`EchoNode`](crate::loadtest::transport::node::EchoNode).
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the driver or the archive is not where the settings
    /// say, when either refuses a resource, or when the archive refuses the recording.
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

        let archive_id = archive.archive_id();

        let mut node = Self {
            settings,
            idle,
            running: Arc::new(AtomicBool::new(true)),
            client,
            archive,
            publication: 0,
            subscription: 0,
            recording_id: ANY_ARCHIVE_ID,
            session_id: 0,
            archive_id,
            assembler: FragmentAssembler::new(),
        };

        node.attach()?;

        Ok(node)
    }

    /// A handle a test or a signal handler can clear to bring the node down.
    #[must_use]
    pub fn running(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.running)
    }

    /// The recording the archive is making of this node's publication.
    ///
    /// `ANY_ARCHIVE_ID` until [`ArchiveNode::run`] has seen the counter appear.
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Create the publication, the subscription, and the recording that joins them.
    fn attach(&mut self) -> Result<(), TransceiverError> {
        let record_channel = self.settings.record_channel.clone();
        let record_stream = self.settings.record_stream;
        let destination_channel = self.settings.destination_channel.clone();
        let destination_stream = self.settings.destination_stream;

        self.publication = self
            .client
            .add_exclusive_publication(&record_channel, record_stream, DEFAULT_TIMEOUT)
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

        // The session id of the publication just made, so that the archive records
        // **this** one and not whatever else is on the channel (`ArchiveNode.java:84-87`).
        let session_id = self
            .client
            .exclusive_publication(self.publication)
            .map(|publication| publication.session_id())
            .ok_or_else(|| TransceiverError::Failed {
                action: "read the node's publication session id",
                message: "the driver answered with a publication this client does not hold"
                    .to_owned(),
            })?;

        self.session_id = session_id;

        let channel = channel_with_session_id(&record_channel, session_id).map_err(|error| {
            TransceiverError::Failed {
                action: "name the publication in the recording channel",
                message: error.to_string(),
            }
        })?;

        self.archive
            .start_recording(
                &mut self.client,
                &channel,
                record_stream,
                // `LOCAL`: the archive subscribes to the channel rather than being
                // sent the stream by another archive (`ArchiveNode.java:87`).
                true,
                // `autoStop`, which is the reference's `true`: the recording ends
                // with the client that asked for it.
                true,
            )
            .map_err(|error| TransceiverError::Failed {
                action: "start the recording",
                message: error.to_string(),
            })?;

        Ok(())
    }

    /// Forward until the client goes away or [`ArchiveNode::running`] is cleared.
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the two ends never find each other, or when the
    /// recording never starts.
    ///
    /// # Panics
    ///
    /// When the publication is not merely slow — as the reference's throw does
    /// (`AeronUtil.checkPublicationResult`).
    pub fn run(&mut self) -> Result<(), TransceiverError> {
        let (publication, subscription) = (self.publication, self.subscription);
        let fragment_limit = self.settings.fragment_limit;

        {
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
        }

        self.recording_id = self.await_recording_id()?;

        // Taken out of `self` for the loop, for the reason the echo node takes its
        // own out: the poll borrows the client mutably and the offer inside it
        // borrows the client again.
        let mut assembler = std::mem::take(&mut self.assembler);
        let mut idle = self.idle;

        loop {
            self.client.poll();

            let mut forwarded = 0;
            {
                let client = &mut self.client;

                client.poll_subscription_fragments_with_publication(
                    subscription,
                    fragment_limit,
                    publication,
                    |fragment, writer| {
                        if fragment.is_unfragmented() {
                            // The two checks the message door makes, in its order:
                            // a run shorter than a message header is not one this
                            // node forwards, and a message the *client* would not
                            // have accepted is not one to record either.
                            if fragment.payload_length() < MIN_MESSAGE_LENGTH {
                                return;
                            }

                            // Straight into the recorded channel, out of the window
                            // it arrived in: one copy, from the received term into
                            // the log an archive is reading.
                            offer_until_accepted(&mut idle, || writer.offer(fragment));
                            forwarded += 1;

                            return;
                        }

                        // A message that arrived in several frames goes back out as
                        // one, through the assembler — which is what the reference's
                        // `ImageFragmentAssembler` does in `AeronUtil.pipeMessages`
                        // too (`AeronUtil.java:327-336`).
                        assembler.push(fragment, &mut |message| {
                            if message.payload.len() < MIN_MESSAGE_LENGTH {
                                return;
                            }

                            offer_until_accepted(&mut idle, || writer.offer_bytes(message.payload));
                            forwarded += 1;
                        });
                    },
                );
            }

            // The client is gone, or somebody asked this node to stop: the
            // reference's node returns for the first of those reasons too, and the
            // second is what a signal handler sets (`EchoNode`'s arrangement, which
            // a node started for a run shares).
            if 0 == forwarded
                && (!self.running.load(Ordering::Acquire)
                    || images(&self.client, subscription) == 0)
            {
                return Ok(());
            }

            // `IdleStrategy.idle(workCount)`: something went out, so start again;
            // nothing did, so wait.
            if 0 == forwarded {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }
    }

    /// Wait for the archive's recording of this node's publication to be running, and
    /// answer with its recording id.
    ///
    /// ★ **The wait has a deadline here and none in the reference**
    /// (`AeronUtil.awaitRecordingStart:277-288` is a `do { … } while`). The reference
    /// runs the archive in the same process and would rather hang than miss; this one
    /// is given the connection timeout so that a recording that never starts is a
    /// message rather than a process that stops responding.
    fn await_recording_id(&mut self) -> Result<i64, TransceiverError> {
        let deadline = Instant::now() + self.settings.connection_timeout;
        let archive_id = self.archive_id;
        let session_id = self.session_id;
        let client = &mut self.client;

        await_recording_id(deadline, || {
            client.poll();

            let counters = client.counters_reader()?;
            let counter_id = find_counter_id_by_session(&counters, session_id, archive_id)?;

            counters.key(counter_id).and_then(|key| parse_key(&key))
        })
        .map(|identity| identity.recording_id)
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the recording to start",
            message: error.to_string(),
        })
    }

    /// Give the two resources back.
    pub fn close(&mut self) {
        if 0 != self.subscription {
            let _ = self
                .client
                .remove_subscription(self.subscription, DEFAULT_TIMEOUT);
            self.subscription = 0;
        }

        if 0 != self.publication {
            let _ = self
                .client
                .remove_publication(self.publication, DEFAULT_TIMEOUT);
            self.publication = 0;
        }
    }
}

/// Wait for a recording's identity, polling until the deadline.
///
/// A function of its own rather than a loop inside the node because it is the half
/// that can be wrong without a driver: what it is given is a closure that answers
/// `None` until the counter is there, and what it must do is neither spin forever nor
/// give up on the first `None`.
///
/// # Errors
///
/// A [`TransceiverError::Failed`] when the deadline passes.
fn await_recording_id(
    deadline: Instant,
    mut poll: impl FnMut() -> Option<deepmsg_archive::server::recording_pos::RecordedIdentity>,
) -> Result<deepmsg_archive::server::recording_pos::RecordedIdentity, TransceiverError> {
    loop {
        if let Some(identity) = poll() {
            return Ok(identity);
        }

        if Instant::now() >= deadline {
            return Err(TransceiverError::Failed {
                action: "wait for the recording to start",
                message: "no recording position counter appeared for this publication".to_owned(),
            });
        }

        std::thread::yield_now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    /// The wait answers as soon as the counter is there, and does not care how many
    /// turns it took.
    #[test]
    fn the_wait_answers_with_the_recording_the_counter_names() {
        let mut polls = 0;
        let identity = await_recording_id(Instant::now() + Duration::from_secs(1), || {
            polls += 1;

            (polls > 3).then_some(deepmsg_archive::server::recording_pos::RecordedIdentity {
                recording_id: 42,
                session_id: 7,
                source_identity: "aeron:ipc".to_owned(),
                archive_id: ANY_ARCHIVE_ID,
            })
        })
        .expect("the counter appears inside the deadline");

        assert_eq!(42, identity.recording_id);
        assert_eq!(4, polls, "three turns of nothing, then the answer");
    }

    /// ★ A counter that never appears is a **message**, not a process that stops
    /// answering: the reference's own wait has no timeout and this one does, which is
    /// the only deliberate difference between them.
    #[test]
    fn the_wait_gives_up_rather_than_spinning_forever() {
        let error = await_recording_id(Instant::now(), || None)
            .expect_err("the deadline has already passed");

        assert!(
            error.to_string().contains("recording position counter"),
            "the message says what was being waited for: {error}"
        );
    }
}
