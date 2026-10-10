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
//!
//! # Where the client's duty cycle runs
//!
//! The reference's client conductor is an agent on a thread of its own
//! (`Aeron.java:174`, `AgentRunner.startOnThread`), and its transceiver's
//! `receive` is one line — `subscription.poll(...)`
//! (`EchoMessageTransceiver.java:176-178`). This rig starts out the other way:
//! `Client::poll` *is* the duty cycle, and [`EchoTransceiver::receive`] runs it
//! on the measured thread on the way to reading the subscription. [`Conductor`]
//! is the switch between running that duty cycle on every turn and gating it by
//! the measured thread's own clock.
//!
//! [`Conductor::Inline`] is the rig's own shape, exactly as it was before the
//! switch existed, and is what an unset switch gives. [`Conductor::InlineGated`]
//! is the shape the A/B asks for: the duty cycle runs at most once an interval,
//! which is what a `receive()` costs when the driver half of the cycle is not
//! run on every turn.

use std::cell::Cell;
use std::path::{Path, PathBuf};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::fragment_assembler::FragmentAssembler;
use deepmsg_client::image::Fragment;
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

/// How often the inline duty cycle runs, when it is not on every `receive()`.
///
/// The rig's duty cycle is [`Client::poll`] on the measured thread (see the
/// module documentation), and every run but the A/B runs it on every turn: that
/// is [`PollGate::Every`], the default, and the shape the rig has always had.
/// [`PollGate::EveryInterval`] is the shape the A/B's "after" arm asks for, read
/// from an environment variable in the binary rather than from a setting — see
/// `loadtest-rig.rs` — so an unset or mistyped switch leaves the rig as it was.
///
/// The duty cycle is **not free to skip**: `Client::poll` carries
/// `check_liveness`, the driver's replies and `expire_pending`
/// (`crates/client/src/client.rs:970-986`). What the gate does *not* touch is
/// the subscription read ([`EchoTransceiver::receive`]'s `poll_messages`), which
/// is where a reply is actually taken off the term — so the gate does not make a
/// run lose a message; it makes it run the driver half of the cycle less often.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollGate {
    /// `Client::poll` on every `receive()`: the rig's own shape, and what an
    /// unset switch gives.
    Every,
    /// At most one `Client::poll` per `interval` nanoseconds of the measured
    /// thread's own monotonic clock, checked on every `receive()`. The interval
    /// is always > 0.
    EveryInterval(u64),
}

/// Where the client's duty cycle runs.
///
/// `Inline` is the rig's own, and the default; `InlineGated` runs the same duty
/// cycle at most once an interval. The gate is on the clock and not on a turn
/// count, because an interval is a property of the run where a turn count is a
/// property of how fast the machine turns — which is what makes the two arms of
/// an A/B comparable across an iteration rate.
// A `Client` is several hundred bytes and `Inline` holds one by value, so the
// variants differ by an order of magnitude. Boxing it to even them out would put
// an indirection on the path the switch promises to leave exactly as it was, and
// that promise is worth more than the enum's size.
#[allow(clippy::large_enum_variant)]
enum Conductor {
    /// This thread runs the duty cycle itself, as it did before the switch
    /// existed.
    Inline(Client),
    /// The inline duty cycle, gated **by time**: `client.poll()` runs only when
    /// the measured thread's monotonic clock has reached `due`, and the turns
    /// between only read the subscription.
    ///
    /// What a run that asks for it (`DEEPMSG_RIG_CLIENT_POLL_MIN_NS` > 0) is
    /// asking is what a `receive()` costs when the driver half of the duty cycle
    /// runs once an interval rather than once a turn.
    ///
    /// [`Conductor::Inline`] is what an unset switch — every run but the A/B —
    /// uses, and its `receive` arm is left byte for byte as it was.
    InlineGated {
        /// The client, polled at most once an interval.
        client: Client,
        /// The interval, in nanoseconds. Always > 0.
        interval: u64,
        /// When the next `poll` is due, on the measured thread's monotonic
        /// clock. A plain `Cell`, on purpose: one thread's own reading, and the
        /// transceiver is never shared.
        due: Cell<i64>,
    },
}

/// A client that publishes to a node and reads what comes back.
pub struct EchoTransceiver {
    settings: ChannelSettings,
    logs_directory: PathBuf,
    conductor: Conductor,
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
    /// Where a **fragmented** message is reassembled.
    ///
    /// The receive path reads fragments rather than messages (see
    /// [`poll_messages`]), because a run's messages fit one frame and a
    /// one-frame message needs no reassembly — the reference hands one straight
    /// to the handler (`FragmentAssembler.java:112-124`), which is the shape this
    /// borrows. A message that did arrive in several frames still has to be put
    /// back together, and this is where that happens; one per transceiver, which
    /// is the arrangement the reference's samples use for one subscription
    /// (`aeron-samples`' `basic_subscriber.c`).
    assembler: FragmentAssembler,
}

impl EchoTransceiver {
    /// Connect to the driver the settings name.
    ///
    /// No media driver is launched here. The reference can start one in-process
    /// when `io.aeron.benchmarks.aeron.embedded.media.driver` is set, and a run
    /// against an external one — which is every run this port is for — leaves it
    /// false and goes through `aeron.dir` instead.
    ///
    /// `gate` selects [`Conductor`], and only matters for the inline shape:
    /// [`PollGate::Every`] — the default — is [`Conductor::Inline`] and the path
    /// this rig has always run, and [`PollGate::EveryInterval`] is
    /// [`Conductor::InlineGated`], the A/B's "after" arm. It is the scene of a
    /// measurement (how much of a `receive()` the duty cycle is) and not a
    /// setting a run should carry.
    ///
    /// # Errors
    ///
    /// [`TransceiverError`] when the driver's CnC file is not where the settings
    /// say, or is not one this build can read.
    pub fn new(
        settings: ChannelSettings,
        idle: IdleStrategy,
        logs_directory: PathBuf,
        gate: PollGate,
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

        let conductor = match gate {
            PollGate::Every => Conductor::Inline(client),
            PollGate::EveryInterval(interval) => Conductor::InlineGated {
                client,
                interval,
                // Poll on the first call, for the same reason the reference
                // polls immediately: `due` in the past is what the first
                // `receive` sees, and every later one is an interval away from
                // the reading it was set from.
                due: Cell::new(0),
            },
        };

        Ok(Self {
            settings,
            logs_directory,
            conductor,
            publication: 0,
            subscription: 0,
            sender,
            window: 0,
            assembler: FragmentAssembler::new(),
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

    /// Run `f` with the client borrowed, whichever [`Conductor`] holds it.
    ///
    /// The one seam between the two shapes: both hand out the client the
    /// transceiver owns, and this is where a shape that shared it would take a
    /// lock instead. Nothing else in this file needs to know which shape it is.
    fn with_client<R>(&mut self, f: impl FnOnce(&mut Client) -> R) -> R {
        match &mut self.conductor {
            Conductor::Inline(client) => f(client),
            Conductor::InlineGated { client, .. } => f(client),
        }
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
            .with_client(|client| {
                client.add_exclusive_publication(
                    &destination_channel,
                    destination_stream,
                    DEFAULT_TIMEOUT,
                )
            })
            .map_err(|error| TransceiverError::Failed {
                action: "add the publication",
                message: error.to_string(),
            })?;

        self.subscription = self
            .with_client(|client| {
                client.add_subscription(&source_channel, source_stream, DEFAULT_TIMEOUT)
            })
            .map_err(|error| TransceiverError::Failed {
                action: "add the subscription",
                message: error.to_string(),
            })?;

        let (publication, subscription, receiver_count) = (
            self.publication,
            self.subscription,
            usize::try_from(self.settings.receiver_count.max(0)).unwrap_or(1),
        );
        let connection_timeout = self.settings.connection_timeout;

        self.with_client(|client| {
            await_connected(
                || {
                    // The driver tells the client about images and about a
                    // publication becoming connected through responses, and
                    // `poll` takes one of those at a time.
                    client.poll();

                    ready(&*client, publication, subscription, receiver_count)
                },
                connection_timeout,
                &SystemClock,
            )
        })
        .map_err(|error| TransceiverError::Failed {
            action: "wait for the node",
            message: error.to_string(),
        })?;

        // Whatever the wait confirmed, so that the first claim does not have to
        // read the counter to find it out again.
        self.window = self.with_client(|client| window_limit(client, publication));

        Ok(())
    }

    fn destroy(&mut self) -> Result<(), TransceiverError> {
        self.with_client(|client| client.close());

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
        // Borrowed apart: the window the outbound writes and the sender that
        // drives it are fields, and so is the conductor the client lives in.
        let window = &mut self.window;
        let sender = &mut self.sender;

        // Sending does not run the duty cycle, so the gate changes nothing here:
        // the client is reached exactly as [`Conductor::Inline`]'s is.
        let client: &Client = match &self.conductor {
            Conductor::Inline(client) => client,
            Conductor::InlineGated { client, .. } => client,
        };

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
        let fragment_limit = self.settings.fragment_limit;
        let subscription = self.subscription;

        match &mut self.conductor {
            // The rig's own shape: the client's duty cycle runs here, on the
            // measured thread, on the way to reading the subscription. It is not
            // optional — it is what refreshes the heartbeat the driver reaps an
            // idle client by, and what takes the driver's answers off the queue,
            // the counter events and the image lifecycle. Polling only the
            // subscription reads messages while telling the driver nothing, and a
            // driver that is watching says so by dropping the client, which takes
            // the far end's image with it.
            //
            // This arm is the whole of a run that did not ask for the gate: no
            // clock is read and no branch is added.
            Conductor::Inline(client) => {
                client.poll();
                poll_messages(
                    client,
                    subscription,
                    fragment_limit,
                    &mut self.assembler,
                    recorder,
                );
            }
            // The same shape, gated by the measured thread's own clock: every
            // turn answers one question — has an interval gone by — so the duty
            // cycle runs at most once an interval and the subscription is still
            // read every turn. The reading is the same [`SystemClock`] the run's
            // round trips come from, so the interval is a duration on the clock
            // the run is measured with.
            //
            // The reading is the **one the rig already took**: the waiting loop
            // reads this clock at the end of every turn (`LoadTestRig::send`,
            // `now_ns = self.recorder.nano_time()`), and the gate runs at the top
            // of the next one, so asking for a second reading here was a
            // `clock_gettime` a turn (14-19 ns, `doc/deepmsg-rust-rig-delta.md`
            // §2.1) to answer a question about an interval of a millisecond.
            // What it costs is that the answer is at most one turn stale, which
            // on that interval is 127-180 ns out of 1_000_000.
            Conductor::InlineGated {
                client,
                interval,
                due,
            } => {
                let now = recorder.last_nano_time();
                if now >= due.get() {
                    client.poll();
                    let interval = i64::try_from(*interval).unwrap_or(i64::MAX);
                    due.set(now.saturating_add(interval));
                }
                poll_messages(
                    client,
                    subscription,
                    fragment_limit,
                    &mut self.assembler,
                    recorder,
                );
            }
        }
    }
}

/// Read up to `fragment_limit` fragments off a subscription and time each whole
/// message **where it lies**.
///
/// The body of the reference's `receive` (`EchoMessageTransceiver.java:176-178`),
/// lifted out of [`EchoTransceiver::receive`] so that the two [`Conductor`]
/// shapes share it — and the **fragment** door rather than the message one, for
/// what a run is measuring. Every message this rig sends fits one frame, so
/// every message comes back unfragmented, and the reference's own reader hands a
/// one-frame message straight through to the handler
/// (`FragmentAssembler.onFragment`, `FragmentAssembler.java:112-124`) with the
/// frame's own buffer under it. The message door delivers the same thing, but it
/// reassembles first: it copies every payload into an assembler buffer before the
/// handler is called at all — the deviation `docs/compat.md` records as "A
/// delivered message is copied" — and that copy is on the measured path of every
/// single message in the run. Reading the fragments costs the same walk and no
/// copy.
///
/// What it does **not** cost is the ability to handle a fragmented message: one
/// that arrived in several frames goes through `assembler` exactly as it did
/// before, and the handler sees the same whole message. This rig's own traffic
/// never takes that branch, but a run that asked for a message length past the
/// MTU does, and it must still be timed.
fn poll_messages<C: Clock>(
    client: &mut Client,
    subscription: i64,
    fragment_limit: usize,
    assembler: &mut FragmentAssembler,
    recorder: &mut Recorder<C>,
) {
    client.poll_subscription_fragments(subscription, fragment_limit, |fragment| {
        if fragment.is_unfragmented() {
            if let Some((timestamp, checksum)) = fragment_fields(fragment) {
                recorder.on_message_received(timestamp, checksum);
            }

            return;
        }

        assembler.push(fragment, &mut |message| {
            if let Some((timestamp, checksum)) = payload_fields(message.payload) {
                recorder.on_message_received(timestamp, checksum);
            }
        });
    });
}

/// The two fields a run's message carries — when it was meant to go out, and the
/// run's checksum — read out of the frame **in place**.
///
/// `None` for a frame too short to be one of this run's messages, which is the
/// check the message door made against the assembled payload.
fn fragment_fields(fragment: &Fragment<'_>) -> Option<(i64, i64)> {
    let length = fragment.payload_length();

    if length < MIN_MESSAGE_LENGTH {
        return None;
    }

    Some((
        fragment.payload_i64_at(TIMESTAMP_OFFSET)?,
        fragment.payload_i64_at(length.checked_sub(8)?)?,
    ))
}

/// The same two fields, out of an assembled message's payload.
fn payload_fields(payload: &[u8]) -> Option<(i64, i64)> {
    if payload.len() < MIN_MESSAGE_LENGTH {
        return None;
    }

    Some((
        i64_at(payload, TIMESTAMP_OFFSET)?,
        i64_at(payload, payload.len().checked_sub(8)?)?,
    ))
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
    /// The publication this batch writes into, resolved **once for the batch**.
    ///
    /// The reference's sender holds the `ExclusivePublication` object itself
    /// (`MessageSender.java:52`), so nothing on its per-message path looks one
    /// up. This could not hold one either until the client let it: the
    /// registration id was the only handle, and every message paid the linear
    /// scan of `Client::exclusive_publication` to turn it back into a
    /// publication. `None` when the client no longer holds it, which is what
    /// each message answered for itself before — and it cannot appear or vanish
    /// inside a batch, because the duty cycle that would move it is not run
    /// while this is sending.
    publication: Option<&'a ExclusivePublication>,
    /// What the **offer** path needs and the claim path no longer does: it hands
    /// the id back to `Client::offer_exclusive`, which resolves the publication
    /// and reads its limit itself. A run that claims does not come through here
    /// at all (`use.try.claim`), and a run that offers is measuring the offer.
    registration_id: i64,
    /// The window, held by the transceiver so that it outlives one batch.
    window: &'a mut i64,
}

/// Read a publication's window limit from the driver's counter.
fn read_window(client: &Client, publication: &ExclusivePublication) -> i64 {
    client
        .counters_reader()
        .and_then(|counters| counters.value(publication.position_limit_counter_id()))
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
        let Some(publication) = self.publication else {
            // The publication is gone from under the client.
            return Err(Appended::Malformed);
        };

        let mut outcome = publication.try_claim(*self.window, length);

        if matches!(
            outcome,
            Err(Appended::BackPressured | Appended::NotConnected)
        ) {
            *self.window = read_window(self.client, publication);
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
