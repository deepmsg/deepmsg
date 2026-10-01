//! The receiver agent: the thread that reads the wire and builds images.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_driver_receiver.c`. Its mirror is
//! [`crate::sender`], and the split is the same one: the conductor decides —
//! which channels, which subscriptions, which images — and this thread owns the
//! sockets and the images afterwards, because it is the thread that reads the
//! terms.
//!
//! # What one pass does
//!
//! 1. take the conductor's commands;
//! 2. poll every endpoint's socket and give each datagram to the thing that
//!    wants it ([`crate::media::dispatcher`] answers "does anything?");
//! 3. for every image: send the status message it owes, and — P1-4's last
//!    piece — the NAK its loss detector has found;
//! 4. re-send the `SETUP`-eliciting status messages of the sessions that have
//!    not answered yet, on a timeout
//!    (`aeron_driver_receiver.c:211-252`).
//!
//! # Why the receiver *sends*
//!
//! Because a subscriber that says nothing is a subscriber that stops the
//! stream: the sender's whole flow control is the status messages this thread
//! sends, and the retransmissions that fill a hole are asked for with the NAKs
//! it sends. The reference says the same thing in the shape of its poller — the
//! receiver's poll returns "work" for sends as well as reads.

use std::io;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver as Inbox, Sender as Outbox};
use std::thread::JoinHandle;

use deepmsg_cnc::{CncFile, CounterManager, CounterRegions};

use crate::idle::Backoff;

use crate::media::dispatcher::Interest;
use crate::media::receive_endpoint::ReceiveChannelEndpoint;
use crate::media::receive_endpoint::ReceiveDestination;
use crate::protocol::{FrameHeader, SetupFrame, frame_type, is_frame_valid};
use crate::publication_image::PublicationImage;
use crate::subscribable::TetherablePosition;
use crate::sys::socket::Datagrams;
use crate::system_counters::{self, System};
use crate::udp_channel::UdpChannel;

/// How many datagrams one poll may read
/// (`AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX`,
/// `aeron-driver/src/main/c/aeron_driver_context.h:59`).
const RECEIVE_SLOTS: usize = 16;

/// How long a pending setup waits before the status message is sent again
/// (`AERON_DRIVER_RECEIVER_PENDING_SETUP_TIMEOUT_NS`,
/// `aeron-driver/src/main/c/aeron_driver_receiver.c:47` — 100 ms).
pub const PENDING_SETUP_TIMEOUT_NS: i64 = 1_000_000_000;

/// What the conductor asks the receiver to do.
pub enum ReceiverCommand {
    /// Take ownership of an endpoint and its socket.
    AddEndpoint {
        /// The id the conductor knows it by.
        id: u64,
        /// The endpoint itself.
        endpoint: Box<ReceiveChannelEndpoint>,
    },
    /// A subscription to a whole stream arrived.
    AddSubscription {
        /// Which endpoint reads it.
        endpoint_id: u64,
        /// The stream.
        stream_id: i32,
    },
    /// A subscription that named a session arrived.
    AddSubscriptionBySession {
        /// Which endpoint reads it.
        endpoint_id: u64,
        /// The stream.
        stream_id: i32,
        /// The session it named.
        session_id: i32,
    },
    /// Ask the far end to describe a stream again
    /// (`aeron_driver_receiver_on_request_setup`, `:412-427`).
    ///
    /// The reference does this only when the channel named a control address —
    /// that is where the request goes, and a channel that named none has
    /// nowhere to send one — and only for a session it already knows, which is
    /// why the session is not optional here.
    RequestSetup {
        /// Which endpoint reads it.
        endpoint_id: u64,
        /// The stream.
        stream_id: i32,
        /// The session.
        session_id: i32,
    },
    /// A subscription went away.
    RemoveSubscription {
        /// Which endpoint read it.
        endpoint_id: u64,
        /// The stream.
        stream_id: i32,
        /// The session it named, when it named one.
        session_id: Option<i32>,
    },
    /// Take ownership of an image the conductor has built.
    AddImage {
        /// The image itself.
        image: Box<PublicationImage>,
    },
    /// Release an image.
    RemoveImage {
        /// Which one.
        registration_id: i64,
    },
    /// Refuse an image: a client rejected it, and its publisher has to be told
    /// (`aeron_driver_receiver_on_invalidate_image`,
    /// `aeron-driver/src/main/c/aeron_driver_receiver.c:633-649`).
    ///
    /// The command reaches the conductor first, which is what knows whether an
    /// image answers that registration id at all; what arrives here is the
    /// reason, because the reason is what the `ERR` frames carry and the
    /// receiver's thread is what sends them.
    ///
    /// **No position.** `REJECT_IMAGE` carries where the rejecting client had
    /// read to, and the reference drops it twice over: its receiver command has
    /// the field and never reads it (`aeron_driver_receiver.c:636-648`), and
    /// `aeron_publication_image_invalidate` does not take one
    /// (`aeron_publication_image.c:1383-1387`). A field nothing reads is not a
    /// fact about the wire, so it is not carried here.
    InvalidateImage {
        /// Which image.
        image_correlation_id: i64,
        /// The rejecting client's own words, which ride the `ERR` frames.
        reason: Vec<u8>,
    },
    /// Give an image a reader (`link_subscribable`'s image case).
    AddSubscriber {
        /// Which image.
        registration_id: i64,
        /// The reader's position.
        position: TetherablePosition,
    },
    /// Take a reader away from an image.
    RemoveSubscriber {
        /// Which image.
        registration_id: i64,
        /// The reader's counter.
        counter_id: i32,
    },
    /// Tell an image which session to answer a response channel with, or — with
    /// [`RESPONSE_NULL_SESSION_ID`](crate::publication_image::RESPONSE_NULL_SESSION_ID)
    /// — that it owes nobody one
    /// (`aeron_publication_image_set_response_session_id`,
    /// `aeron_publication_image.h:352-356`).
    ///
    /// It is a command rather than something the conductor writes because the
    /// image belongs to this thread, and it is one command for both directions
    /// because the reference's "clear it" is its "set it" with the sentinel
    /// (`aeron_publication_image_remove_response_session_id`, `:1389-1392`).
    SetResponseSessionId {
        /// Which image.
        registration_id: i64,
        /// The session, or the sentinel.
        response_session_id: i64,
    },
    /// Attach a destination a client added
    /// (`aeron_driver_receiver_on_add_destination`, `:442-497`).
    ///
    /// The destination arrives **built** — its socket open, the counter holding
    /// its address allocated — because the conductor is what has a counter
    /// manager (`aeron_driver_conductor.c:5903-5919`). This is the same shape as
    /// [`ReceiverCommand::AddEndpoint`], and for the same reason.
    AddDestination {
        /// Which endpoint reads from it.
        endpoint_id: u64,
        /// The destination itself.
        destination: Box<ReceiveDestination>,
    },
    /// Take a destination off (`aeron_driver_receiver_on_remove_destination`).
    RemoveDestination {
        /// Which endpoint read from it.
        endpoint_id: u64,
        /// Which destination, by the channel it was added with — the reference
        /// compares two by `aeron_udp_channel_equals`
        /// (`media/aeron_receive_channel_endpoint.c:877-905`).
        channel: Box<UdpChannel>,
    },
    /// Stop the thread.
    Stop,
}

/// What the receiver tells the conductor.
#[derive(Debug)]
pub enum ReceiverEvent {
    /// A `SETUP` arrived for a session nothing is serving: the conductor
    /// should build an image for it
    /// (`aeron_driver_conductor_proxy_on_create_publication_image_cmd`).
    CreateImage {
        /// Which endpoint it arrived on.
        endpoint_id: u64,
        /// The stream it names.
        stream_id: i32,
        /// The session it names.
        session_id: i32,
        /// The term id the stream started at.
        initial_term_id: i32,
        /// The term the sender is writing.
        active_term_id: i32,
        /// Where in that term.
        term_offset: i32,
        /// How long a term is.
        term_length: i32,
        /// The sender's MTU.
        mtu: i32,
        /// The header flags of that `SETUP`. They ride beside
        /// [`SetupFrame`](crate::protocol::SetupFrame) rather than inside it
        /// because they belong to the frame *header*: the body the frame
        /// carries is the same whatever the sender asked for, and the one bit
        /// the driver acts on — whether a response channel is wanted back — is
        /// the header's to say.
        setup_flags: u8,
        /// Where a control frame goes: the source of the `SETUP`, or the
        /// channel's control address.
        control_address: std::net::SocketAddr,
        /// Where the packet came from.
        source: std::net::SocketAddr,
    },
    /// An image has finished its life and may be released.
    ImageDone {
        /// Which one.
        registration_id: i64,
    },
    /// The untethered state machine moved a reader: the conductor is the side
    /// that can tell the client, and the only side that owns the transmitter.
    Untethered {
        /// Which image.
        registration_id: i64,
        /// What happened, per reader.
        events: Vec<crate::publication_image::UntetheredEvent>,
    },
    /// Something for the conductor to record.
    Fault {
        /// The protocol error code to record it under.
        error_code: i32,
        /// The words.
        description: String,
    },
}

/// The conductor's end of the conversation.
pub struct ReceiverProxy {
    /// The proxy's end of the commands it sends the thread.
    commands: Outbox<ReceiverCommand>,
    /// The thread's events, waiting to be taken.
    events: Inbox<ReceiverEvent>,
}

impl ReceiverProxy {
    /// Ask the receiver to take an endpoint.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_endpoint(&self, id: u64, endpoint: Box<ReceiveChannelEndpoint>) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::AddEndpoint { id, endpoint })
            .map_err(|_| stopped())
    }

    /// Hand the receiver a destination to start reading from.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_destination(
        &self,
        endpoint_id: u64,
        destination: Box<ReceiveDestination>,
    ) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::AddDestination {
                endpoint_id,
                destination,
            })
            .map_err(|_| stopped())
    }

    /// Tell the receiver to stop reading from a destination.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_destination(&self, endpoint_id: u64, channel: Box<UdpChannel>) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::RemoveDestination {
                endpoint_id,
                channel,
            })
            .map_err(|_| stopped())
    }

    /// Tell the receiver a subscription arrived.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_subscription(
        &self,
        endpoint_id: u64,
        stream_id: i32,
        session_id: Option<i32>,
    ) -> io::Result<()> {
        let command = match session_id {
            Some(session_id) => ReceiverCommand::AddSubscriptionBySession {
                endpoint_id,
                stream_id,
                session_id,
            },
            None => ReceiverCommand::AddSubscription {
                endpoint_id,
                stream_id,
            },
        };

        self.commands.send(command).map_err(|_| stopped())
    }

    /// Ask the receiver to elicit a setup for a session it reads.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn request_setup(
        &self,
        endpoint_id: u64,
        stream_id: i32,
        session_id: i32,
    ) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::RequestSetup {
                endpoint_id,
                stream_id,
                session_id,
            })
            .map_err(|_| stopped())
    }

    /// Tell the receiver a subscription went away.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_subscription(
        &self,
        endpoint_id: u64,
        stream_id: i32,
        session_id: Option<i32>,
    ) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::RemoveSubscription {
                endpoint_id,
                stream_id,
                session_id,
            })
            .map_err(|_| stopped())
    }

    /// Hand the receiver an image.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_image(&self, image: Box<PublicationImage>) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::AddImage { image })
            .map_err(|_| stopped())
    }

    /// Give an image a reader.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_subscriber(
        &self,
        registration_id: i64,
        position: TetherablePosition,
    ) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::AddSubscriber {
                registration_id,
                position,
            })
            .map_err(|_| stopped())
    }

    /// Take a reader away from an image.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_subscriber(&self, registration_id: i64, counter_id: i32) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::RemoveSubscriber {
                registration_id,
                counter_id,
            })
            .map_err(|_| stopped())
    }

    /// Refuse an image: reject it, and let it say so to its publisher.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn invalidate_image(&self, image_correlation_id: i64, reason: Vec<u8>) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::InvalidateImage {
                image_correlation_id,
                reason,
            })
            .map_err(|_| stopped())
    }

    /// Tell an image which session to answer a response channel with, or that
    /// it owes nobody one.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn set_response_session_id(
        &self,
        registration_id: i64,
        response_session_id: i64,
    ) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::SetResponseSessionId {
                registration_id,
                response_session_id,
            })
            .map_err(|_| stopped())
    }

    /// Release an image.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_image(&self, registration_id: i64) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::RemoveImage { registration_id })
            .map_err(|_| stopped())
    }

    /// Take everything the receiver has said since the last call.
    pub fn poll(&self) -> Vec<ReceiverEvent> {
        self.events.try_iter().collect()
    }

    /// Ask the thread to stop.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn stop(&self) -> io::Result<()> {
        self.commands
            .send(ReceiverCommand::Stop)
            .map_err(|_| stopped())
    }
}

/// The thread itself, with its proxy.
pub struct Receiver {
    proxy: ReceiverProxy,
    thread: Option<JoinHandle<()>>,
}

impl Receiver {
    /// Start the receiver thread.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the thread cannot be spawned.
    pub fn start(
        cnc: Arc<CncFile>,
        values_length: usize,
        free_to_reuse_timeout_ms: i64,
        mtu_length: usize,
        status_message_timeout_ns: i64,
        initial_window_length: i32,
        cycle_threshold_ns: i64,
    ) -> io::Result<Self> {
        let (command_tx, command_rx) = mpsc::channel::<ReceiverCommand>();
        let (event_tx, event_rx) = mpsc::channel::<ReceiverEvent>();

        let thread = std::thread::Builder::new()
            .name("deepmsg-receiver".to_owned())
            .spawn(move || {
                let Some(counters) = CounterManager::new(values_length, free_to_reuse_timeout_ms)
                else {
                    return;
                };

                let mut receiver = ReceiverThread::new(
                    cnc,
                    counters,
                    mtu_length,
                    status_message_timeout_ns,
                    initial_window_length,
                    cycle_threshold_ns,
                    event_tx,
                );
                receiver.run(&command_rx);
            })?;

        Ok(Self {
            proxy: ReceiverProxy {
                commands: command_tx,
                events: event_rx,
            },
            thread: Some(thread),
        })
    }

    /// The conductor's end.
    pub const fn proxy(&self) -> &ReceiverProxy {
        &self.proxy
    }

    /// Ask the thread to stop and wait for it.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is already gone or panicked.
    pub fn close(&mut self) -> io::Result<()> {
        let _ = self.proxy.stop();

        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("the receiver thread panicked"))?;
        }

        Ok(())
    }
}

fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "the receiver thread has stopped")
}

/// A session whose `SETUP` was asked for and has not arrived
/// (`aeron_driver_receiver_pending_setup_entry_t`,
/// `aeron-driver/src/main/c/aeron_driver_receiver.h:60-70`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingSetup {
    pub(crate) endpoint_id: u64,
    pub(crate) stream_id: i32,
    pub(crate) session_id: i32,
    /// Where the eliciting status message goes, or [`None`] when it goes to
    /// wherever the packets came from.
    ///
    /// This is also what makes an entry **periodic**
    /// (`aeron_driver_receiver_add_pending_setup`, `:653-687`: `is_periodic` is
    /// false, and set true only when a control address was given). A periodic
    /// entry is asked again every [`PENDING_SETUP_TIMEOUT_NS`]; one that is not
    /// periodic is **given up on** after that long, and the session's interest
    /// is dropped so that the next frame from it asks again.
    pub(crate) control_address: Option<std::net::SocketAddr>,
    /// When it was last sent.
    pub(crate) time_of_status_message_ns: i64,
}

/// What the thread owns.
struct ReceiverThread {
    cnc: Arc<CncFile>,
    counters: CounterManager,
    #[allow(dead_code)]
    // the datagram size the buffers are built from, kept for a caller that re-reads it
    mtu_length: usize,
    #[allow(dead_code)] // the image's status-message cadence, which the image itself carries
    status_message_timeout_ns: i64,
    #[allow(dead_code)] // the window an image offers, which the conductor passes at create
    initial_window_length: i32,
    cycle_threshold_ns: i64,
    events: Outbox<ReceiverEvent>,
    endpoints: Vec<(u64, Box<ReceiveChannelEndpoint>)>,
    images: Vec<PublicationImage>,
    pending_setups: Vec<PendingSetup>,
    buffers: Vec<Vec<u8>>,
    datagrams: Datagrams,
    last_cycle_ns: i64,
    idle: Backoff,
}

impl ReceiverThread {
    #[allow(clippy::too_many_arguments)]
    fn new(
        cnc: Arc<CncFile>,
        counters: CounterManager,
        mtu_length: usize,
        status_message_timeout_ns: i64,
        initial_window_length: i32,
        cycle_threshold_ns: i64,
        events: Outbox<ReceiverEvent>,
    ) -> Self {
        Self {
            cnc,
            counters,
            mtu_length,
            status_message_timeout_ns,
            initial_window_length,
            cycle_threshold_ns,
            events,
            endpoints: Vec::new(),
            images: Vec::new(),
            pending_setups: Vec::new(),
            buffers: (0..RECEIVE_SLOTS).map(|_| vec![0u8; mtu_length]).collect(),
            datagrams: Datagrams::new(),
            last_cycle_ns: deepmsg_core::clock::monotonic_nano_time(),
            idle: Backoff::new(),
        }
    }

    fn run(&mut self, commands: &Inbox<ReceiverCommand>) {
        loop {
            let mut stop = false;

            for command in commands.try_iter() {
                match command {
                    ReceiverCommand::AddEndpoint { id, endpoint } => {
                        // An endpoint whose channel named a `control=` asks
                        // **first**, and asks as soon as it exists
                        // (`aeron_driver_receiver_on_add_endpoint`,
                        // `aeron-driver/src/main/c/aeron_driver_receiver.c:305-320`,
                        // whose `add_pending_setup` is the endpoint's half of
                        // what the destination path below does for a client's).
                        //
                        // It is the whole of who speaks on a dynamic channel:
                        // the sender has nowhere to send until it is asked, so
                        // a subscription whose own channel named the control
                        // address that stayed silent would be a session neither
                        // side could start.
                        let now_ns = deepmsg_core::clock::monotonic_nano_time();
                        let mut endpoint = endpoint;

                        for index in 0..endpoint.destination_count() {
                            if let Some(setup) = endpoint.ask_for_setup(id, index, now_ns) {
                                self.pending_setups.push(setup);
                            }
                        }

                        self.endpoints.push((id, endpoint));
                    }
                    ReceiverCommand::AddDestination {
                        endpoint_id,
                        destination,
                    } => {
                        // A destination whose channel named a `control=` is one
                        // the sender does not know about: the receiver has to
                        // ask, and has to keep asking until it is answered
                        // (`aeron_driver_receiver.c:475-486`, which adds a
                        // periodic pending setup for exactly these).
                        let now_ns = deepmsg_core::clock::monotonic_nano_time();
                        let setup_address = destination.setup_address();

                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            endpoint.add_destination(*destination);

                            let index = endpoint.destination_count() - 1;
                            if let Some(setup) = endpoint.ask_for_setup(endpoint_id, index, now_ns)
                            {
                                self.pending_setups.push(setup);
                            }
                        }

                        // And every image already running on that endpoint hears
                        // from it too (`aeron_driver_receiver.c:483-486`): a
                        // stream that is already up has to send its status
                        // messages and NAKs to the new source as well.
                        for image in self.images.iter_mut() {
                            if image.endpoint_id == endpoint_id {
                                image.add_destination(setup_address, now_ns);
                            }
                        }
                    }
                    ReceiverCommand::RemoveDestination {
                        endpoint_id,
                        channel,
                    } => {
                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            // The counter the destination held is not freed
                            // here: the conductor allocated it, and giving a
                            // counter back is the conductor's to do — the
                            // receiver only stops reading.
                            let _ = endpoint.remove_destination(&channel);
                        }
                    }
                    ReceiverCommand::AddSubscription {
                        endpoint_id,
                        stream_id,
                    } => {
                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            endpoint.add_subscription(stream_id);
                        }
                    }
                    ReceiverCommand::AddSubscriptionBySession {
                        endpoint_id,
                        stream_id,
                        session_id,
                    } => {
                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            endpoint.add_subscription_by_session(stream_id, session_id);

                            // And then the ask, which is the whole reason a
                            // session-addressed subscription exists here: a
                            // response subscription names no session until the
                            // far end's `RSP_SETUP` says which one it is, and
                            // *this* is where that session is finally asked
                            // for by name (`aeron_driver_receiver.c:401-409`).
                            //
                            // Unlike the ask beside a destination
                            // (`:475-486`), this one carries a real stream and
                            // session, and that is not a detail: the far end
                            // finds its publication by
                            // `(stream_id << 32) | session_id`, so an ask with
                            // no session in it reaches no publication at all —
                            // it reaches only the destination tracker.
                            //
                            // A channel with no control address has nowhere to
                            // send it, so the guard is the channel's
                            // (`:418-426`).
                            if endpoint.channel.has_explicit_control {
                                endpoint.elicit_setup_to_destinations(stream_id, session_id);
                            }
                        }
                    }
                    ReceiverCommand::RequestSetup {
                        endpoint_id,
                        stream_id,
                        session_id,
                    } => {
                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            // A channel that named no control address has
                            // nowhere to send the request (`:418-426`), so the
                            // guard is the channel's, not the caller's.
                            if endpoint.channel.has_explicit_control {
                                endpoint.elicit_setup_to_destinations(stream_id, session_id);
                            }
                        }
                    }
                    ReceiverCommand::RemoveSubscription {
                        endpoint_id,
                        stream_id,
                        session_id,
                    } => {
                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            match session_id {
                                Some(session_id) => {
                                    endpoint.remove_subscription_by_session(stream_id, session_id);
                                }
                                None => {
                                    endpoint.remove_subscription(stream_id);
                                }
                            }
                        }
                    }
                    ReceiverCommand::AddImage { image } => {
                        let image = *image;
                        let registration_id = image.registration_id;
                        let stream_id = image.stream_id;
                        let session_id = image.session_id;
                        let endpoint_id = image.endpoint_id;

                        if let Some((_, endpoint)) =
                            self.endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
                        {
                            endpoint.dispatcher_mut().add_image(
                                stream_id,
                                session_id,
                                registration_id,
                            );
                        }

                        self.images.push(image);
                    }
                    ReceiverCommand::RemoveImage { registration_id } => {
                        // The image goes, and its log buffer goes with it: the
                        // file is unmapped and unlinked here because this is
                        // the thread that owns the mapping.
                        if let Some(index) = self
                            .images
                            .iter()
                            .position(|image| image.registration_id == registration_id)
                        {
                            let image = self.images.swap_remove(index);

                            if let Err(error) = image.into_log().remove() {
                                let _ = self.events.send(ReceiverEvent::Fault {
                                    error_code: deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                                    description: format!(
                                        "could not remove an image's log buffer: {error}"
                                    ),
                                });
                            }
                        }
                        self.pending_setups
                            .retain(|pending| pending.session_id != registration_id as i32);

                        for (_, endpoint) in &mut self.endpoints {
                            let images = endpoint.dispatcher().images();
                            for (stream_id, session_id, image_id) in images {
                                if image_id == registration_id {
                                    endpoint.dispatcher_mut().remove_image(
                                        stream_id,
                                        session_id,
                                        registration_id,
                                        false,
                                    );
                                }
                            }
                        }
                    }
                    ReceiverCommand::InvalidateImage {
                        image_correlation_id,
                        reason,
                    } => {
                        // The image is marked, not removed: what happens next
                        // is that it tells its publisher, on the status
                        // message's own timer, and only the conductor's
                        // removal takes it away. A command that cannot find
                        // its image is one that was removed between the two
                        // threads, which is a race the reference has too — its
                        // loop simply finds nothing (`:640-648`).
                        if let Some(image) = self
                            .images
                            .iter_mut()
                            .find(|image| image.registration_id == image_correlation_id)
                        {
                            image.invalidate(&reason);
                        }
                    }
                    ReceiverCommand::AddSubscriber {
                        registration_id,
                        position,
                    } => {
                        if let Some(image) = self
                            .images
                            .iter_mut()
                            .find(|image| image.registration_id == registration_id)
                        {
                            image.add_subscriber(position);
                        }
                    }
                    ReceiverCommand::RemoveSubscriber {
                        registration_id,
                        counter_id,
                    } => {
                        if let Some(image) = self
                            .images
                            .iter_mut()
                            .find(|image| image.registration_id == registration_id)
                        {
                            image.remove_subscriber(counter_id);
                        }
                    }
                    ReceiverCommand::SetResponseSessionId {
                        registration_id,
                        response_session_id,
                    } => {
                        if let Some(image) = self
                            .images
                            .iter_mut()
                            .find(|image| image.registration_id == registration_id)
                        {
                            image.set_response_session_id(response_session_id);
                        }
                    }
                    ReceiverCommand::Stop => stop = true,
                }
            }

            let work = if stop { 0 } else { self.do_work() };
            self.idle.idle(work);

            if stop {
                break;
            }
        }
    }

    /// One pass (`aeron_driver_receiver_do_work`,
    /// `aeron-driver/src/main/c/aeron_driver_receiver.c:130-260`).
    fn do_work(&mut self) -> usize {
        let cnc = Arc::clone(&self.cnc);
        let Some(regions) = cnc.counter_regions() else {
            return 0;
        };
        let system = System::new(&self.counters, &regions);
        let now_ns = deepmsg_core::clock::monotonic_nano_time();

        let mut work = Self::receive_datagrams(
            &mut self.endpoints,
            &mut self.images,
            &mut self.buffers,
            &mut self.datagrams,
            &mut self.pending_setups,
            &system,
            &self.counters,
            &regions,
            &self.events,
            &cnc,
            now_ns,
        );

        work += Self::send_status_messages(
            &mut self.endpoints,
            &mut self.images,
            &system,
            &self.counters,
            &regions,
            now_ns,
        );

        work += Self::send_pending_setups(
            &mut self.pending_setups,
            &mut self.endpoints,
            &system,
            now_ns,
        );

        work += Self::check_untethered_subscriptions(
            &mut self.images,
            &mut self.counters,
            &regions,
            &self.events,
            now_ns,
        );
        work += self.run_time_events(&regions, now_ns);
        Self::track_cycle(
            &self.counters,
            &regions,
            self.cycle_threshold_ns,
            &mut self.last_cycle_ns,
        );

        work
    }

    /// Read every endpoint's socket and give each datagram to the thing that
    /// wants it (`aeron_receive_channel_endpoint_dispatch`, `:535-553`).
    #[allow(clippy::too_many_arguments)]
    fn receive_datagrams(
        endpoints: &mut [(u64, Box<ReceiveChannelEndpoint>)],
        images: &mut [PublicationImage],
        buffers: &mut [Vec<u8>],
        datagrams: &mut Datagrams,
        pending_setups: &mut Vec<PendingSetup>,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        events: &Outbox<ReceiverEvent>,
        cnc: &Arc<CncFile>,
        now_ns: i64,
    ) -> usize {
        let _ = cnc;
        let mut work = 0;

        for (endpoint_id, endpoint) in endpoints.iter_mut() {
            // Every destination, not just the first: a multi-destination channel
            // has one socket per destination and a datagram that arrives on any
            // of them is a datagram this endpoint has to read
            // (`aeron_driver_receiver_do_work`, `:130-260`). With one
            // destination — which is every channel until a client adds another —
            // this is the single poll it always was.
            //
            // Which destination a datagram arrived on is *not* passed on yet.
            // Its only reader is the reply path — an answer has to leave through
            // the socket it arrived on, not through the first one — and that
            // arrives with the commands that create a second destination. The
            // compiler objected to the parameter existing before then, correctly:
            // a parameter nothing reads is not a fact about the wire.
            for destination_index in 0..endpoint.destination_count() {
                let received = match endpoint.receive_from(destination_index, buffers, datagrams) {
                    Ok(received) => received,
                    Err(error) => {
                        let _ = events.send(ReceiverEvent::Fault {
                            error_code: deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                            description: format!("could not receive on a channel: {error}"),
                        });
                        continue;
                    }
                };

                if received == 0 {
                    continue;
                }

                let batch = *datagrams;
                let mut bytes_received = 0i64;

                for (slot, datagram) in batch.as_slice().iter().enumerate() {
                    bytes_received += i64::try_from(datagram.length).unwrap_or(0);
                    work += 1;

                    let Some(source) = datagram.source else {
                        continue;
                    };

                    let packet = &buffers[slot][..datagram.length];
                    Self::dispatch(
                        *endpoint_id,
                        destination_index,
                        endpoint,
                        images,
                        pending_setups,
                        packet,
                        source,
                        system,
                        counters,
                        regions,
                        events,
                        now_ns,
                    );
                }

                system.add(system_counters::id::BYTES_RECEIVED, bytes_received);
            }
        }

        work
    }

    /// One datagram, to the dispatcher
    /// (`aeron_receive_channel_endpoint_dispatch`, `:535-553`).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)] // the frame, and where it arrived
    fn dispatch(
        endpoint_id: u64,
        destination_index: usize,
        endpoint: &mut ReceiveChannelEndpoint,
        images: &mut [PublicationImage],
        pending_setups: &mut Vec<PendingSetup>,
        packet: &[u8],
        source: std::net::SocketAddr,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        events: &Outbox<ReceiverEvent>,
        now_ns: i64,
    ) {
        let Some(header) = FrameHeader::read(packet) else {
            return;
        };

        if !is_frame_valid(&header, packet.len()) {
            system.increment(system_counters::id::INVALID_PACKETS);
            return;
        }

        match header.frame_type {
            frame_type::PAD | frame_type::DATA => {
                // Both kinds a term holds, through the reader that admits both:
                // a padding frame carries no payload anyone will read, and it
                // is still a frame the image has to write into its term —
                // `DataFrame::read` refuses it, and a receiver that used that
                // one dropped every padding datagram, leaving the term's tail
                // unwritten and its gap scanner asking for it forever.
                let Some(frame) = crate::protocol::DataFrame::read_in_a_term(packet) else {
                    return;
                };

                let is_eos = header.flags & crate::protocol::header_flags::EOS != 0;

                match endpoint.on_data(frame.stream_id, frame.session_id, is_eos) {
                    Interest::Image { registration_id } => {
                        if let Some(image) = images
                            .iter_mut()
                            .find(|image| image.registration_id == registration_id)
                        {
                            image.insert_packet(
                                frame.term_id,
                                frame.term_offset,
                                packet,
                                source,
                                system,
                                counters,
                                regions,
                                now_ns,
                            );
                        }
                    }
                    Interest::ElicitSetup => {
                        // The packet named a session nothing serves: ask the
                        // sender for a SETUP, and remember that we asked
                        // (`elicit_setup_from_source`, `:616-659`).
                        if endpoint.elicit_setup(frame.stream_id, frame.session_id)
                            && endpoint
                                .send_sm_from(
                                    destination_index,
                                    endpoint.control_address(destination_index, source),
                                    frame.stream_id,
                                    frame.session_id,
                                    0,
                                    0,
                                    0,
                                    ReceiveChannelEndpoint::send_setup_flag(),
                                )
                                .is_ok()
                        {
                            system.increment(system_counters::id::STATUS_MESSAGES_SENT);
                        }

                        // And remember that we asked — **without** a control
                        // address, which is what makes this entry non-periodic
                        // (`aeron_driver_receiver_add_pending_setup`, `:653-687`:
                        // this path is given a `NULL` there).
                        //
                        // That is the whole point of recording it: a session
                        // that never answers is given up on after a second, its
                        // interest is dropped, and the next frame from it asks
                        // again. `elicit_setup` alone answers yes once per
                        // session for ever, so before this a sender that missed
                        // the first request was never asked a second time.
                        pending_setups.push(PendingSetup {
                            endpoint_id,
                            stream_id: frame.stream_id,
                            session_id: frame.session_id,
                            control_address: None,
                            time_of_status_message_ns: now_ns,
                        });
                    }
                    Interest::None => {}
                }
            }
            frame_type::SETUP => {
                let Some(setup) = SetupFrame::read(packet) else {
                    return;
                };
                let setup_flags = header.flags;

                if !endpoint
                    .dispatcher_mut()
                    .on_setup(setup.stream_id, setup.session_id)
                {
                    return;
                }

                let control_address = endpoint.control_address(destination_index, source);
                let _ = endpoint_id;

                let _ = events.send(ReceiverEvent::CreateImage {
                    endpoint_id,
                    stream_id: setup.stream_id,
                    session_id: setup.session_id,
                    initial_term_id: setup.initial_term_id,
                    active_term_id: setup.active_term_id,
                    term_offset: setup.term_offset,
                    term_length: setup.term_length,
                    mtu: setup.mtu,
                    setup_flags,
                    control_address,
                    source,
                });
            }
            frame_type::RTTM => {
                // An RTTM is the answer to a measurement this build does not
                // ask for: `max` with the static window never measures a round
                // trip (`aeron_static_window_congestion_control_strategy_should_measure_rtt`,
                // `aeron_congestion_control.c:78-81`).
                system.increment(system_counters::id::STATUS_MESSAGES_RECEIVED);
            }
            _ => {}
        }
    }

    /// The untethered state machine for every image, once per pass
    /// (`aeron_publication_image_check_untethered_subscriptions`, run from the
    /// image's time event on the conductor in the reference and here from the
    /// thread that owns the readers' positions).
    fn check_untethered_subscriptions(
        images: &mut [PublicationImage],
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        events: &Outbox<ReceiverEvent>,
        now_ns: i64,
    ) -> usize {
        let mut work = 0;

        for image in images.iter_mut() {
            let moved = image.check_untethered_subscriptions(counters, regions, now_ns);

            if !moved.is_empty() {
                work += moved.len();
                let _ = events.send(ReceiverEvent::Untethered {
                    registration_id: image.registration_id,
                    events: moved,
                });
            }
        }

        work
    }

    /// Every image's status message, and the NAK its loss detector owes
    /// (`aeron_driver_receiver.c:170-209`).
    #[allow(clippy::too_many_arguments)]
    fn send_status_messages(
        endpoints: &mut [(u64, Box<ReceiveChannelEndpoint>)],
        images: &mut [PublicationImage],
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> usize {
        let mut work = 0;

        for image in images.iter_mut() {
            let Some((_, endpoint)) = endpoints
                .iter_mut()
                .find(|(id, _)| *id == image.endpoint_id)
            else {
                continue;
            };

            // What a reader may read, which is what the next status message
            // will say — and the hole, if one is in the way and has waited long
            // enough to be asked for (`send_pending_loss`, `:995-1075`).
            let gap = image.track_rebuild(counters, regions, now_ns);

            if let Some(gap) = gap {
                if image.is_reliable() {
                    // A NAK goes to every connection this image hears from, like
                    // a status message: each receiver has its own view of what is
                    // missing, and one of them having it is not the others having
                    // it. With one connection this is the message it always was.
                    for connection in &image.connections {
                        let Some(control_address) = connection.control_address else {
                            continue;
                        };

                        if endpoint
                            .send_nak(
                                control_address,
                                image.stream_id,
                                image.session_id,
                                gap.term_id,
                                gap.term_offset,
                                i32::try_from(gap.length).unwrap_or(i32::MAX),
                            )
                            .is_ok()
                        {
                            system.increment(system_counters::id::NAK_MESSAGES_SENT);
                            // …and the same count under the image that asked for
                            // it: the system counter says the driver is
                            // retransmitting, this one says for which stream
                            // (`aeron_publication_image.c:1052`).
                            let _ = system_counters::increment(
                                counters,
                                regions,
                                image.counters().rcv_naks_sent,
                            );
                            work += 1;
                        }
                    }
                } else {
                    // An unreliable image does not ask: it covers the hole and
                    // reads on, so whatever was in it is gone for good — which
                    // is what the channel asked for
                    // (`aeron_publication_image_send_pending_loss`,
                    // `:1053-1066`). Nothing goes on the network, and the
                    // counter is the only trace of it.
                    //
                    // `work` moves whether or not the fill was made, as the
                    // reference's `work_count = 1` does: a gap that could not be
                    // filled is one something else has landed in, and the next
                    // scan will not find a gap at all.
                    if image.fill_gap(gap) {
                        system.increment(system_counters::id::LOSS_GAP_FILLS);
                    }

                    work += 1;
                }
            }

            if image
                .send_pending_status_message(endpoint, counters, regions, system, now_ns)
                .is_ok()
            {
                work += 1;
            }
        }

        work
    }

    /// Ask again for the `SETUP` of a session that has not answered
    /// (`aeron_driver_receiver.c:211-252`).
    fn send_pending_setups(
        pending_setups: &mut Vec<PendingSetup>,
        endpoints: &mut [(u64, Box<ReceiveChannelEndpoint>)],
        system: &System<'_>,
        now_ns: i64,
    ) -> usize {
        let mut work = 0;
        let mut index = pending_setups.len();

        while index > 0 {
            index -= 1;
            let pending = pending_setups[index];

            if now_ns <= pending.time_of_status_message_ns + PENDING_SETUP_TIMEOUT_NS {
                continue;
            }

            let Some((_, endpoint)) = endpoints
                .iter_mut()
                .find(|(id, _)| *id == pending.endpoint_id)
            else {
                // The endpoint is gone, so there is nothing left to ask and
                // nothing left to tell. The entry goes with it.
                pending_setups.swap_remove(index);
                continue;
            };

            let Some(control_address) = pending.control_address else {
                // Not periodic: a session that has not answered in a second is
                // given up on (`:215-227`), and dropping the interest is what
                // lets the next frame from it ask again.
                endpoint.remove_pending_setup(pending.stream_id, pending.session_id);
                pending_setups.swap_remove(index);
                work += 1;
                continue;
            };

            let sent = endpoint.send_sm(
                control_address,
                pending.stream_id,
                pending.session_id,
                0,
                0,
                0,
                ReceiveChannelEndpoint::send_setup_flag(),
            );

            if sent.is_ok() {
                system.increment(system_counters::id::STATUS_MESSAGES_SENT);
                work += 1;
            }

            pending_setups[index].time_of_status_message_ns = now_ns;
        }

        work
    }

    /// The image lifecycle, once per pass.
    fn run_time_events(&mut self, regions: &CounterRegions<'_>, now_ns: i64) -> usize {
        let mut work = 0;
        let mut done = Vec::new();

        for image in self.images.iter_mut() {
            if image.on_time_event(&self.counters, regions, now_ns) {
                work += 1;
            }

            if image.state == crate::publication_image::ImageState::Done {
                done.push(image.registration_id);
            }
        }

        for registration_id in done {
            let _ = self
                .events
                .send(ReceiverEvent::ImageDone { registration_id });
        }

        work
    }

    /// The cycle-time counters, which are 30 and 31
    /// (`aeron_driver_receiver.c:262-276`).
    fn track_cycle(
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        cycle_threshold_ns: i64,
        last_cycle_ns: &mut i64,
    ) {
        let now_ns = deepmsg_core::clock::monotonic_nano_time();
        let cycle_ns = now_ns.saturating_sub(*last_cycle_ns);
        *last_cycle_ns = now_ns;

        system_counters::propose_max(
            counters,
            regions,
            system_counters::id::RECEIVER_MAX_CYCLE_TIME,
            cycle_ns,
        );

        if cycle_ns > cycle_threshold_ns {
            system_counters::increment(
                counters,
                regions,
                system_counters::id::RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_that_is_asked_to_stop_stops() {
        let dir = std::env::temp_dir().join(format!("deepmsg-receiver-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");

        let cnc = CncFile::create(
            &dir,
            &deepmsg_cnc::CncLayout {
                counters_values_length: deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN,
                ..deepmsg_cnc::CncLayout::default()
            },
            &deepmsg_cnc::CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: deepmsg_core::clock::epoch_millis(),
                pid: i64::from(std::process::id()),
            },
        )
        .expect("a CnC file");

        let mut receiver = Receiver::start(
            Arc::new(cnc),
            deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN,
            1_000,
            1408,
            crate::publication_image::STATUS_MESSAGE_TIMEOUT_NS,
            128 * 1024,
            100_000_000,
        )
        .expect("a receiver");

        // A pass with nothing to do is a pass, and the thread answers with
        // silence rather than an error.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(receiver.proxy().poll().is_empty());

        receiver.close().expect("a clean stop");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
