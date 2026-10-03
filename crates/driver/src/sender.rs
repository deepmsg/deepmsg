//! The sender agent: the thread that owns the send sockets and the
//! publications.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_driver_sender.c`. It is the first
//! thing in this build that is a *thread of its own*, and what it exists to
//! keep apart is the conductor's control plane from the data plane's two
//! costs: a `sendmmsg` on a socket whose buffer is full, and a `recvmmsg` that
//! walks every endpoint's queue.
//!
//! # It sends *and* receives
//!
//! A publication is not one-way: status messages (the receivers' flow
//! control), NAKs, RTTMs and ERR frames arrive on the *same* socket the data
//! leaves by, and they are read here — by the sender's own poll
//! (`aeron_driver_sender.c:154-171`). That is why the reference's sender has a
//! poller at all, and why a data plane without this half cannot be told what
//! the far end has room for. `send.to.sm.poll.ratio` decides how often the
//! second half runs; this build polls every pass, which is that ratio at one
//! and costs only the syscalls.
//!
//! # Who owns what
//!
//! The conductor decides — it parses the channel, allocates the counters, and
//! creates the endpoints and the publications — and then *moves* them here
//! through [`SenderCommand`]. That is the reference's arrangement too (its
//! `aeron_driver_sender_proxy_on_add_endpoint` is called from
//! `aeron_driver_conductor.c:2015`), and it is what keeps each data structure
//! with one owner: after the handover an endpoint's socket and a publication's
//! term buffer have exactly one, and every other party addresses them by id.
//!
//! # Counters, without owning them
//!
//! Both threads need the counters, and neither may own the mapping. The
//! conductor holds the [`CncFile`] behind an `Arc`, and each agent derives its
//! own [`CounterRegions`] view per pass — the same memory, seen through the
//! same atomics, which is what the whole CnC contract is. The agent's
//! [`CounterManager`] is its own and is used only to *read and write values*:
//! allocation stays on the conductor, where the ownership rules are.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender as Channel};
use std::thread::JoinHandle;

use deepmsg_cnc::command::OwnedPublicationError;
use deepmsg_cnc::layout::NULL_VALUE;
use deepmsg_cnc::{CncFile, CounterManager, CounterRegions, layout};

use crate::driver::Role;
use crate::media::destination_tracker::DESTINATION_TIMEOUT_NS;
use crate::media::send_endpoint::SendChannelEndpoint;
use crate::network_publication::NetworkPublication;
use crate::protocol::{
    ErrorFrame, FRAME_ALIGNMENT, FrameHeader, MAX_ERROR_TEXT_LENGTH, NakFrame, RspSetupFrame,
    RttmFrame, StatusMessageFrame, frame_type, header_flags, is_frame_valid,
};
use crate::subscribable::{TetherablePosition, UntetheredEvent};
use crate::sys::socket::Datagrams;
use crate::system_counters::{self, System};
use crate::udp_channel::UdpChannel;
use deepmsg_core::logbuffer::descriptor::TERM_MAX_LENGTH;

/// How many datagrams one poll may read
/// (`AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX`,
/// `aeron-driver/src/main/c/aeron_driver_context.h:55`).
const RECEIVE_SLOTS: usize = 16;

/// What the conductor asks the sender to do.
pub enum SenderCommand {
    /// Take ownership of an endpoint and its socket.
    AddEndpoint {
        /// The id the conductor knows it by.
        id: u64,
        /// The endpoint itself.
        endpoint: Box<SendChannelEndpoint>,
    },
    /// Take ownership of a publication and start sending it.
    AddPublication {
        /// The publication itself.
        publication: Box<NetworkPublication>,
    },
    /// Stop sending a publication.
    RemovePublication {
        /// Which one.
        registration_id: i64,
    },
    /// The publication's last client has let go, so it may **end**: write where
    /// the stream stopped and linger while it still has readers to tell
    /// (`aeron_driver_managed_resource_event_t`'s `DECREF`-to-zero,
    /// `media/aeron_network_publication.c:1048-1069`).
    ///
    /// Not the same as [`SenderCommand::RemovePublication`], and the difference
    /// is the whole reason a channel can be published on again right after its
    /// publication is closed: the endpoint is released with the publication,
    /// and a publication that lingers holds it.
    EndPublication {
        /// Which one.
        registration_id: i64,
    },
    /// The name resolved somewhere else: point the endpoint at it
    /// (`aeron_driver_sender_on_resolution_change`,
    /// `aeron-driver/src/main/c/aeron_driver_sender.c:400-415`), which is also
    /// where `Resolution changes` is counted.
    ResolutionChange {
        /// Which endpoint, by the id the conductor knows it by.
        endpoint_id: u64,
        /// The name that resolved — what a multi-destination endpoint matches
        /// its destinations by.
        endpoint_name: String,
        /// Where it resolved to.
        new_addr: SocketAddr,
    },
    /// Cut a publication's stream off, telling its readers why
    /// (`AERON_DRIVER_MANAGED_RESOURCE_EVENT_REVOKE`, which
    /// `aeron_network_publication_handle_managed_resource_event` answers by
    /// setting the byte on its log metadata, `:1075-1079`).
    ///
    /// The conductor cannot do this itself for the reason it cannot give a
    /// publication a reader: the log buffer is the sender's. The flag is what
    /// travels, and everything that follows — the heartbeat saying `REVOKED`,
    /// the image at the far end draining, the readers being told — is already
    /// this build's.
    RevokePublication {
        /// Which one.
        registration_id: i64,
    },
    /// Give a publication a local reader
    /// (`aeron_driver_subscribable_add_position`,
    /// `aeron_driver_conductor.c:3497-3523`).
    ///
    /// The conductor cannot do this itself: what a reader joins is the
    /// publication's own subscribable set, and a network publication is the
    /// sender's. A publication that has gone in the meantime is skipped — the
    /// link is the conductor's record either way, and the reader is a position
    /// nothing will ever compute a limit from.
    AddSubscriber {
        /// Which publication.
        registration_id: i64,
        /// The reader, whose counter the conductor has **already** seeded:
        /// the sender reads positions to compute the producer's limit, and a
        /// reader that appeared at zero would hold the producer back to a
        /// place it never was.
        position: TetherablePosition,
    },
    /// Take one away (`aeron_driver_subscribable_remove_position`, `:3525-3545`).
    ///
    /// The counter goes back to the conductor's hands after this, which is why
    /// the message is sent before the free and not after: a set that still
    /// holds a freed id reads whatever took its place.
    RemoveSubscriber {
        /// Which publication.
        registration_id: i64,
        /// Which reader.
        counter_id: i32,
    },
    /// Close an endpoint's socket and give the endpoint up.
    RemoveEndpoint {
        /// Which one.
        id: u64,
    },
    /// Put a destination on an endpoint's tracker
    /// (`aeron_driver_sender_on_add_destination`,
    /// `aeron-driver/src/main/c/aeron_driver_sender.c:350-363`).
    ///
    /// The conductor answers the *client* before this runs — the reference does
    /// (`aeron_driver_conductor.c:5366-5367`) — so this is the sender catching
    /// up, not a step anyone is waiting on.
    AddDestination {
        /// The endpoint whose tracker it goes on.
        endpoint_id: u64,
        /// The destination's channel, as the client named it. Boxed for the
        /// same reason [`SenderCommand::AddEndpoint`]'s endpoint is: a channel
        /// is much larger than the rest of these commands, and one variant
        /// would otherwise set the size of every one.
        channel: Box<UdpChannel>,
        /// Where it resolved to; [`None`] is an address that did not resolve,
        /// which is kept and skipped rather than refused (`:5337-5343`).
        address: Option<SocketAddr>,
        /// The id the client removes it by.
        registration_id: i64,
    },
    /// Take a destination off an endpoint's tracker by the address it was added
    /// with (`aeron_driver_sender_on_remove_destination`, `:365-385`).
    RemoveDestination {
        /// The endpoint whose tracker it comes off.
        endpoint_id: u64,
        /// Which destination.
        address: SocketAddr,
    },
    /// The same, by the id the client was given.
    RemoveDestinationById {
        /// The endpoint whose tracker it comes off.
        endpoint_id: u64,
        /// Which destination.
        registration_id: i64,
    },
    /// Stop the thread.
    Stop,
}

impl SenderCommand {
    /// The endpoint a destination command names, or [`None`] for the commands
    /// that are not about destinations.
    const fn destination_endpoint_id(&self) -> Option<u64> {
        match self {
            Self::AddDestination { endpoint_id, .. }
            | Self::RemoveDestination { endpoint_id, .. }
            | Self::RemoveDestinationById { endpoint_id, .. } => Some(*endpoint_id),
            _ => None,
        }
    }
}

/// What the sender tells the conductor.
#[derive(Debug)]
pub enum SenderEvent {
    /// An endpoint was closed: its channel-status counter can be freed, which
    /// only the conductor may do.
    EndpointRemoved {
        /// Which endpoint.
        id: u64,
    },
    /// A revoked publication has nothing left to tell its readers
    /// (`aeron_network_publication_check_managed_resources`'s LINGER arm,
    /// `:1327-1340`), so the conductor may finish releasing it.
    ///
    /// It is the second half of a two-phase removal: the client's link goes
    /// when it asks to, and the publication stays long enough to say
    /// `REVOKED` — a publication dropped in the same pass as its revoke would
    /// never get to say anything.
    PublicationDrained {
        /// Which one.
        registration_id: i64,
    },
    /// A publication is no longer being sent.
    PublicationRemoved {
        /// Which publication.
        registration_id: i64,
    },
    /// An endpoint has heard nothing for its destination timeout, so its
    /// channel's name has to be resolved again
    /// (`aeron_driver_conductor_proxy_on_re_resolve_endpoint`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor_proxy.c:73-89`).
    ///
    /// The conductor resolves it and answers with
    /// [`SenderCommand::ResolutionChange`] — or with nothing at all, when the
    /// name resolves to the address the endpoint already has, which is the
    /// reference's `memcmp` (`aeron_driver_conductor.c:6932-6938`).
    ReResolveEndpoint {
        /// Which endpoint, by the id the conductor knows it by.
        endpoint_id: u64,
        /// The name the channel named, which is what is resolved again.
        endpoint_name: String,
        /// The address it has now, which the answer is compared against —
        /// [`None`] for a destination that never resolved, which is the
        /// reference's `AF_UNSPEC` and is always a change once it does.
        address: Option<SocketAddr>,
    },
    /// A responder answered a publication that asked for a response channel
    /// (`aeron_driver_conductor_proxy_on_response_setup`,
    /// `aeron_driver_conductor_proxy.c:157-172`).
    ///
    /// The frame named the publication; the correlation id is read off the
    /// publication itself, which is what ties the answer to the subscription
    /// waiting for it — the frame carries no correlation id of its own
    /// (`aeron_send_channel_endpoint.c:738-748`).
    ResponseSetup {
        /// The registration id of the subscription the publication was made
        /// for.
        response_correlation_id: i64,
        /// The session that subscription should now read.
        response_session_id: i32,
    },
    /// A publication's receiver refused the stream: an `ERR` frame named this
    /// publication, and its client has to be told
    /// (`aeron_driver_conductor_proxy_on_publication_error`,
    /// `aeron_driver_conductor_proxy.c:207-242`).
    ///
    /// The reference's sender reaches the conductor's handler directly through
    /// the proxy. Here the words travel as an event and the conductor sends the
    /// response, because every client message in this build is written by the
    /// one thread that holds the to-clients ring.
    PublicationError {
        /// The response's fields and its message.
        error: OwnedPublicationError,
    },
    /// A publication heard from a live receiver for the first time
    /// (`aeron_driver_conductor_proxy_on_response_connected`,
    /// `aeron_driver_conductor_proxy.c:160-170`).
    ///
    /// The handshake a response channel runs is one-way until this: the image
    /// keeps saying the response session on every status-message period, and
    /// this is how it is told to stop. What is reported is the publication's
    /// own `response-correlation-id`, which for the publication that asked for
    /// a response channel is the registration id of the image that owes it.
    ResponseConnected {
        /// The registration id the publication names, whether or not it names
        /// an image on this driver.
        response_correlation_id: i64,
    },
    /// A publication's readers moved through the tether cycle
    /// (`aeron_network_publication_check_untethered_subscriptions`,
    /// `aeron_network_publication.c:1120-1236`).
    ///
    /// The machine runs on the sender — what it moves is the publication's own
    /// set of readers — and what it produces is three *client* messages, which
    /// only the conductor can send. So the outcome travels here, in the order
    /// the readers are held.
    Untethered {
        /// Which publication.
        registration_id: i64,
        /// What moved.
        events: Vec<UntetheredEvent>,
    },
    /// Something for the conductor to record: a socket that refused a send, a
    /// frame that could not be believed.
    Fault {
        /// The protocol error code to record it under.
        error_code: i32,
        /// The words.
        description: String,
    },
}

/// The conductor's end of the conversation.
pub struct SenderProxy {
    commands: Channel<SenderCommand>,
    events: Receiver<SenderEvent>,
}

impl SenderProxy {
    /// A proxy whose thread is **not there**: everything a caller hands it is
    /// dropped, and every method answers as if the sender had stopped.
    ///
    /// It exists for the parts of the driver the conductor exercises on their
    /// own — a client being reaped, a subscription being removed — where what
    /// is under test is the conductor's own bookkeeping and not what the
    /// sender does with it. Those callers already ignore the failure, because
    /// a sender that has stopped is not a reason to leak a counter.
    #[cfg(test)]
    pub(crate) fn disconnected() -> Self {
        let (commands, _) = mpsc::channel();

        Self {
            commands,
            events: mpsc::channel().1,
        }
    }

    /// Ask the sender to take an endpoint.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_endpoint(&self, id: u64, endpoint: Box<SendChannelEndpoint>) -> io::Result<()> {
        self.commands
            .send(SenderCommand::AddEndpoint { id, endpoint })
            .map_err(|_| stopped())
    }

    /// Ask the sender to take a publication.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_publication(&self, publication: Box<NetworkPublication>) -> io::Result<()> {
        self.commands
            .send(SenderCommand::AddPublication { publication })
            .map_err(|_| stopped())
    }

    /// Ask the sender to stop sending a publication.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_publication(&self, registration_id: i64) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemovePublication { registration_id })
            .map_err(|_| stopped())
    }

    /// Tell the sender to revoke a publication
    /// (`AERON_DRIVER_MANAGED_RESOURCE_REVOKE`).
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn revoke_publication(&self, registration_id: i64) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RevokePublication { registration_id })
            .map_err(|_| stopped())
    }

    /// Ask the sender to close an endpoint.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_endpoint(&self, id: u64) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemoveEndpoint { id })
            .map_err(|_| stopped())
    }

    /// Tell the sender that a publication's last client has let go.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn end_publication(&self, registration_id: i64) -> io::Result<()> {
        self.commands
            .send(SenderCommand::EndPublication { registration_id })
            .map_err(|_| stopped())
    }

    /// Tell the sender that a name resolved somewhere else.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn resolution_change(
        &self,
        endpoint_id: u64,
        endpoint_name: String,
        new_addr: SocketAddr,
    ) -> io::Result<()> {
        self.commands
            .send(SenderCommand::ResolutionChange {
                endpoint_id,
                endpoint_name,
                new_addr,
            })
            .map_err(|_| stopped())
    }

    /// Ask the sender to put a destination on an endpoint's tracker.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn add_destination(
        &self,
        endpoint_id: u64,
        channel: Box<UdpChannel>,
        address: Option<SocketAddr>,
        registration_id: i64,
    ) -> io::Result<()> {
        self.commands
            .send(SenderCommand::AddDestination {
                endpoint_id,
                channel,
                address,
                registration_id,
            })
            .map_err(|_| stopped())
    }

    /// Ask the sender to take a destination off an endpoint's tracker.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_destination(&self, endpoint_id: u64, address: SocketAddr) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemoveDestination {
                endpoint_id,
                address,
            })
            .map_err(|_| stopped())
    }

    /// The same, by the id the client was given.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_destination_by_id(
        &self,
        endpoint_id: u64,
        registration_id: i64,
    ) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemoveDestinationById {
                endpoint_id,
                registration_id,
            })
            .map_err(|_| stopped())
    }

    /// Ask the sender to make a publication count a local reader.
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
            .send(SenderCommand::AddSubscriber {
                registration_id,
                position,
            })
            .map_err(|_| stopped())
    }

    /// Ask the sender to stop counting one.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn remove_subscriber(&self, registration_id: i64, counter_id: i32) -> io::Result<()> {
        self.commands
            .send(SenderCommand::RemoveSubscriber {
                registration_id,
                counter_id,
            })
            .map_err(|_| stopped())
    }

    /// Take everything the sender has said since the last call.
    pub fn poll(&self) -> Vec<SenderEvent> {
        self.events.try_iter().collect()
    }

    /// Ask the thread to stop.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the thread is gone.
    pub fn stop(&self) -> io::Result<()> {
        self.commands
            .send(SenderCommand::Stop)
            .map_err(|_| stopped())
    }
}

/// The sender's two halves, before either has a thread.
pub(crate) struct SenderParts {
    /// The conductor's end: commands in, events out.
    pub proxy: SenderProxy,
    /// The work, which a runner drives one pass at a time.
    pub agent: SenderThread,
}

/// The thread itself, with its proxy.
pub struct Sender {
    proxy: SenderProxy,
    thread: Option<JoinHandle<()>>,
}

/// One pass of the sender, for the runner that drives it.
impl crate::driver::Agent for SenderThread {
    fn do_work(&mut self) -> Option<usize> {
        Self::do_work(self)
    }
}

impl Sender {
    /// Start the sender thread.
    ///
    /// `values_length` is the counters region's length, which is what fixes a
    /// counter manager's id space; the agent's own manager is built from it so
    /// that a counter id means the same thing on both sides.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the thread cannot be spawned.
    #[allow(clippy::too_many_arguments)] // one per setting the agent is built with
    pub fn start(
        cnc: Arc<CncFile>,
        values_length: usize,
        free_to_reuse_timeout_ms: i64,
        mtu_length: usize,
        cycle_threshold_ns: i64,
        linger_timeout_ns: i64,
        re_resolution_interval_ns: i64,
        status_message_timeout_ns: i64,
        send_to_sm_poll_ratio: u8,
    ) -> io::Result<Self> {
        let SenderParts { proxy, agent } = Self::split(
            cnc,
            values_length,
            free_to_reuse_timeout_ms,
            mtu_length,
            cycle_threshold_ns,
            linger_timeout_ns,
            re_resolution_interval_ns,
            status_message_timeout_ns,
            send_to_sm_poll_ratio,
        )?;

        let thread = crate::driver::run_agent(
            Role::Sender.classic_name(),
            agent,
            crate::driver::default_strategy(),
        )?;

        Ok(Self {
            proxy,
            thread: Some(thread),
        })
    }

    /// The two halves this is made of, **without** a thread: the end the
    /// conductor holds and the work a runner drives.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the counters region is too small for a sender's own
    /// view of it.
    #[allow(clippy::too_many_arguments)] // one per setting the agent is built with
    pub(crate) fn split(
        cnc: Arc<CncFile>,
        values_length: usize,
        free_to_reuse_timeout_ms: i64,
        mtu_length: usize,
        cycle_threshold_ns: i64,
        linger_timeout_ns: i64,
        re_resolution_interval_ns: i64,
        status_message_timeout_ns: i64,
        send_to_sm_poll_ratio: u8,
    ) -> io::Result<SenderParts> {
        let (command_tx, command_rx) = mpsc::channel::<SenderCommand>();
        let (event_tx, event_rx) = mpsc::channel::<SenderEvent>();

        let Some(counters) = CounterManager::new(values_length, free_to_reuse_timeout_ms) else {
            return Err(io::Error::other(
                "the counters region is too small for the sender's view of it",
            ));
        };

        Ok(SenderParts {
            proxy: SenderProxy {
                commands: command_tx,
                events: event_rx,
            },
            agent: SenderThread::new(
                cnc,
                counters,
                mtu_length,
                cycle_threshold_ns,
                linger_timeout_ns,
                re_resolution_interval_ns,
                status_message_timeout_ns,
                send_to_sm_poll_ratio,
                deepmsg_core::clock::monotonic_nano_time(),
                event_tx,
                command_rx,
            ),
        })
    }

    /// The conductor's end.
    pub const fn proxy(&self) -> &SenderProxy {
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
                .map_err(|_| io::Error::other("the sender thread panicked"))?;
        }

        Ok(())
    }
}

/// An error for "the thread is not there", which every proxy method reports.
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "the sender thread has stopped")
}

/// What the thread owns.
pub(crate) struct SenderThread {
    cnc: Arc<CncFile>,
    counters: CounterManager,
    cycle_threshold_ns: i64,
    /// How long a publication that has said everything it has to say sticks
    /// around before it is released (`aeron.publication.linger.timeout`).
    linger_timeout_ns: i64,
    events: Channel<SenderEvent>,
    endpoints: Vec<(u64, Box<SendChannelEndpoint>)>,
    /// Answers to re-resolutions, waiting for a pass that has the counter
    /// regions — the same hand-off the destination commands make, and for the
    /// same reason: `Resolution changes` is a counter.
    pending_resolutions: Vec<(u64, String, SocketAddr)>,
    /// How often to look, and when to look next
    /// (`aeron_driver_sender.c:183-188`). Zero is a driver that does not look
    /// at all.
    re_resolution_interval_ns: i64,
    re_resolution_deadline_ns: i64,
    publications: Vec<NetworkPublication>,
    /// One buffer per receive slot, allocated once.
    buffers: Vec<Vec<u8>>,
    datagrams: Datagrams,
    /// Destination changes waiting for a pass that has the counters.
    ///
    /// The command loop has none — they arrive with [`SenderThread::do_work`] —
    /// and a tracker writes `mdc-num-dest` whenever its table changes. The
    /// reference applies these at the top of its send pass for the same reason
    /// (`aeron_driver_sender.c:196-200`).
    pending_destinations: Vec<SenderCommand>,
    /// Reader changes waiting for a pass that has the counters, for the same
    /// reason and with the same shape as the destinations above: what a reader
    /// joins is a publication's position set, and computing anything from it
    /// needs the counter regions.
    pending_subscribers: Vec<SenderCommand>,
    last_cycle_ns: i64,
    /// When this sender reads its control sockets, and how often.
    duty_cycle: DutyCycle,
    /// The commands the conductor sends, which this drains one pass at a time.
    commands: Receiver<SenderCommand>,
}

impl SenderThread {
    #[allow(clippy::too_many_arguments)] // one per setting the agent is built with
    fn new(
        cnc: Arc<CncFile>,
        counters: CounterManager,
        mtu_length: usize,
        cycle_threshold_ns: i64,
        linger_timeout_ns: i64,
        re_resolution_interval_ns: i64,
        status_message_timeout_ns: i64,
        send_to_sm_poll_ratio: u8,
        now_ns: i64,
        events: Channel<SenderEvent>,
        commands: Receiver<SenderCommand>,
    ) -> Self {
        Self {
            commands,
            cnc,
            counters,
            cycle_threshold_ns,
            linger_timeout_ns,
            re_resolution_interval_ns,
            re_resolution_deadline_ns: now_ns + re_resolution_interval_ns,
            duty_cycle: DutyCycle::new(send_to_sm_poll_ratio, status_message_timeout_ns),
            events,
            endpoints: Vec::new(),
            pending_resolutions: Vec::new(),
            publications: Vec::new(),
            buffers: (0..RECEIVE_SLOTS).map(|_| vec![0u8; mtu_length]).collect(),
            datagrams: Datagrams::new(),
            pending_destinations: Vec::new(),
            pending_subscribers: Vec::new(),
            last_cycle_ns: deepmsg_core::clock::monotonic_nano_time(),
        }
    }

    /// One pass: the commands that arrived, then the send
    /// (`aeron_driver_sender_do_work`, `aeron_driver_sender.c:133-180`, which
    /// drains the proxy queue and then calls `_do_send`).
    ///
    /// `None` when a `Stop` was among them — the reference's `running` flag,
    /// cleared and checked after the pass, which is why the pass that stops
    /// sends nothing. `Some(work)` is what the idle strategy is given.
    ///
    /// This is the reference's agent loop body (`aeron_agent.c:395-412`)
    /// separated from the loop, so that a `SHARED` runner can drive it on a
    /// thread the receiver and the agent are on too.
    pub fn do_work(&mut self) -> Option<usize> {
        {
            let mut stop = false;

            for command in self.commands.try_iter() {
                match command {
                    SenderCommand::AddEndpoint { id, endpoint } => {
                        self.endpoints.push((id, endpoint));
                    }
                    SenderCommand::AddPublication { publication } => {
                        self.publications.push(*publication);
                    }
                    SenderCommand::RevokePublication { registration_id } => {
                        if let Some(publication) = self
                            .publications
                            .iter()
                            .find(|publication| publication.registration_id == registration_id)
                        {
                            publication.set_revoked();
                        }
                    }
                    SenderCommand::EndPublication { registration_id } => {
                        if let Some(publication) = self
                            .publications
                            .iter_mut()
                            .find(|publication| publication.registration_id == registration_id)
                        {
                            publication.request_end();
                        }
                    }
                    SenderCommand::RemovePublication { registration_id } => {
                        self.publications
                            .retain(|publication| publication.registration_id != registration_id);
                        let _ = self
                            .events
                            .send(SenderEvent::PublicationRemoved { registration_id });
                    }
                    SenderCommand::RemoveEndpoint { id } => {
                        self.endpoints.retain(|(endpoint_id, _)| *endpoint_id != id);
                        let _ = self.events.send(SenderEvent::EndpointRemoved { id });
                    }
                    SenderCommand::ResolutionChange {
                        endpoint_id,
                        endpoint_name,
                        new_addr,
                    } => {
                        self.pending_resolutions
                            .push((endpoint_id, endpoint_name, new_addr));
                    }
                    command @ (SenderCommand::AddDestination { .. }
                    | SenderCommand::RemoveDestination { .. }
                    | SenderCommand::RemoveDestinationById { .. }) => {
                        self.pending_destinations.push(command);
                    }
                    command @ (SenderCommand::AddSubscriber { .. }
                    | SenderCommand::RemoveSubscriber { .. }) => {
                        self.pending_subscribers.push(command);
                    }
                    SenderCommand::Stop => stop = true,
                }
            }

            if stop {
                return None;
            }
        }

        Some(self.do_send())
    }

    /// Send each publication's share, and then — when the duty cycle says it is
    /// due — read the control sockets
    /// (`aeron_driver_sender_do_work`,
    /// `aeron-driver/src/main/c/aeron_driver_sender.c:132-190`: the send is
    /// `aeron_driver_sender_do_send` at `:416-458`, and the decision below is
    /// `:149-152`).
    ///
    /// The order is the reference's and it is load-bearing: what a status
    /// message says about a publication's window takes effect on the **next**
    /// pass, because the send that this pass was going to do has already
    /// happened.
    ///
    /// The body is written against the fields rather than through methods so
    /// that the counter view's borrow and the mutable borrows of the two lists
    /// are visibly disjoint — which is what the borrow checker asks for, and
    /// what makes it obvious that this pass touches nothing else.
    fn do_send(&mut self) -> usize {
        // The Arc is cloned rather than borrowed so that the regions' borrow is
        // of this local: a region view holds the mapping, and holding `self`
        // borrowed for its lifetime would forbid every mutation below.
        let cnc = Arc::clone(&self.cnc);
        let Some(regions) = cnc.counter_regions() else {
            return 0;
        };
        let now_ns = deepmsg_core::clock::monotonic_nano_time();
        Self::apply_destinations(
            &mut self.endpoints,
            &mut self.pending_destinations,
            &self.counters,
            &regions,
            now_ns,
        );
        Self::apply_subscribers(
            &mut self.publications,
            &mut self.pending_subscribers,
            &self.counters,
            &regions,
        );
        self.check_untethered_subscriptions(&regions, now_ns);

        let system = System::new(&self.counters, &regions);

        // The answers to re-resolutions first, so that a name that just moved
        // is the address this pass sends to.
        let mut resolved = Self::apply_resolution_changes(
            &mut self.endpoints,
            &mut self.pending_resolutions,
            &system,
        );

        // And the question, on its own deadline (`aeron_driver_sender.c:183-188`):
        // a zero interval is a driver that never asks.
        if self.re_resolution_interval_ns > 0 && now_ns > self.re_resolution_deadline_ns {
            self.re_resolution_deadline_ns = now_ns + self.re_resolution_interval_ns;
            resolved += Self::check_for_re_resolution(&mut self.endpoints, &self.events, now_ns);
        }

        // Read before the send, because a short send this pass is one of the
        // four things that asks for the poll below (`:145-155`).
        let short_sends_before = system.value(system_counters::id::SHORT_SENDS);

        let mut work = Self::send_publications(
            &mut self.endpoints,
            &mut self.publications,
            &system,
            &self.counters,
            &regions,
            self.linger_timeout_ns,
            &self.events,
        );

        let short_sends_after = system.value(system_counters::id::SHORT_SENDS);

        // The decision needs the two counter readings and nothing else, so the
        // view over them can go out of scope before it — which is what lets it
        // take the sender's own duty-cycle state mutably. It is `Copy`; the
        // poll below makes another.
        if self
            .duty_cycle
            .is_due(work, short_sends_before, short_sends_after, now_ns)
        {
            let system = System::new(&self.counters, &regions);

            work += Self::receive_control_frames(
                &mut self.endpoints,
                &mut self.publications,
                &mut self.buffers,
                &mut self.datagrams,
                &system,
                &self.counters,
                &regions,
                &self.events,
            );

            self.duty_cycle.polled(now_ns);
        }

        Self::track_cycle(
            &self.counters,
            &regions,
            self.cycle_threshold_ns,
            &mut self.last_cycle_ns,
        );

        work + resolved
    }

    /// Apply the destination changes the conductor asked for
    /// (`aeron_driver_sender_do_send`, `:196-200`).
    ///
    /// An endpoint that has gone away, or one whose channel is not
    /// multi-destination, is skipped: the client has already been answered, and
    /// a destination is not a thing to fail a pass over.
    fn apply_destinations(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        pending: &mut Vec<SenderCommand>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) {
        for command in std::mem::take(pending) {
            let Some(endpoint_id) = command.destination_endpoint_id() else {
                continue;
            };

            let Some((_, endpoint)) = endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
            else {
                continue;
            };

            let Some(tracker) = endpoint.destination_tracker_mut() else {
                continue;
            };

            match command {
                SenderCommand::AddDestination {
                    channel,
                    address,
                    registration_id,
                    ..
                } => {
                    tracker.manual_add(
                        counters,
                        regions,
                        now_ns,
                        *channel,
                        address,
                        registration_id,
                    );
                }
                SenderCommand::RemoveDestination { address, .. } => {
                    tracker.remove(counters, regions, &address);
                }
                SenderCommand::RemoveDestinationById {
                    registration_id, ..
                } => {
                    tracker.remove_by_id(counters, regions, registration_id);
                }
                _ => {}
            }
        }
    }

    /// Apply the reader changes the conductor asked for.
    ///
    /// A publication that is not there is skipped, and so is a reader whose
    /// publication never existed: the conductor has already answered the
    /// client and holds the link itself, so what is lost is a limit computed
    /// without a reader that is on its way out anyway.
    ///
    /// The connected status is rewritten here rather than in the hook, and
    /// only when `ssc` is set — which is what the reference's two hooks do
    /// (`aeron_network_publication.c:1353-1378`) and the only thing about a
    /// spy that a *client* can see.
    fn apply_subscribers(
        publications: &mut [NetworkPublication],
        pending: &mut Vec<SenderCommand>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) {
        for command in std::mem::take(pending) {
            match command {
                SenderCommand::AddSubscriber {
                    registration_id,
                    position,
                } => {
                    let Some(publication) = publications
                        .iter_mut()
                        .find(|publication| publication.registration_id == registration_id)
                    else {
                        continue;
                    };

                    // Both hooks write `true` — the add one literally, and the
                    // remove one's `has_subscribers` is a constant where it
                    // runs (see [`NetworkPublication::remove_spy`]). What
                    // settles the status afterwards is the next pass of
                    // `update_pub_pos_and_lmt`, the same pass that would settle
                    // it in the reference.
                    if publication.add_spy(position) {
                        publication.update_connected_status(counters, regions, true);
                    }
                }
                SenderCommand::RemoveSubscriber {
                    registration_id,
                    counter_id,
                } => {
                    let Some(publication) = publications
                        .iter_mut()
                        .find(|publication| publication.registration_id == registration_id)
                    else {
                        continue;
                    };

                    if publication.remove_spy(counter_id) {
                        publication.update_connected_status(counters, regions, true);
                    }
                }
                _ => {}
            }
        }
    }

    /// Run every publication's tether cycle and hand what moved to the
    /// conductor (`aeron_network_publication_check_untethered_subscriptions`,
    /// `aeron_network_publication.c:1120-1236`).
    ///
    /// It is done here rather than inside a publication's `send` because it
    /// needs the counter manager mutably — a closed reader's counter is
    /// written back as `NULL` in the set — and the send pass holds it shared.
    ///
    /// The reference runs the same machine from the conductor, on its timer
    /// tier (`:1277`). See
    /// [`NetworkPublication::check_untethered_subscriptions`] for why it
    /// cannot be done that way here and what the difference amounts to.
    fn check_untethered_subscriptions(&mut self, regions: &CounterRegions<'_>, now_ns: i64) {
        let counters = &mut self.counters;
        let channel = &self.events;

        for publication in &mut self.publications {
            let events = publication.check_untethered_subscriptions(counters, regions, now_ns);

            if !events.is_empty() {
                let _ = channel.send(SenderEvent::Untethered {
                    registration_id: publication.registration_id,
                    events,
                });
            }
        }
    }

    /// Ask about the names of the endpoints that have gone quiet
    /// (`aeron_udp_transport_poller_check_send_endpoint_re_resolutions`,
    /// `media/aeron_udp_transport_poller.c:295-310`, which the sender calls on
    /// its own deadline, `aeron_driver_sender.c:183-188`).
    ///
    /// One **event** per endpoint that needs it, because the resolution is the
    /// conductor's: it is the one holding the resolver, and a name resolved on
    /// this thread would be a name resolved twice if there were two senders.
    /// The endpoint is not touched here — the answer comes back as a command
    /// and is applied by [`Self::apply_resolution_changes`].
    fn check_for_re_resolution(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        events: &Channel<SenderEvent>,
        now_ns: i64,
    ) -> usize {
        let mut work = 0;

        for (id, endpoint) in endpoints.iter_mut() {
            // The endpoint's own address, for the channel that has one
            // (`:763-773`).
            if endpoint.needs_re_resolution(now_ns) {
                if let Some(name) = endpoint.endpoint_name() {
                    if events
                        .send(SenderEvent::ReResolveEndpoint {
                            endpoint_id: *id,
                            endpoint_name: name.to_owned(),
                            address: Some(endpoint.remote_data_addr()),
                        })
                        .is_ok()
                    {
                        work += 1;
                    }
                }

                continue;
            }

            // And its **destinations**, for the manual channel that has those
            // instead (`:757-761`, which hands the check to the tracker —
            // `aeron_udp_destination_tracker.c:402-428`). Each one is asked
            // about once per timeout, because asking stamps it.
            let Some(tracker) = endpoint.destination_tracker_mut() else {
                continue;
            };

            let due = tracker.destinations_to_re_resolve(now_ns, DESTINATION_TIMEOUT_NS);

            for (index, name) in due {
                let address = tracker.destination_addr(index);
                tracker.mark_re_resolution_checked(index, now_ns);

                if events
                    .send(SenderEvent::ReResolveEndpoint {
                        endpoint_id: *id,
                        endpoint_name: name,
                        address,
                    })
                    .is_ok()
                {
                    work += 1;
                }
            }
        }

        work
    }

    /// Apply the answers the conductor sent back
    /// (`aeron_driver_sender_on_resolution_change`,
    /// `aeron-driver/src/main/c/aeron_driver_sender.c:400-415`).
    ///
    /// The endpoint may be gone — a client can close the publication while a
    /// name is being resolved — and a command for an endpoint that is not there
    /// is **not** counted: the reference's handler is handed the endpoint
    /// pointer and would crash on one it had lost, which this build cannot do
    /// and does not need to (the lookup is by id, and a missing id is a no-op).
    ///
    /// `Resolution changes` is counted **here**, on the thread that made the
    /// change, which is the reference's own arrangement: the counter is the
    /// sender's (`sender->resolution_changes_counter`,
    /// `aeron_driver_sender.c:110-111`), and a receiver counts its own.
    fn apply_resolution_changes(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        pending: &mut Vec<(u64, String, SocketAddr)>,
        system: &System<'_>,
    ) -> usize {
        let mut work = 0;

        for (endpoint_id, endpoint_name, new_addr) in pending.drain(..) {
            let Some((_, endpoint)) = endpoints.iter_mut().find(|(id, _)| *id == endpoint_id)
            else {
                continue;
            };

            if endpoint
                .on_resolution_change(&endpoint_name, new_addr)
                .is_err()
            {
                // A transport that will not reconnect is the reference's own
                // failure here (`:408-414`, which logs and counts nothing).
                continue;
            }

            system.increment(system_counters::id::RESOLUTION_CHANGES);
            work += 1;
        }

        work
    }

    /// Read everything the endpoints' sockets hold and hand each frame to the
    /// publication it names (`aeron_send_channel_endpoint_dispatch`,
    /// `media/aeron_send_channel_endpoint.c:463-516`).
    #[allow(clippy::too_many_arguments)] // the fields of one pass, made explicit
    fn receive_control_frames(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        publications: &mut [NetworkPublication],
        buffers: &mut [Vec<u8>],
        datagrams: &mut Datagrams,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        events: &Channel<SenderEvent>,
    ) -> usize {
        let mut work = 0;

        for index in 0..endpoints.len() {
            // The borrow of this endpoint ends before the datagrams are
            // dispatched, because a dispatch may need to *send* through any of
            // them (a resend answers a NAK on the endpoint it arrived at).
            let received = endpoints[index]
                .1
                .transport_mut()
                .receive(buffers, datagrams);

            let received = match received {
                Ok(received) => received,
                Err(error) => {
                    let _ = events.send(SenderEvent::Fault {
                        error_code: deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                        description: format!("could not receive on a send endpoint: {error}"),
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

                Self::dispatch(
                    counters,
                    regions,
                    publications,
                    endpoints,
                    index,
                    datagram.source,
                    &buffers[slot][..datagram.length],
                    events,
                );
            }

            system.add(system_counters::id::BYTES_RECEIVED, bytes_received);
        }

        work
    }

    /// One frame, to the publication that names it
    /// (`aeron_send_channel_endpoint_dispatch`, `:463-516`).
    ///
    /// An associated function rather than a method so that the borrow of the
    /// datagram buffers and the borrow of the publication list are visibly
    /// disjoint.
    #[allow(clippy::too_many_arguments)] // the frame, and where it arrived
    fn dispatch(
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut [NetworkPublication],
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        endpoint_index: usize,
        source: Option<SocketAddr>,
        bytes: &[u8],
        events: &Channel<SenderEvent>,
    ) {
        let system = System::new(counters, regions);

        let Some(header) = FrameHeader::read(bytes) else {
            return;
        };

        if !is_frame_valid(&header, bytes.len()) {
            system.increment(system_counters::id::INVALID_PACKETS);
            return;
        }

        let now_ns = deepmsg_core::clock::monotonic_nano_time();

        match header.frame_type {
            frame_type::SM => {
                let Some(frame) = StatusMessageFrame::read(bytes) else {
                    return;
                };

                // A status message is checked before anything reads a position
                // out of it. What it is measured against is the publication's
                // term length when a publication here answers to it, and the
                // largest a term may be otherwise
                // (`aeron_send_channel_endpoint.c:589-596`, `:613-617`).
                let index = index_of_publication(
                    publications,
                    endpoints[endpoint_index].0,
                    frame.stream_id,
                    frame.session_id,
                );
                let term_length =
                    index.map_or(TERM_MAX_LENGTH, |index| publications[index].term_length);

                if !is_valid_status_message(&frame, term_length) {
                    system.increment(system_counters::id::INVALID_PACKETS);
                    return;
                }

                // Counted whether or not a publication here can read it: the
                // counter answers "is the far end talking to us", which does not
                // depend on our recognising what it said (`:625`).
                system.increment(system_counters::id::STATUS_MESSAGES_RECEIVED);

                let is_send_setup = header.flags & header_flags::SM_SEND_SETUP != 0;

                // A position the publication cannot place is one it must not act
                // on (`aeron_network_publication_is_valid_status_message`,
                // `aeron_network_publication.c:841-856`) — and one the
                // *destinations* must not act on either. The reference refuses
                // it before the tracker ever sees it
                // (`aeron_send_channel_endpoint.c:627-634`), so a message that
                // is not believed cannot keep a destination alive or bring one
                // into being.
                //
                // `SEND_SETUP` is not a position report — it is a receiver
                // saying it has no image and wants the stream described
                // (`:649-656`) — so it is exempt from that check, which is what
                // the reference's guard says too.
                if !is_send_setup {
                    if let Some(index) = index {
                        let snd_pos = counters
                            .value(regions, publications[index].counters.snd_pos)
                            .unwrap_or(0);

                        if !publications[index].is_valid_status_message(&frame, snd_pos) {
                            system.increment(system_counters::id::STATUS_MESSAGES_REJECTED);
                            return;
                        }
                    }
                }

                // What was not refused reaches the endpoint's destinations, and
                // reaches them whether or not a publication here answers to the
                // frame: on a dynamic channel a status message from somewhere
                // unknown *creates* a destination, which is the only way such a
                // channel learns one (`:636-645`).
                if let Some(address) = source {
                    if let Some(tracker) = endpoints[endpoint_index].1.destination_tracker_mut() {
                        tracker.on_status_message(
                            counters,
                            regions,
                            frame.receiver_id,
                            &address,
                            now_ns,
                        );
                    }
                }

                let Some(index) = index else {
                    return;
                };

                if is_send_setup {
                    publications[index].trigger_send_setup_frame(source, frame.group_tag(bytes));
                    return;
                }

                // The endpoint remembers when it last heard from anyone at all,
                // which is what its own re-resolution measures
                // (`aeron_send_channel_endpoint.c:658`, the line `:765` reads).
                endpoints[endpoint_index].1.on_status_message(now_ns);

                if let Some(response_correlation_id) = publications[index].on_status_message(
                    &frame,
                    header.flags,
                    frame.group_tag(bytes),
                    counters,
                    regions,
                    now_ns,
                ) {
                    let _ = events.send(SenderEvent::ResponseConnected {
                        response_correlation_id,
                    });
                }
            }
            frame_type::NAK => {
                let Some(frame) = NakFrame::read(bytes) else {
                    return;
                };

                // A gap report is only checked when a publication answers to it,
                // because the term length it is measured against is that
                // publication's (`:549-552`).
                let Some(index) = index_of_publication(
                    publications,
                    endpoints[endpoint_index].0,
                    frame.stream_id,
                    frame.session_id,
                ) else {
                    return;
                };

                let endpoint_id = publications[index].endpoint_id;
                let Some(position) = endpoints.iter().position(|(id, _)| *id == endpoint_id) else {
                    return;
                };

                if !is_valid_nak(&frame, publications[index].term_length) {
                    system.increment(system_counters::id::INVALID_PACKETS);
                    return;
                }

                system.increment(system_counters::id::NAK_MESSAGES_RECEIVED);

                let (_, endpoint) = &mut endpoints[position];
                let publication = &mut publications[index];

                // The resend happens *inside* `on_nak` when the channel's delay
                // is zero — the same place the reference's callback fires
                // (`aeron_retransmit_handler.c:110-124`).
                let _ = publication.on_nak(&frame, &system, endpoint, counters, regions, now_ns);
            }
            frame_type::ERR => {
                let Some(frame) = ErrorFrame::read(bytes) else {
                    return;
                };

                if !is_valid_error(&frame, header.frame_length) {
                    system.increment(system_counters::id::INVALID_PACKETS);
                    return;
                }

                // Counted before the lookup, like a status message: an error
                // about a publication this endpoint no longer holds is still an
                // error that arrived (`:686`).
                system.increment(system_counters::id::ERROR_FRAMES_RECEIVED);

                let Some(index) = index_of_publication(
                    publications,
                    endpoints[endpoint_index].0,
                    frame.stream_id,
                    frame.session_id,
                ) else {
                    return;
                };

                // The frame is what a reader says when it refuses the stream,
                // and the two things it carries that the client cannot work out
                // are kept: whether this was a receiver the publication was
                // still waiting on — an `ERR` from one already gone is counted
                // and dropped (`:872-875`) — and what it said.
                if !publications[index].on_error(&frame, counters, regions) {
                    return;
                }

                let endpoint_id = publications[index].endpoint_id;

                // The reference asks the endpoint's destination tracker which
                // destination the datagram belongs to, and answers
                // `AERON_NULL_VALUE` when there is no tracker at all
                // (`media/aeron_send_channel_endpoint.c:685-691`).
                let destination_registration_id = source
                    .and_then(|source| {
                        endpoints
                            .iter()
                            .find(|(id, _)| *id == endpoint_id)
                            .and_then(|(_, endpoint)| endpoint.destination_tracker())
                            .map(|tracker| tracker.find_registration_id(frame.receiver_id, &source))
                    })
                    .unwrap_or(NULL_VALUE);

                // A group tag is meaningful only under its flag, and a reader
                // that sees the bit clear must ignore what it finds in the
                // field (`aeron_network_publication.c:880`).
                let group_tag = if 0 != header.flags & header_flags::ERR_HAS_GROUP_TAG {
                    frame.group_tag
                } else {
                    NULL_VALUE
                };

                let _ = events.send(SenderEvent::PublicationError {
                    error: OwnedPublicationError {
                        registration_id: publications[index].registration_id,
                        destination_registration_id,
                        session_id: frame.session_id,
                        stream_id: frame.stream_id,
                        receiver_id: frame.receiver_id,
                        group_tag,
                        source,
                        error_code: frame.error_code,
                        message: frame.text(bytes).unwrap_or_default().to_vec(),
                    },
                });
            }
            frame_type::RTTM => {
                // A measurement request. Answering it is the publication's job,
                // not the flow control strategy's: the strategy that *asks* for
                // measurements is the peer's, and the one here has nothing to
                // do with the answer (`aeron_send_channel_endpoint.c:709-728`).
                if let Some(frame) = RttmFrame::read(bytes) {
                    let Some(index) = index_of_publication(
                        publications,
                        endpoints[endpoint_index].0,
                        frame.stream_id,
                        frame.session_id,
                    ) else {
                        return;
                    };

                    let endpoint_id = publications[index].endpoint_id;
                    let Some(position) = endpoints.iter().position(|(id, _)| *id == endpoint_id)
                    else {
                        return;
                    };

                    let (_, endpoint) = &mut endpoints[position];
                    let publication = &mut publications[index];

                    let _ = publication.on_rttm(
                        &frame,
                        header.flags,
                        endpoint,
                        &system,
                        counters,
                        regions,
                        now_ns,
                    );
                }
            }
            frame_type::RSP_SETUP => {
                // A responder answering a publication that asked for a
                // response channel. The reference does three things and no
                // more (`aeron_send_channel_endpoint.c:730-751`): find the
                // publication the frame names, read *its* correlation id, and
                // report that to the conductor. An unresolvable frame, or one
                // whose publication never asked for a response, is silence —
                // there is no counter and no error, because a publication the
                // far end knows about and this endpoint does not is not a
                // fault, it is a stale frame.
                let Some(frame) = RspSetupFrame::read(bytes) else {
                    return;
                };

                if let Some(publication) =
                    find_publication(publications, frame.stream_id, frame.session_id)
                {
                    let response_correlation_id = publication.response_correlation_id;

                    if response_correlation_id != layout::NULL_VALUE {
                        let _ = events.send(SenderEvent::ResponseSetup {
                            response_correlation_id,
                            response_session_id: frame.response_session_id,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    /// Send every publication's share. The reference rotates one publication
    /// per call of its flywheel; this sends each of them every pass, which is
    /// the same set of datagrams with a different interleaving.
    #[allow(clippy::too_many_arguments)] // the fields of one pass, made explicit
    fn send_publications(
        endpoints: &mut [(u64, Box<SendChannelEndpoint>)],
        publications: &mut [NetworkPublication],
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        linger_timeout_ns: i64,
        events: &Channel<SenderEvent>,
    ) -> usize {
        let now_ns = deepmsg_core::clock::monotonic_nano_time();
        // Bytes, not publications: the reference's `do_send` returns
        // `bytes_sent` and its duty cycle keys on that (`:150`, `:455`), so a
        // pass that walked ten publications and sent nothing is a pass that
        // sent nothing.
        let mut bytes_sent = 0;

        for publication in publications.iter_mut() {
            let endpoint_id = publication.endpoint_id;

            let Some(position) = endpoints.iter().position(|(id, _)| *id == endpoint_id) else {
                continue;
            };

            let endpoint = &mut endpoints[position].1;
            let registration_id = publication.registration_id;

            // A revoked publication has one last thing to do — say so — and
            // this is where it is noticed, on the thread that owns it
            // (`aeron_network_publication_check_managed_resources`'s ACTIVE and
            // LINGER arms, `:1240-1340`).
            if publication.notice_revoke(now_ns, linger_timeout_ns, system, counters, regions) {
                let _ = events.send(SenderEvent::PublicationDrained { registration_id });
            }

            match publication.send(endpoint, system, counters, regions, now_ns) {
                Ok(bytes) => {
                    if bytes > 0 {
                        system.add(
                            system_counters::id::BYTES_SENT,
                            i64::try_from(bytes).unwrap_or(i64::MAX),
                        );
                    }

                    bytes_sent += bytes;
                }
                Err(error) => {
                    let _ = events.send(SenderEvent::Fault {
                        error_code: deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                        description: format!(
                            "could not send on the channel of publication {registration_id}: {error}"
                        ),
                    });
                }
            }
        }

        bytes_sent
    }

    /// Measure the pass that just ended, and count it if it ran long — the
    /// same duty-cycle tracker the conductor keeps, on the sender's two
    /// counters (`aeron-driver/src/main/c/aeron_driver_sender.c:262-276`).
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
            system_counters::id::SENDER_MAX_CYCLE_TIME,
            cycle_ns,
        );

        if cycle_ns > cycle_threshold_ns {
            system_counters::increment(
                counters,
                regions,
                system_counters::id::SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            );
        }
    }
}

/// When the sender reads its control sockets, and how often — the reference's
/// duty cycle for the poll (`aeron_driver_sender.c:94-98` sets it up, `:149-152`
/// is the decision, `:179-180` is the poll).
///
/// A sender that read its control sockets on every pass would spend a syscall
/// per pass on a stream that is usually sending: the sockets carry status
/// messages, NAKs and errors, and one read in every `ratio` passes that
/// **sent** something is the reference's answer. Three things cut the wait
/// short, and all four are in [`DutyCycle::is_due`].
#[derive(Debug)]
struct DutyCycle {
    /// `aeron.send.to.status.poll.ratio`, six by default.
    ratio: u8,
    /// Passes that sent something since the last read. It does **not** advance
    /// on a pass that sent nothing, because such a pass reads anyway.
    counter: u8,
    /// The longest the sender may go without reading at all: half the
    /// status-message timeout (`:97`, whose `/ 2` is the reference's own).
    read_timeout_ns: i64,
    /// When the next read is due by time. Zero at startup, so the first pass
    /// reads.
    deadline_ns: i64,
}

impl DutyCycle {
    /// The cycle a sender configured with these two settings runs.
    fn new(ratio: u8, status_message_timeout_ns: i64) -> Self {
        Self {
            ratio,
            counter: 0,
            read_timeout_ns: status_message_timeout_ns / 2,
            deadline_ns: 0,
        }
    }

    /// Whether this pass is one that reads the control sockets.
    ///
    /// `bytes_sent` is what the send just did, and the two short-send readings
    /// bracket it — a short send is one of the four things that asks for the
    /// read, and it can only be seen by comparing before with after.
    ///
    /// The reference's `||` short-circuits, and that is load-bearing: the
    /// counter only advances where the first condition was false, which is what
    /// keeps a stream of quiet passes from counting towards the ratio.
    fn is_due(
        &mut self,
        bytes_sent: usize,
        short_sends_before: i64,
        short_sends_after: i64,
        now_ns: i64,
    ) -> bool {
        if 0 == bytes_sent {
            return true;
        }

        // `is_due` returns at the comparison below at the latest, and the
        // largest ratio the configuration accepts is `u8::MAX`, so this cannot
        // wrap.
        self.counter += 1;

        if self.counter >= self.ratio {
            return true;
        }

        if now_ns > self.deadline_ns {
            return true;
        }

        short_sends_before < short_sends_after
    }

    /// The read happened: the passes start over and the deadline moves out.
    fn polled(&mut self, now_ns: i64) {
        self.counter = 0;
        self.deadline_ns = now_ns + self.read_timeout_ns;
    }
}

/// The publication a control frame names, as an index — for a caller that
/// needs another field beside it and so cannot hold the borrow.
///
/// The lookup is the **arriving endpoint's**, which is what the reference's is:
/// every control frame is answered out of `endpoint->publication_dispatch_map`,
/// keyed by stream and session, on the endpoint the datagram came in on
/// (`media/aeron_send_channel_endpoint.c:560`, `:616`, `:697`, `:722`, `:740`).
///
/// It has to be, because (stream, session, endpoint) is not a unique key across
/// a driver: a publication that names another's session with
/// `session-id=tag:N` shares both ids and differs only in where it sends from.
/// Search the whole list and the second one never hears anything — its status
/// messages are answered by whichever publication was created first, and its
/// own `pub-lmt` stays at zero for ever.
fn index_of_publication(
    publications: &[NetworkPublication],
    endpoint_id: u64,
    stream_id: i32,
    session_id: i32,
) -> Option<usize> {
    publications.iter().position(|publication| {
        publication.endpoint_id == endpoint_id
            && publication.stream_id == stream_id
            && publication.session_id == session_id
    })
}

/// Whether a gap report names a frame a term could hold
/// (`aeron_send_channel_endpoint_is_valid_nak`,
/// `media/aeron_send_channel_endpoint.c:519-526`).
///
/// A report that names an unaligned offset, or a length that runs past the end
/// of the term, is one the retransmit path would otherwise read a frame out of
/// at an offset no frame starts at.
fn is_valid_nak(frame: &NakFrame, term_length: i32) -> bool {
    let term_buffer_length = i64::from(term_length);
    let term_offset = i64::from(frame.term_offset);

    term_offset >= 0
        && term_offset < term_buffer_length
        && term_offset % i64::from(FRAME_ALIGNMENT as i32) == 0
        && frame.length >= 0
        && term_offset + i64::from(frame.length) <= term_buffer_length
}

/// Whether a status message reports a position and a window a term could hold
/// (`aeron_send_channel_endpoint_is_valid_status_message`, `:589-596`).
fn is_valid_status_message(frame: &StatusMessageFrame, term_length: i32) -> bool {
    let term_buffer_length = i64::from(term_length);
    let term_offset = i64::from(frame.consumption_term_offset);

    term_offset >= 0
        && term_offset < term_buffer_length
        && term_offset % i64::from(FRAME_ALIGNMENT as i32) == 0
        && frame.receiver_window >= 0
        && i64::from(frame.receiver_window) <= (term_buffer_length >> 1)
}

/// Whether an error frame's text fits inside its own frame
/// (`aeron_send_channel_endpoint_is_valid_error`, `:658-662`).
fn is_valid_error(frame: &ErrorFrame, frame_length: i32) -> bool {
    let error_length = i64::from(frame.error_length);

    error_length >= 0
        && error_length <= i64::from(MAX_ERROR_TEXT_LENGTH)
        && error_length + i64::from(ErrorFrame::LENGTH as i32) <= i64::from(frame_length)
}

/// The publication a control frame names
/// (`aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map, aeron_map_compound_key(stream_id, session_id))`).
fn find_publication(
    publications: &mut [NetworkPublication],
    stream_id: i32,
    session_id: i32,
) -> Option<&mut NetworkPublication> {
    publications.iter_mut().find(|publication| {
        publication.stream_id == stream_id && publication.session_id == session_id
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::flowcontrol::FlowControl;
    use crate::media::TransportParams;
    use crate::network_publication::{NetworkPublication, PublicationCounters};
    use crate::publication_params::PublicationParams;
    use crate::retransmit_handler::RetransmitHandler;
    use crate::sys::AddressFamily;
    use crate::sys::socket::DatagramSocket;
    use crate::udp_channel::UdpChannel;
    use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
    use deepmsg_cnc::{CncIdentity, CncLayout};
    use deepmsg_core::buffer::AtomicBuffer;
    use deepmsg_core::logbuffer::descriptor;
    use deepmsg_core::logbuffer::frame::{FLAG_UNFRAGMENTED, Frame, TYPE_DATA};
    use deepmsg_core::logbuffer::logfile::LogFile;
    use deepmsg_core::logbuffer::position::RawTail;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("deepmsg-sender-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("a temp directory");

            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const TERM_LENGTH: i32 = 64 * 1024;

    /// `aeron.send.to.status.poll.ratio` is the passes-between-reads knob, and
    /// the count only advances on a pass that **sent** something
    /// (`aeron_driver_sender.c:149-152`, whose `||` short-circuits).
    #[test]
    fn the_control_read_waits_for_its_share_of_passes() {
        let mut duty = DutyCycle::new(6, 1_000_000);
        duty.polled(1);

        for pass in 1..6 {
            assert!(
                !duty.is_due(64, 0, 0, 1 + pass),
                "pass {pass} of six is not the read"
            );
        }

        assert!(duty.is_due(64, 0, 0, 7), "the sixth pass is");

        // And the count starts over, so the read is every sixth pass and not
        // every sixth *sending* pass.
        duty.polled(7);
        assert!(!duty.is_due(64, 0, 0, 8));
    }

    /// A ratio of one is a read on every sending pass, which is what this build
    /// did before the setting was acted on.
    #[test]
    fn a_ratio_of_one_reads_every_pass() {
        let mut duty = DutyCycle::new(1, 1_000_000);
        duty.polled(0);

        assert!(duty.is_due(64, 0, 0, 1));
        duty.polled(1);
        assert!(duty.is_due(64, 0, 0, 2));
    }

    /// Zero is `++counter >= ratio` being true on the first pass — every pass
    /// reads. The configuration refuses the value (`config.rs`, because the
    /// reference's `uint8_t` cast makes `256` arrive as it), so this is the
    /// arithmetic and not a reachable setting.
    #[test]
    fn a_ratio_of_zero_reads_every_pass_too() {
        let mut duty = DutyCycle::new(0, 1_000_000);
        duty.polled(0);

        assert!(duty.is_due(64, 0, 0, 1));
    }

    /// A pass that sent nothing reads, and does not count towards the ratio:
    /// the reference returns before its `++`, so a stream of quiet passes is a
    /// stream of reads.
    #[test]
    fn a_pass_that_sent_nothing_reads_and_does_not_count() {
        let mut duty = DutyCycle::new(6, 1_000_000);
        duty.polled(0);

        for _ in 0..8 {
            assert!(duty.is_due(0, 0, 0, 1), "a quiet pass reads");
            duty.polled(1);
        }
    }

    /// The deadline is `status_message_timeout_ns / 2`, and it asks for a read
    /// on its own (`aeron_driver_sender.c:97`).
    #[test]
    fn the_read_timeout_asks_for_a_read_by_itself() {
        let mut duty = DutyCycle::new(6, 1_000_000);
        duty.polled(0);
        assert_eq!(500_000, duty.read_timeout_ns, "half of the timeout");

        // One pass, well inside the ratio and well inside the deadline.
        assert!(!duty.is_due(64, 0, 0, 1));

        // And the same pass once the deadline has gone by.
        assert!(duty.is_due(64, 0, 0, 500_001));
    }

    /// A short send asks for a read on the pass it happened, whatever the
    /// counter and the deadline say — the counter is read before the send and
    /// compared after it.
    #[test]
    fn a_short_send_asks_for_a_read() {
        let mut duty = DutyCycle::new(6, 1_000_000);
        duty.polled(0);

        assert!(
            duty.is_due(64, 3, 4, 1),
            "the short-send counter moved during the pass"
        );
        assert!(!duty.is_due(64, 4, 4, 1), "and it did not move here");
    }

    #[test]
    fn a_nak_names_a_frame_a_term_can_hold() {
        const TERM: i32 = 64 * 1024;

        let nak = |term_offset: i32, length: i32| NakFrame {
            session_id: 1,
            stream_id: 2,
            term_id: 3,
            term_offset,
            length,
        };

        assert!(is_valid_nak(&nak(0, 1024), TERM));
        assert!(
            is_valid_nak(&nak(1024, TERM - 1024), TERM),
            "a report that reaches exactly the end of the term is one a term can hold"
        );

        assert!(
            !is_valid_nak(&nak(8, 1024), TERM),
            "an unaligned offset is one no frame starts at"
        );
        assert!(!is_valid_nak(&nak(-32, 1024), TERM));
        assert!(
            !is_valid_nak(&nak(0, TERM + 32), TERM),
            "a length that reaches past the end of the term"
        );
        assert!(!is_valid_nak(&nak(0, -1), TERM));
    }

    #[test]
    fn a_status_message_reports_a_position_and_a_window_a_term_can_hold() {
        const TERM: i32 = 64 * 1024;

        let sm = |consumption_term_offset: i32, receiver_window: i32| StatusMessageFrame {
            session_id: 1,
            stream_id: 2,
            consumption_term_id: 3,
            consumption_term_offset,
            receiver_window,
            receiver_id: 4,
        };

        assert!(is_valid_status_message(&sm(0, TERM / 2), TERM));
        assert!(is_valid_status_message(&sm(1024, 0), TERM));

        assert!(
            !is_valid_status_message(&sm(0, TERM / 2 + 32), TERM),
            "a window wider than half a term asks for more than a term holds"
        );
        assert!(!is_valid_status_message(&sm(0, -1), TERM));
        assert!(!is_valid_status_message(&sm(8, 1024), TERM));
        assert!(!is_valid_status_message(&sm(TERM, 1024), TERM));
    }

    #[test]
    fn an_error_frame_carries_text_that_fits_it() {
        const FRAME: i32 = 64;

        let error = |error_length: i32| ErrorFrame {
            session_id: 1,
            stream_id: 2,
            receiver_id: 3,
            group_tag: 0,
            error_code: 0,
            error_length,
        };

        assert!(is_valid_error(&error(0), FRAME));
        assert!(is_valid_error(&error(MAX_ERROR_TEXT_LENGTH), 1023 + 40));
        assert!(
            is_valid_error(&error(FRAME - 40), FRAME),
            "the text exactly fills the frame"
        );

        assert!(!is_valid_error(&error(-1), FRAME));
        assert!(
            !is_valid_error(&error(MAX_ERROR_TEXT_LENGTH + 1), i32::MAX),
            "more text than an error frame may carry"
        );
        assert!(
            !is_valid_error(&error(FRAME - 40 + 32), FRAME),
            "text that runs past the frame's own length"
        );
    }

    /// A sender thread and a publication producing to a socket this test owns.
    ///
    /// The whole path is exercised: the conductor-side create, the handover
    /// through the command channel, the sender's own poll of its socket, and
    /// the bytes that leave.
    #[test]
    fn a_publication_handed_to_the_sender_is_sent() {
        let dir = TempDir::new();
        let cnc = CncFile::create(
            &dir.0,
            &CncLayout {
                counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
                ..CncLayout::default()
            },
            &CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: deepmsg_core::clock::epoch_millis(),
                pid: i64::from(std::process::id()),
            },
        )
        .expect("a CnC file");

        let cnc = Arc::new(cnc);

        let listener = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        listener
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        listener.set_nonblocking().expect("non-blocking");
        let bound = listener.local_address().expect("a bound address");

        let uri = format!("aeron:udp?endpoint={bound}");
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        let channel = UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel");

        let endpoint = channel.clone();
        let mut metadata = vec![0u8; COUNTERS_VALUES_BUFFER_LENGTH_MIN * 4];
        let mut values = vec![0u8; COUNTERS_VALUES_BUFFER_LENGTH_MIN];
        let endpoint_regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");
        let mut endpoint_manager =
            CounterManager::new(COUNTERS_VALUES_BUFFER_LENGTH_MIN, 1_000).expect("room");

        let endpoint = SendChannelEndpoint::create(
            endpoint,
            &mut crate::port_manager::WildcardPortManager::sender(),
            &TransportParams::default(),
            &mut endpoint_manager,
            &endpoint_regions,
            7,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        // The publication's six counters, allocated on the conductor's side of
        // the handover.
        let (counters, publication) = {
            let regions = cnc.counter_regions().expect("regions");
            let mut manager =
                CounterManager::new(COUNTERS_VALUES_BUFFER_LENGTH_MIN, 1_000).expect("room");

            let allocate = |manager: &mut CounterManager, name: &[u8]| {
                manager
                    .allocate(&regions, 12, &[], name, 1)
                    .expect("a counter")
            };

            let ids = PublicationCounters {
                pub_pos: allocate(&mut manager, b"pub-pos"),
                pub_lmt: allocate(&mut manager, b"pub-lmt"),
                snd_pos: allocate(&mut manager, b"snd-pos"),
                snd_lmt: allocate(&mut manager, b"snd-lmt"),
                snd_bpe: allocate(&mut manager, b"snd-bpe"),
                snd_naks_received: allocate(&mut manager, b"snd-naks"),
                fc_receivers: None,
            };

            // The window is open and the producer has published a frame.
            let _ = manager.set_value(&regions, ids.snd_lmt, 1024);
            let _ = manager.set_value(&regions, ids.pub_lmt, 1024);

            let log = LogFile::create(
                &dir.0.join("publication.logbuffer"),
                TERM_LENGTH,
                4096,
                false,
            )
            .expect("a log buffer");
            let term = log.term(0).expect("a term");
            let frame = Frame::new(&term, 0);
            frame
                .begin(132, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 42, 1001, 1_000)
                .expect("in range");
            frame.write_payload(&[9u8; 100]).expect("in range");
            frame.publish(132).expect("in range");
            log.metadata()
                .expect("metadata")
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(1_000, 160).raw(),
                )
                .expect("in range");

            let params = PublicationParams {
                term_length: TERM_LENGTH,
                term_length_named: false,
                mtu_length: 1408,
                mtu_length_named: false,
                publication_window_length: 32 * 1024,
                max_resend: 0,
                entity_tag: -1,
                response_correlation_id: -1,
                is_response: false,
                session_id: Some(42),
                linger_timeout_ns: 5_000_000_000,
                untethered_window_limit_timeout_ns: 5_000_000_000,
                untethered_linger_timeout_ns: 5_000_000_000,
                untethered_resting_timeout_ns: 10_000_000_000,
                is_sparse: true,
                signal_eos: true,
                spies_simulate_connection: false,
                starting_position: None,
                initial_term_id: 1_000,
            };

            let publication = NetworkPublication::create(
                7,
                9,
                42,
                1001,
                1,
                uri.as_bytes(),
                Box::new(log),
                &params,
                false,
                ids,
                4,
                FlowControl::default(),
                RetransmitHandler::new(1_000, 5_000_000, false, 1),
                4096,
                crate::sys::SocketBufferLengths {
                    rcvbuf: 0,
                    sndbuf: 0,
                },
                0,
                0,
                0,
            )
            .expect("a publication");

            (manager, publication)
        };

        let mut sender = Sender::start(
            Arc::clone(&cnc),
            COUNTERS_VALUES_BUFFER_LENGTH_MIN,
            1_000,
            1408,
            100_000_000,
            crate::config::PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT,
            0,
            crate::config::RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT,
            crate::config::SEND_TO_STATUS_POLL_RATIO_DEFAULT,
        )
        .expect("a sender");

        sender
            .proxy()
            .add_endpoint(1, Box::new(endpoint))
            .expect("an endpoint");
        sender
            .proxy()
            .add_publication(Box::new(publication))
            .expect("a publication");

        // The thread runs on its own schedule, so poll for what it sent. It
        // says SETUP while nothing has answered (`has_initial_connection` is a
        // status message away) and the frame itself is what this waits for.
        let mut buffers = vec![vec![0u8; 2048]];
        let mut datagrams = Datagrams::new();
        let mut setup_frames = 0;
        let mut data_frame: Option<Vec<u8>> = None;

        for _ in 0..200 {
            let received = listener
                .receive_batch(&mut buffers, &mut datagrams)
                .unwrap_or(0);

            for (slot, datagram) in datagrams.as_slice()[..received].iter().enumerate() {
                let bytes = &buffers[slot][..datagram.length];
                let header = FrameHeader::read(bytes).expect("every datagram is a frame");

                match header.frame_type {
                    frame_type::SETUP => setup_frames += 1,
                    frame_type::DATA => data_frame = Some(bytes.to_vec()),
                    other => panic!("a sender sent a {other:#x} frame"),
                }
            }

            if data_frame.is_some() {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        sender.close().expect("a clean stop");

        let data_frame = data_frame.expect("the sender thread put the frame on the wire");
        let header = FrameHeader::read(&data_frame).expect("a header");
        assert_eq!(frame_type::DATA, header.frame_type);
        assert_eq!(132, header.frame_length);
        assert!(data_frame[32..132].iter().all(|byte| *byte == 9));
        assert!(
            setup_frames > 0,
            "and it said SETUP first: nothing had answered it"
        );

        // And the sender position moved on the conductor's own view of the
        // counters, which is the shared-memory contract this test is really
        // about: two threads, one region.
        let regions = cnc.counter_regions().expect("regions");
        assert_eq!(
            Some(160),
            counters.value(&regions, publication_id(&counters, &regions))
        );
    }

    /// The `snd-pos` counter's id, recovered by its label — the test allocated
    /// the ids itself, so a label search is as good as remembering them.
    fn publication_id(counters: &CounterManager, regions: &CounterRegions<'_>) -> i32 {
        let mut found = -1;
        regions.reader().for_each(|descriptor| {
            if descriptor.label == "snd-pos" {
                found = descriptor.counter_id;
            }
        });
        let _ = counters;

        found
    }
}
