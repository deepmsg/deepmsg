//! A receive channel endpoint: one socket, the streams read through it, and
//! the frames that answer what arrives.
//!
//! Mirrors `aeron-driver/src/main/c/media/aeron_receive_channel_endpoint.c`.
//! Its mirror image is [`crate::media::send_endpoint`], and one asymmetry
//! between them is the whole reason both exist: a *send* endpoint binds the
//! interface and connects to the endpoint parameter, while a **receive**
//! endpoint binds the endpoint parameter itself
//! (`aeron_receive_destination.c:47-76`) — the address a subscription names is
//! where it listens.
//!
//! # What it answers with
//!
//! Three frames go back the other way, and none of them comes from here:
//!
//! * a **status message** says how far this endpoint has read and how much room
//!   it has, which is the publisher's whole flow control. One is sent to elicit
//!   a `SETUP` when data arrives for a session with no image
//!   (`elicit_setup_from_source`, `aeron_data_packet_dispatcher.c:616-659`),
//!   and one periodically for each image (`aeron_publication_image.c:862-990`);
//! * a **NAK** says which bytes were missed;
//! * an **RTTM** answers a measurement.
//!
//! The address they go to is the *source* of the data being answered — that is
//! the implicit-unicast control address, and it is why a subscriber can answer
//! a publisher it was never told the address of. A channel that named an
//! explicit `control=` overrides it, and multicast uses the group's control
//! address (`:120-129`).
//!
//! # Reference counts
//!
//! The endpoint lives while anything reads through it: one count per stream and
//! one per (stream, session) subscription, both kept here
//! (`stream_id_to_refcnt_map`, `stream_and_session_id_to_refcnt_map`, `:104-122`)
//! because the endpoint is what a subscription is *linked* to. The image count
//! is separate and lives on the conductor, where images are created.

use std::io;
use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::protocol::{NakFrame, RttmFrame, StatusMessageFrame};
use crate::udp_channel::{ControlMode, UdpChannel};
use crate::{position as counter_position, system_counters};

use super::dispatcher::{DataPacketDispatcher, ImageState, Interest};
use super::{Transport, TransportParams};

/// Where a receive endpoint is in its life
/// (`aeron_receive_channel_endpoint_status_t`,
/// `aeron-driver/src/main/c/media/aeron_receive_channel_endpoint.h:32-38`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointStatus {
    /// Open and reading.
    Active,
    /// Being torn down.
    Closing,
    /// Gone.
    Closed,
}

/// Why a receive endpoint could not be made.
#[derive(Debug)]
pub enum ReceiveEndpointError {
    /// The counter manager had no room for the channel-status counter.
    NoCounter,
    /// The socket could not be opened or bound.
    Socket(io::Error),
}

impl std::fmt::Display for ReceiveEndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCounter => f.write_str("could not allocate the receive channel status counter"),
            Self::Socket(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReceiveEndpointError {}

/// A receive channel endpoint and its socket.
pub struct ReceiveChannelEndpoint {
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// The socket it reads from and answers through.
    transport: Box<dyn Transport>,
    /// The `rcv-channel` counter, whose value is its state.
    channel_status_counter_id: i32,
    /// Which receiver this endpoint is, to a publisher that has several
    /// (`receiver_id`): it travels in every status message, and an `ERR` frame
    /// names it back.
    receiver_id: i64,
    /// Which streams are read here, and which sessions of each.
    dispatcher: DataPacketDispatcher,
    /// How many subscriptions read each stream.
    stream_refcounts: Vec<(i32, i32)>,
    /// How many read each (stream, session).
    session_refcounts: Vec<((i32, i32), i32)>,
    /// `SO_RCVBUF` the channel asked for, zero meaning the driver's default.
    pub socket_rcvbuf: usize,
    /// `SO_SNDBUF`, likewise.
    pub socket_sndbuf: usize,
}

impl ReceiveChannelEndpoint {
    /// Create the endpoint: allocate its channel-status counter and open the
    /// socket bound to the channel's endpoint parameter
    /// (`aeron_receive_channel_endpoint_create`, `:58-170`, with the bind in
    /// `aeron_receive_destination.c:30-139`).
    ///
    /// # Errors
    ///
    /// [`ReceiveEndpointError`] when the counter cannot be allocated or the
    /// socket cannot be bound.
    #[allow(clippy::too_many_arguments)] // one per field the create needs
    pub fn create(
        channel: UdpChannel,
        params: &TransportParams,
        receiver_id: i64,
        stream_session_limit: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
    ) -> Result<Self, ReceiveEndpointError> {
        let channel_status_counter_id = counter_position::allocate_channel_status_counter(
            counters,
            regions,
            counter_position::RECEIVE_CHANNEL_STATUS_NAME,
            counter_position::channel_type_id::RECEIVE_CHANNEL_STATUS,
            registration_id,
            &channel.original_uri,
            now_ms,
        )
        .ok_or(ReceiveEndpointError::NoCounter)?;

        // The receive side binds the *endpoint* — where a subscription
        // listens — and is never connected: it answers whoever writes to it.
        let transport =
            match super::udp_transport::UdpTransport::open(channel.remote_data, None, params) {
                Ok(transport) => transport,
                Err(error) => {
                    counters.free(regions, channel_status_counter_id, now_ms);
                    return Err(ReceiveEndpointError::Socket(error));
                }
            };

        Ok(Self {
            channel,
            transport: Box::new(transport),
            channel_status_counter_id,
            receiver_id,
            dispatcher: DataPacketDispatcher::new(stream_session_limit),
            stream_refcounts: Vec::new(),
            session_refcounts: Vec::new(),
            socket_rcvbuf: params.socket_rcvbuf,
            socket_sndbuf: params.socket_sndbuf,
        })
    }

    /// Wrap an endpoint around a transport a caller built, for the tests that
    /// inject loss or a stub.
    ///
    /// # Errors
    ///
    /// [`ReceiveEndpointError::NoCounter`] when the manager is full.
    #[allow(clippy::too_many_arguments)]
    pub fn with_transport(
        channel: UdpChannel,
        transport: Box<dyn Transport>,
        receiver_id: i64,
        stream_session_limit: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
    ) -> Result<Self, ReceiveEndpointError> {
        let channel_status_counter_id = counter_position::allocate_channel_status_counter(
            counters,
            regions,
            counter_position::RECEIVE_CHANNEL_STATUS_NAME,
            counter_position::channel_type_id::RECEIVE_CHANNEL_STATUS,
            registration_id,
            &channel.original_uri,
            now_ms,
        )
        .ok_or(ReceiveEndpointError::NoCounter)?;

        Ok(Self {
            channel,
            transport,
            channel_status_counter_id,
            receiver_id,
            dispatcher: DataPacketDispatcher::new(stream_session_limit),
            stream_refcounts: Vec::new(),
            session_refcounts: Vec::new(),
            socket_rcvbuf: 0,
            socket_sndbuf: 0,
        })
    }

    /// The `rcv-channel` counter a client reads.
    pub const fn channel_status_counter_id(&self) -> i32 {
        self.channel_status_counter_id
    }

    /// The id this receiver answers to, which every status message carries.
    pub const fn receiver_id(&self) -> i64 {
        self.receiver_id
    }

    /// Write the channel status (`aeron_counter_set_release(endpoint->channel_status.value_addr, ...)`).
    ///
    /// # Errors
    ///
    /// `None` when the counter id is not one this manager knows.
    pub fn set_status(
        &self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        status: EndpointStatus,
    ) -> Option<()> {
        let value = match status {
            EndpointStatus::Active => counter_position::channel_status::ACTIVE,
            EndpointStatus::Closing | EndpointStatus::Closed => {
                counter_position::channel_status::CLOSING
            }
        };

        counters.set_value(regions, self.channel_status_counter_id, value)
    }

    /// Give the endpoint's counter back
    /// (`aeron_receive_channel_endpoint_delete`, `:172-186`).
    pub fn free_counter(
        &self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> bool {
        counters.free(regions, self.channel_status_counter_id, now_ms)
    }

    /// The dispatcher, for the receiver's decisions.
    pub const fn dispatcher(&self) -> &DataPacketDispatcher {
        &self.dispatcher
    }

    /// The dispatcher, mutably.
    pub const fn dispatcher_mut(&mut self) -> &mut DataPacketDispatcher {
        &mut self.dispatcher
    }

    /// The endpoint's counter, for a caller that wants to read it back.
    pub const fn channel_status(&self) -> i32 {
        self.channel_status_counter_id
    }

    /// A subscription to a whole stream arrived
    /// (`aeron_receive_channel_endpoint_on_add_subscription`, `:1000`-ish, and
    /// the refcount the conductor's link keeps here).
    pub fn add_subscription(&mut self, stream_id: i32) {
        self.dispatcher.add_subscription(stream_id);

        match self
            .stream_refcounts
            .iter_mut()
            .find(|(id, _)| *id == stream_id)
        {
            Some((_, count)) => *count += 1,
            None => self.stream_refcounts.push((stream_id, 1)),
        }
    }

    /// A subscription that named a session arrived.
    pub fn add_subscription_by_session(&mut self, stream_id: i32, session_id: i32) {
        self.dispatcher
            .add_subscription_by_session(stream_id, session_id);
        self.add_subscription(stream_id);

        match self
            .session_refcounts
            .iter_mut()
            .find(|((stream, session), _)| *stream == stream_id && *session == session_id)
        {
            Some((_, count)) => *count += 1,
            None => self.session_refcounts.push(((stream_id, session_id), 1)),
        }
    }

    /// A subscription went away. Returns whether the endpoint is now reading
    /// nothing at all, which is when the conductor may release it.
    pub fn remove_subscription(&mut self, stream_id: i32) -> bool {
        self.dispatcher.remove_subscription(stream_id);
        self.decrement(&stream_id, None);

        self.stream_refcounts.is_empty() && self.session_refcounts.is_empty()
    }

    /// A subscription for one session went away.
    pub fn remove_subscription_by_session(&mut self, stream_id: i32, session_id: i32) -> bool {
        self.dispatcher
            .remove_subscription_by_session(stream_id, session_id);
        self.decrement(&stream_id, Some(session_id));

        self.stream_refcounts.is_empty() && self.session_refcounts.is_empty()
    }

    /// Take one reference off a stream, and off a session when one was named.
    fn decrement(&mut self, stream_id: &i32, session_id: Option<i32>) {
        if let Some(index) = self
            .stream_refcounts
            .iter()
            .position(|(id, _)| id == stream_id)
        {
            self.stream_refcounts[index].1 -= 1;
            if self.stream_refcounts[index].1 <= 0 {
                self.stream_refcounts.swap_remove(index);
            }
        }

        if let Some(session_id) = session_id {
            if let Some(index) = self
                .session_refcounts
                .iter()
                .position(|((stream, session), _)| *stream == *stream_id && *session == session_id)
            {
                self.session_refcounts[index].1 -= 1;
                if self.session_refcounts[index].1 <= 0 {
                    self.session_refcounts.swap_remove(index);
                }
            }
        }
    }

    /// How many subscriptions read through this endpoint.
    pub fn subscription_count(&self) -> i32 {
        self.stream_refcounts.iter().map(|(_, count)| *count).sum()
    }

    /// Whether any image is served here (`image_ref_count`, which the conductor
    /// keeps beside the endpoint it created).
    pub fn image_count(&self) -> usize {
        self.dispatcher.images().len()
    }

    /// Read whatever the socket holds
    /// (`aeron_udp_channel_transport_recvmmsg` through the poller).
    ///
    /// # Errors
    ///
    /// The transport's error; an empty socket is `Ok(0)`.
    pub fn receive(
        &mut self,
        buffers: &mut [Vec<u8>],
        datagrams: &mut crate::sys::socket::Datagrams,
    ) -> io::Result<usize> {
        self.transport.receive(buffers, datagrams)
    }

    /// The address this endpoint's socket is bound to, which is the channel's
    /// endpoint parameter — and what the channel status reports as local.
    ///
    /// # Errors
    ///
    /// The error from `getsockname(2)`.
    pub fn local_address(&self) -> io::Result<SocketAddr> {
        self.transport.local_address()
    }

    /// Where a status message about `source` should go
    /// (`aeron_receive_destination.c:120-129`): the channel's control address
    /// when it named one, and otherwise the source of the data being answered.
    pub fn control_address(&self, source: SocketAddr) -> SocketAddr {
        if self.channel.has_explicit_control {
            self.channel.local_control
        } else {
            source
        }
    }

    /// Send a status message
    /// (`aeron_receive_channel_endpoint_send_sm`, `:291-338`).
    ///
    /// # Errors
    ///
    /// The socket's error.
    #[allow(clippy::too_many_arguments)] // the status message's fields
    pub fn send_sm(
        &mut self,
        destination: SocketAddr,
        stream_id: i32,
        session_id: i32,
        consumption_term_id: i32,
        consumption_term_offset: i32,
        receiver_window: i32,
        flags: u8,
    ) -> io::Result<usize> {
        let frame = StatusMessageFrame {
            session_id,
            stream_id,
            consumption_term_id,
            consumption_term_offset,
            receiver_window,
            receiver_id: self.receiver_id,
        };

        let mut buffer = [0u8; StatusMessageFrame::LENGTH];
        if frame.write_with_flags(&mut buffer, flags).is_none() {
            return Ok(0);
        }

        self.transport.send(Some(destination), &[&buffer])
    }

    /// Send a NAK (`aeron_receive_channel_endpoint_send_nak`, `:340-375`).
    ///
    /// # Errors
    ///
    /// The socket's error.
    pub fn send_nak(
        &mut self,
        destination: SocketAddr,
        stream_id: i32,
        session_id: i32,
        term_id: i32,
        term_offset: i32,
        length: i32,
    ) -> io::Result<usize> {
        let frame = NakFrame {
            session_id,
            stream_id,
            term_id,
            term_offset,
            length,
        };

        let mut buffer = [0u8; NakFrame::LENGTH];
        if frame.write(&mut buffer).is_none() {
            return Ok(0);
        }

        self.transport.send(Some(destination), &[&buffer])
    }

    /// Send an RTTM (`aeron_receive_channel_endpoint_send_rttm`, `:377-430`).
    ///
    /// # Errors
    ///
    /// The socket's error.
    #[allow(clippy::too_many_arguments)] // the frame's fields
    pub fn send_rttm(
        &mut self,
        destination: SocketAddr,
        stream_id: i32,
        session_id: i32,
        echo_timestamp: i64,
        reception_delta: i64,
        flags: u8,
    ) -> io::Result<usize> {
        let frame = RttmFrame {
            session_id,
            stream_id,
            echo_timestamp,
            reception_delta,
            receiver_id: self.receiver_id,
        };

        let mut buffer = [0u8; RttmFrame::LENGTH];
        if frame.write_with_flags(&mut buffer, flags).is_none() {
            return Ok(0);
        }

        self.transport.send(Some(destination), &[&buffer])
    }

    /// The state of one session, for the receiver's pending-setup sweep.
    pub fn state_of(&self, stream_id: i32, session_id: i32) -> ImageState {
        self.dispatcher.state_of(stream_id, session_id)
    }

    /// What a data packet wants
    /// (`aeron_data_packet_dispatcher_on_data`, `:385-431`).
    pub fn on_data(&mut self, stream_id: i32, session_id: i32, is_end_of_stream: bool) -> Interest {
        self.dispatcher
            .on_data(stream_id, session_id, is_end_of_stream)
    }

    /// Ask the source of a packet for a `SETUP`
    /// (`elicit_setup_from_source`, `:616-659`) and answer whether the status
    /// message should be sent.
    pub fn elicit_setup(&mut self, stream_id: i32, session_id: i32) -> bool {
        self.dispatcher
            .elicit_setup_from_source(stream_id, session_id)
    }

    /// A status message's flags for the two cases this module sends.
    pub const fn send_setup_flag() -> u8 {
        crate::protocol::header_flags::SM_SEND_SETUP
    }

    /// Whether this endpoint counted a short send, for the caller's counters.
    pub fn short_send(&self, system: &system_counters::System<'_>, sent: usize, expected: usize) {
        if sent < expected {
            system.increment(system_counters::id::SHORT_SENDS);
        }
    }

    /// The mode the channel's control address is in, which decides whether an
    /// arriving packet moves it.
    pub fn control_mode(&self) -> ControlMode {
        self.channel.control_mode
    }
}

impl std::fmt::Debug for ReceiveChannelEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveChannelEndpoint")
            .field("canonical_form", &self.channel.canonical_form)
            .field("receiver_id", &self.receiver_id)
            .field("streams", &self.dispatcher.stream_count())
            .field("subscriptions", &self.subscription_count())
            .finish_non_exhaustive()
    }
}
