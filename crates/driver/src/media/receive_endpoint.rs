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

use crate::protocol::{
    ErrorFrame, MAX_ERROR_TEXT_LENGTH, NakFrame, RspSetupFrame, RttmFrame, StatusMessageFrame,
};
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
impl ReceiveDestination {
    /// A destination with a socket of its own
    /// (`aeron_receive_destination_create`,
    /// `media/aeron_receive_destination.c:30-139`).
    ///
    /// # Errors
    ///
    /// [`ReceiveEndpointError::Socket`] when the socket cannot be opened or
    /// bound, [`ReceiveEndpointError::NoCounter`] when the manager has no room
    /// for the counter that holds its address.
    pub fn open(
        channel: UdpChannel,
        params: &TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        channel_status_counter_id: i32,
        now_ms: i64,
    ) -> Result<Self, ReceiveEndpointError> {
        // `aeron_receive_destination.c:69-75`: the bind address is the
        // channel's `remote_data` — the group, for a group — the interface is
        // its `local_data`, and a destination **never** connects, which is why
        // a subscriber's transport has one descriptor.
        let transport = super::udp_transport::UdpTransport::open(
            channel.remote_data,
            Some(channel.local_data),
            None,
            params,
        )
        .map_err(ReceiveEndpointError::Socket)?;

        Self::attach(
            channel,
            Box::new(transport),
            counters,
            regions,
            registration_id,
            channel_status_counter_id,
            now_ms,
        )
    }

    /// The same around a transport a caller built — the tests' seam, and the
    /// shape the conductor hands a destination over in.
    ///
    /// # Errors
    ///
    /// [`ReceiveEndpointError::NoCounter`] when the manager has no room, or the
    /// socket has no address to report.
    fn attach(
        channel: UdpChannel,
        transport: Box<dyn Transport>,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        channel_status_counter_id: i32,
        now_ms: i64,
    ) -> Result<Self, ReceiveEndpointError> {
        let local_sockaddr_counter_id = destination_local_sockaddr_counter(
            &*transport,
            counters,
            regions,
            registration_id,
            channel_status_counter_id,
            now_ms,
        )
        .ok_or(ReceiveEndpointError::NoCounter)?;

        let has_explicit_control = channel.has_explicit_control;

        Ok(Self {
            channel,
            transport,
            local_sockaddr_counter_id,
            has_explicit_control,
        })
    }

    /// The address this destination is actually bound to, as a counter a client
    /// can find by the channel status it belongs to
    /// (`rcv-local-sockaddr`, type 14).
    pub const fn local_sockaddr_counter_id(&self) -> i32 {
        self.local_sockaddr_counter_id
    }

    /// Whether the receiver has to ask this destination to describe its stream
    /// (`has_explicit_control`).
    pub const fn has_explicit_control(&self) -> bool {
        self.has_explicit_control
    }

    /// Where the ask goes: the channel's own control address
    /// (`udp_channel->local_control`,
    /// `media/aeron_receive_channel_endpoint.c:1138-1139`).
    ///
    /// [`None`] for a destination that has nothing to ask.
    pub fn setup_address(&self) -> Option<SocketAddr> {
        self.has_explicit_control
            .then_some(self.channel.local_control)
    }

    /// The channel this destination is for, which is what identifies it: the
    /// reference compares two by `aeron_udp_channel_equals` when one is removed
    /// (`media/aeron_receive_channel_endpoint.c:877-905`).
    pub const fn channel(&self) -> &UdpChannel {
        &self.channel
    }
}

/// Allocate the counter that says where a destination is **actually** bound
/// (`aeron_receive_destination.c:88-105`).
///
/// The address comes from the kernel, not from the channel: a destination is
/// allowed to name port zero — a multi-destination one does — and then this
/// counter's key is the only place the port that was chosen can be read from.
/// That is what `ReplayMerge` reads when it has to resolve a replay port
/// (`LocalSocketAddressStatus.findAddress`).
///
/// The value is the endpoint's state, as the channel status's is: the counter
/// being there is the news, and the reader reads the key.
///
/// # Errors
///
/// `None` when the socket has no address to report or the manager has no room.
/// Nothing is left behind.
fn destination_local_sockaddr_counter(
    transport: &dyn Transport,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    registration_id: i64,
    channel_status_counter_id: i32,
    now_ms: i64,
) -> Option<i32> {
    let local_sockaddr =
        crate::udp_channel::format_source_identity(transport.local_address().ok()?).ok()?;

    let counter_id = counter_position::allocate_local_sockaddr_counter(
        counters,
        regions,
        counter_position::RECEIVE_LOCAL_SOCKADDR_NAME,
        registration_id,
        channel_status_counter_id,
        &local_sockaddr,
        now_ms,
    )?;

    if counters
        .set_value(
            regions,
            counter_id,
            counter_position::channel_status::ACTIVE,
        )
        .is_none()
    {
        counters.free(regions, counter_id, now_ms);
        return None;
    }

    Some(counter_id)
}

/// One place a receive endpoint reads from
/// (`aeron_receive_destination_t`, `media/aeron_receive_destination.h:26-44`).
///
/// The reference gives every destination **its own socket** (`transport`,
/// `:34`) and its own channel, and a receive endpoint holds a list of them. A
/// unicast channel has exactly one — the endpoint it named — and a
/// multi-destination channel starts with none and gains them as clients add
/// them (`aeron_driver_conductor.c:2099-2114`).
///
/// The reference's entry also carries the control address it answers through,
/// whether the channel named one, and when something was last heard from it.
/// None of those are here yet: nothing in this build reads them, and a field
/// nothing writes is not a fact about the wire. They arrive with the
/// destinations that can differ from one another.
pub struct ReceiveDestination {
    /// The channel this destination is for.
    pub channel: UdpChannel,
    /// The socket it reads from and answers through.
    transport: Box<dyn Transport>,
    /// `rcv-local-sockaddr` (type 14): where this destination is **actually**
    /// bound, which is not what the channel said when it named port zero.
    local_sockaddr_counter_id: i32,
    /// Whether the destination's channel named a `control=`
    /// (`has_explicit_control`).
    ///
    /// This is what decides whether the receiver **opens the conversation**:
    /// a destination with an explicit control address is one the sender does
    /// not know about, so the receiver has to ask (`aeron_driver_receiver.c:475-486`,
    /// which adds a periodic pending setup for exactly these and no others).
    /// A destination the sender already knows — an implicit-unicast one — is
    /// asked for nothing.
    has_explicit_control: bool,
}

pub struct ReceiveChannelEndpoint {
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// Where this endpoint reads from, one entry per destination
    /// (`destinations`, `aeron_receive_destination.h:26-44`).
    ///
    /// A unicast channel has one — the endpoint it named — and every path below
    /// uses it. A multi-destination channel starts with none and gains them as
    /// clients add them (`aeron_driver_conductor.c:2099-2114`), which is why the
    /// callers handle the empty case rather than assuming a destination.
    destinations: Vec<ReceiveDestination>,
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

/// Write the address the endpoint is bound to into its channel-status label
/// (`aeron_driver_conductor.c:2157-2165`, asking
/// `aeron_receive_channel_endpoint_bind_addr_and_port`, `:316-329`).
///
/// The address is the **first destination's**, because the endpoint's socket is
/// a destination's socket — that is what the reference's accessor answers with,
/// and for a channel that starts with no destination it answers with nothing at
/// all. The label is then written anyway, with an empty address, so it ends in
/// a space: that is the reference's own output for the case, and matching it is
/// the whole point of writing the label rather than leaving the one the counter
/// was allocated with.
///
/// The socket is asked rather than the channel believed: a destination is
/// allowed to name port zero, and the kernel's answer is the only true one —
/// the same rule the send side's `publish_local_sockaddr` follows.
///
/// # Errors
///
/// [`ReceiveEndpointError::NoCounter`] when the label cannot be written, which
/// is the counter having vanished from under the allocator.
fn publish_bound_address(
    channel_status_counter_id: i32,
    channel: &[u8],
    destinations: &[ReceiveDestination],
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
) -> Result<(), ReceiveEndpointError> {
    let address = match destinations.first() {
        Some(destination) => crate::udp_channel::format_source_identity(
            destination
                .transport
                .local_address()
                .map_err(ReceiveEndpointError::Socket)?,
        )
        .map_err(|error| ReceiveEndpointError::Socket(io::Error::other(error)))?,
        None => String::new(),
    };

    // `"%s: %.*s %.*s"` — the name, the channel, the address
    // (`aeron_position.c:229-244`), replacing the label the counter was
    // allocated with. The channel is bytes rather than text, as it is there: it
    // is whatever the client sent, and this end never decodes it.
    let mut label = format!("{}: ", counter_position::RECEIVE_CHANNEL_STATUS_NAME).into_bytes();
    label.extend_from_slice(channel);
    label.push(b' ');
    label.extend_from_slice(address.as_bytes());

    counters
        .update_label(regions, channel_status_counter_id, &label)
        .ok_or(ReceiveEndpointError::NoCounter)?;

    Ok(())
}

/// Release the counters a failed create had already allocated, so a refusal
/// leaves nothing behind.
fn release_counters(
    destinations: &[ReceiveDestination],
    channel_status_counter_id: i32,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    now_ms: i64,
) {
    for destination in destinations {
        counters.free(regions, destination.local_sockaddr_counter_id, now_ms);
    }

    counters.free(regions, channel_status_counter_id, now_ms);
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

        // The endpoint's first destination opens the socket, because the
        // socket *is* a destination's: the receive side binds the endpoint —
        // where a subscription listens — and is never connected, because it
        // answers whoever writes to it (`aeron_receive_destination.c:30-139`).
        //
        // **Except on a manual channel, which starts with none at all.** The
        // reference creates this one only `if (AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL
        // != channel->control_mode)` (`aeron_driver_conductor.c:2099-2114`): a
        // manual channel's sources are the ones a client names with
        // `ADD_RCV_DESTINATION`, and until one is named there is nowhere to
        // listen. Every other channel is told where to listen by its own
        // channel, so it starts with that one.
        let destinations = if ControlMode::Manual == channel.control_mode {
            Vec::new()
        } else {
            match ReceiveDestination::open(
                channel.clone(),
                params,
                counters,
                regions,
                registration_id,
                channel_status_counter_id,
                now_ms,
            ) {
                Ok(destination) => vec![destination],
                Err(error) => {
                    // The counters were allocated for an endpoint that will not
                    // exist.
                    counters.free(regions, channel_status_counter_id, now_ms);
                    return Err(error);
                }
            }
        };

        if let Err(error) = publish_bound_address(
            channel_status_counter_id,
            &channel.original_uri,
            &destinations,
            counters,
            regions,
        ) {
            release_counters(
                &destinations,
                channel_status_counter_id,
                counters,
                regions,
                now_ms,
            );
            return Err(error);
        }

        Ok(Self {
            destinations,
            channel,
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

        // The same rule as [`Self::create`]: a manual channel starts with no
        // destinations, and a transport a caller supplied for one is not
        // attached to anything.
        let destinations = if ControlMode::Manual == channel.control_mode {
            Vec::new()
        } else {
            match ReceiveDestination::attach(
                channel.clone(),
                transport,
                counters,
                regions,
                registration_id,
                channel_status_counter_id,
                now_ms,
            ) {
                Ok(destination) => vec![destination],
                Err(error) => {
                    counters.free(regions, channel_status_counter_id, now_ms);
                    return Err(error);
                }
            }
        };

        if let Err(error) = publish_bound_address(
            channel_status_counter_id,
            &channel.original_uri,
            &destinations,
            counters,
            regions,
        ) {
            release_counters(
                &destinations,
                channel_status_counter_id,
                counters,
                regions,
                now_ms,
            );
            return Err(error);
        }

        Ok(Self {
            destinations,
            channel,
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
    pub fn receive_from(
        &mut self,
        index: usize,
        buffers: &mut [Vec<u8>],
        datagrams: &mut crate::sys::socket::Datagrams,
    ) -> io::Result<usize> {
        match self.destinations.get_mut(index) {
            Some(destination) => destination.transport.receive(buffers, datagrams),
            // A destination that is not there has nothing to read, which is a
            // state rather than a failure — the reference polls each of them and
            // polls none when there are none.
            None => Ok(0),
        }
    }

    /// Attach a destination a client added
    /// (`aeron_receive_channel_endpoint_add_destination`).
    ///
    /// The destination arrives **built** — its socket open, the counter holding
    /// its address allocated — because the conductor is what has a counter
    /// manager (`aeron_driver_conductor.c:5903-5919`: it creates the destination
    /// and hands it to the receiver, exactly as it hands over an endpoint).
    /// This is the receiver's half: the endpoint holds it, and the next pass
    /// reads from it.
    pub fn add_destination(&mut self, destination: ReceiveDestination) {
        self.destinations.push(destination);
    }

    /// Take a destination off, answering with it when there was one.
    ///
    /// A destination is identified by its channel, which is how the reference
    /// compares two (`media/aeron_receive_channel_endpoint.c:877-905`). The
    /// counters it holds are the caller's to give back, because the caller
    /// allocated them.
    pub fn remove_destination(&mut self, channel: &UdpChannel) -> Option<ReceiveDestination> {
        let index = self
            .destinations
            .iter()
            .position(|destination| destination.channel.canonical_form == channel.canonical_form)?;

        Some(self.destinations.swap_remove(index))
    }

    /// The destinations, in the order they were added.
    pub fn destinations(&self) -> &[ReceiveDestination] {
        &self.destinations
    }

    /// How many places this endpoint reads from.
    ///
    /// A unicast channel has one; a multi-destination channel has as many as
    /// clients have added. The receiver polls every one of them
    /// (`aeron_driver_receiver_do_work`, `:130-260`, which polls the transport
    /// poller — every transport in it).
    pub fn destination_count(&self) -> usize {
        self.destinations.len()
    }

    /// The address this endpoint's socket is bound to, which is the channel's
    /// endpoint parameter — and what the channel status reports as local.
    ///
    /// # Errors
    ///
    /// The error from `getsockname(2)`.
    pub fn local_address(&self) -> io::Result<SocketAddr> {
        match self.destination() {
            Some(destination) => destination.transport.local_address(),
            None => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "a channel with no destinations is bound to nothing",
            )),
        }
    }

    /// Where a status message about `source` should go
    /// (`aeron_receive_destination.c:120-129`): a group's control twin, the
    /// control address a channel named, or else the source of the data being
    /// answered.
    ///
    /// It is the **destination's** channel that answers, not the endpoint's
    /// ([`UdpChannel::control_address`], asked of the destination's own
    /// channel): a multi-destination endpoint's destinations are channels of
    /// their own, and one of them may be a group while the endpoint is not.
    pub fn control_address(&self, destination_index: usize, source: SocketAddr) -> SocketAddr {
        self.destinations
            .get(destination_index)
            .map_or(source, |destination| {
                destination.channel.control_address(source)
            })
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
        self.send_sm_from(
            0,
            destination,
            stream_id,
            session_id,
            consumption_term_id,
            consumption_term_offset,
            receiver_window,
            flags,
        )
    }

    /// The same, through the destination at `index`.
    ///
    /// A datagram arrives on **one** destination's socket, and an answer has to
    /// leave through that one — a status message that went out of another
    /// destination's socket would be a receiver reporting a position to a sender
    /// that never asked it (`aeron_receive_channel_endpoint_send_sm`, which
    /// takes the destination it is answering).
    ///
    /// # Errors
    ///
    /// The socket's error.
    #[allow(clippy::too_many_arguments)] // the status message's fields
    pub fn send_sm_from(
        &mut self,
        index: usize,
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

        match self.destinations.get_mut(index) {
            Some(entry) => entry.transport.send(Some(destination), &[&buffer]),
            None => Ok(0),
        }
    }

    /// Send a `RSP_SETUP`
    /// (`aeron_receive_channel_endpoint_send_response_setup`, `:432-466`).
    ///
    /// This is the answer an image owes a publication whose `SETUP` carried
    /// [`header_flags::SETUP_SEND_RESPONSE`](crate::protocol::header_flags::SETUP_SEND_RESPONSE):
    /// the publisher asked for a response channel and cannot know the session
    /// the answers will arrive on, because it is *this image's*. The frame
    /// carries no correlation id — a registration id never crosses the wire —
    /// so the session is the whole of what it says
    /// (`aeron_udp_protocol.h:158-165`).
    ///
    /// It leaves through the destination the request arrived on, the same one a
    /// status message would ([`Self::send_sm_from`]).
    ///
    /// # Errors
    ///
    /// The socket's error.
    pub fn send_response_setup(
        &mut self,
        index: usize,
        destination: SocketAddr,
        stream_id: i32,
        session_id: i32,
        response_session_id: i32,
    ) -> io::Result<usize> {
        let frame = RspSetupFrame {
            session_id,
            stream_id,
            response_session_id,
        };

        let mut buffer = [0u8; RspSetupFrame::LENGTH];
        if frame.write(&mut buffer).is_none() {
            return Ok(0);
        }

        match self.destinations.get_mut(index) {
            Some(entry) => entry.transport.send(Some(destination), &[&buffer]),
            None => Ok(0),
        }
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

        match self.destination_mut() {
            Some(entry) => entry.transport.send(Some(destination), &[&buffer]),
            None => Ok(0),
        }
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

        match self.destination_mut() {
            Some(entry) => entry.transport.send(Some(destination), &[&buffer]),
            None => Ok(0),
        }
    }

    /// Tell a publisher its image was refused
    /// (`aeron_receiver_channel_endpoint_send_error_frame`, `:466-509`).
    ///
    /// This is the **only** frame this endpoint sends that is not an answer to
    /// something the far end asked for: a status message, a NAK and an RTTM all
    /// report on data that arrived, and an `ERR` reports a decision the reader
    /// made. It goes to the connection's control address, like the rest.
    ///
    /// The text is clipped to [`MAX_ERROR_TEXT_LENGTH`], which is the frame's
    /// own bound and the client's — `strnlen(invalidation_reason,
    /// AERON_ERROR_MAX_TEXT_LENGTH)` is what the reference measures with
    /// (`:479`), so a longer reason is cut here rather than sent and cut there.
    ///
    /// The buffer is a stack array sized for the largest frame the format
    /// allows, as the reference's is (`AERON_ERROR_MAX_FRAME_LENGTH`), and the
    /// counters are raised here for the same reason they are there: the
    /// distinction between a frame that went and one that was cut short is only
    /// visible to whoever saw the socket's answer.
    ///
    /// # Errors
    ///
    /// The socket's error.
    pub fn send_error_frame(
        &mut self,
        destination: SocketAddr,
        stream_id: i32,
        session_id: i32,
        error_code: i32,
        invalidation_reason: &[u8],
        system: &system_counters::System<'_>,
    ) -> io::Result<usize> {
        #[allow(clippy::cast_sign_loss)] // the constant is 1023
        let limit = MAX_ERROR_TEXT_LENGTH as usize;
        let reason = &invalidation_reason[..invalidation_reason.len().min(limit)];

        #[allow(clippy::cast_possible_truncation)] // bounded by the limit above
        let error_length = reason.len() as i32;

        let frame = ErrorFrame {
            session_id,
            stream_id,
            receiver_id: self.receiver_id,
            // Written even when the flag says to ignore it, which is what the
            // reference does (`:488-489`). The flag is never set here: a group
            // tag belongs to a multicast channel, and this build's endpoints do
            // not carry one yet.
            group_tag: 0,
            error_code,
            error_length,
        };

        let mut buffer = [0u8; ErrorFrame::LENGTH + MAX_ERROR_TEXT_LENGTH as usize];
        if frame.write(&mut buffer[..ErrorFrame::LENGTH]).is_none() {
            return Ok(0);
        }

        buffer[ErrorFrame::LENGTH..ErrorFrame::LENGTH + reason.len()].copy_from_slice(reason);

        let length = ErrorFrame::LENGTH + reason.len();
        let sent = match self.destination_mut() {
            Some(entry) => entry
                .transport
                .send(Some(destination), &[&buffer[..length]])?,
            None => return Ok(0),
        };

        if sent < 1 {
            // One datagram, not one frame's worth of bytes. The reference
            // compares what the socket wrote against the `iovec` it was handed
            // (`:496-502`), which is a byte count — but its socket is UDP, so a
            // partial write cannot happen there, and a short send means a
            // transport underneath that cut the frame in two. This build's
            // [`Transport::send`](super::Transport::send) answers with how many
            // *datagrams* left, so the same counter stands for the frame that
            // did not go at all.
            self.short_send(system, sent, 1);
        } else {
            system.increment(system_counters::id::ERROR_FRAMES_SENT);
        }

        Ok(sent)
    }

    /// The destination this endpoint reads from, or answers through.
    ///
    /// A unicast channel has exactly one, so every path here uses it. The
    /// reference chooses by the destination a source belongs to, which needs the
    /// per-destination control addresses that arrive with the multi-destination
    /// work.
    fn destination(&self) -> Option<&ReceiveDestination> {
        self.destinations.first()
    }

    /// The same, mutably — reading is what needs it.
    fn destination_mut(&mut self) -> Option<&mut ReceiveDestination> {
        self.destinations.first_mut()
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
    /// Give up on a session that never answered
    /// (`aeron_receive_channel_endpoint_on_remove_pending_setup`, `RCE:1127-1177`
    /// reaching the dispatcher): its interest is dropped, so the next frame
    /// from it asks for a `SETUP` again.
    ///
    /// This is what a **non-periodic** pending setup does when it expires — the
    /// one a data frame elicited. A periodic one, which a destination with a
    /// control address creates, is asked again instead.
    pub fn remove_pending_setup(&mut self, stream_id: i32, session_id: i32) {
        self.dispatcher.remove_pending_setup(stream_id, session_id);
    }

    pub fn elicit_setup(&mut self, stream_id: i32, session_id: i32) -> bool {
        self.dispatcher
            .elicit_setup_from_source(stream_id, session_id)
    }

    /// Ask **every** destination for the stream
    /// (`aeron_receive_channel_endpoint_elicit_setup`,
    /// `media/aeron_receive_channel_endpoint.c:259-289`).
    ///
    /// Unconditional, and that is the reference's shape: the endpoint-level ask
    /// is not a decision, it is the whole act, and it goes to every destination
    /// the endpoint has. What decides *whether* to ask is the caller's, and the
    /// guard there is the channel's — a channel that named no control address
    /// has nowhere to send it (`aeron_driver_receiver.c:418-426`).
    ///
    /// It is a separate method from [`Self::elicit_setup`] because that one
    /// answers a different question: it is the *data*-driven path
    /// (`aeron_data_packet_dispatcher_elicit_setup_from_source`, `:616-659`),
    /// which is asked once per session and decides whether an ask is owed at
    /// all. This one is the ask.
    ///
    /// Returns how many destinations it reached, which is what the reference's
    /// `work_count` counts.
    pub fn elicit_setup_to_destinations(&mut self, stream_id: i32, session_id: i32) -> usize {
        let mut reached = 0;

        for index in 0..self.destinations.len() {
            let Some(address) = self.destinations[index].setup_address() else {
                continue;
            };

            if self
                .send_sm_from(
                    index,
                    address,
                    stream_id,
                    session_id,
                    0,
                    0,
                    0,
                    Self::send_setup_flag(),
                )
                .is_ok()
            {
                reached += 1;
            }
        }

        reached
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

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::AtomicBuffer;

    const VALUES_LENGTH: usize = 64 * 1024;

    #[repr(align(64))]
    struct Region(Vec<u8>);

    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                metadata: Region(vec![0u8; VALUES_LENGTH * 4]),
                values: Region(vec![0u8; VALUES_LENGTH]),
            }
        }

        fn open(&mut self) -> (CounterManager, CounterRegions<'_>) {
            let regions = CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.values.0).expect("aligned"),
            )
            .expect("four-to-one");
            let manager = CounterManager::new(VALUES_LENGTH, 1_000).expect("room");

            (manager, regions)
        }
    }

    /// A transport that is somewhere and moves nothing.
    struct Stub(SocketAddr);

    impl Transport for Stub {
        fn send(&mut self, _address: Option<SocketAddr>, _buffers: &[&[u8]]) -> io::Result<usize> {
            Ok(0)
        }

        fn receive(
            &mut self,
            _buffers: &mut [Vec<u8>],
            _datagrams: &mut crate::sys::socket::Datagrams,
        ) -> io::Result<usize> {
            Ok(0)
        }

        fn local_address(&self) -> io::Result<SocketAddr> {
            Ok(self.0)
        }

        fn receive_buffer_size(&self) -> io::Result<usize> {
            Ok(0)
        }
    }

    fn channel(uri: &str) -> UdpChannel {
        let parsed = crate::channel_uri::ChannelUri::parse(uri.as_bytes()).expect("a URI");
        UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel")
    }

    fn stub(port: u16) -> Box<dyn Transport> {
        Box::new(Stub(SocketAddr::from(([127, 0, 0, 1], port))))
    }

    /// A transport that keeps what it was handed, for the frames that *leave* —
    /// which the stub above cannot answer, because "what went out" is the whole
    /// of what an `ERR` frame is.
    ///
    /// The handle is shared rather than borrowed because a transport is moved
    /// into the endpoint, and behind a mutex because a transport is `Send`.
    #[derive(Clone, Default)]
    struct Sent(std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>);

    impl Sent {
        fn frames(&self) -> Vec<Vec<u8>> {
            self.0.lock().expect("no other thread holds it").clone()
        }

        fn last(&self) -> Vec<u8> {
            self.frames().pop().expect("a frame went out")
        }
    }

    impl Transport for Sent {
        fn send(&mut self, _address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
            let mut frames = self.0.lock().expect("no other thread holds it");

            for buffer in buffers {
                frames.push(buffer.to_vec());
            }

            Ok(buffers.len())
        }

        fn receive(
            &mut self,
            _buffers: &mut [Vec<u8>],
            _datagrams: &mut crate::sys::socket::Datagrams,
        ) -> io::Result<usize> {
            Ok(0)
        }

        fn local_address(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 40123)))
        }

        fn receive_buffer_size(&self) -> io::Result<usize> {
            Ok(0)
        }
    }

    /// Every destination brings its own address counter, and two destinations
    /// do not share one (`rcv-local-sockaddr`, type 14).
    #[test]
    fn a_destination_brings_its_own_socket_and_its_own_address_counter() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let first = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            stub(40123),
            &mut counters,
            &regions,
            7,
            0,
            1_000,
        )
        .expect("a destination");

        let second = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40124"),
            stub(40124),
            &mut counters,
            &regions,
            8,
            0,
            1_000,
        )
        .expect("a destination");

        assert_ne!(
            first.local_sockaddr_counter_id(),
            second.local_sockaddr_counter_id()
        );
        assert_eq!(
            0,
            first.local_sockaddr_counter_id(),
            "the first counter a fresh manager hands out"
        );
    }

    /// Only a destination whose channel named a `control=` has anything to ask
    /// (`aeron_driver_receiver.c:475-486`).
    ///
    /// This is the condition the receiver pushes a periodic pending setup on: a
    /// sender that does not know the receiver exists will never describe its
    /// stream, so the receiver has to say so — and keep saying so, which is what
    /// makes the entry periodic. A destination the sender already knows is asked
    /// for nothing.
    #[test]
    fn only_a_destination_with_a_control_address_has_something_to_ask() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let with_control = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124"),
            stub(40123),
            &mut counters,
            &regions,
            7,
            0,
            1_000,
        )
        .expect("a destination");

        assert!(
            with_control.has_explicit_control(),
            "the channel named a control address"
        );
        assert_eq!(
            Some("127.0.0.1:40124".parse().expect("an address")),
            with_control.setup_address(),
            "and the ask goes to it"
        );

        let without = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40125"),
            stub(40125),
            &mut counters,
            &regions,
            8,
            0,
            1_000,
        )
        .expect("a destination");

        assert!(!without.has_explicit_control());
        assert_eq!(None, without.setup_address(), "nothing to ask");
    }

    /// An endpoint holds what it is given and reads from all of them: one on
    /// creation, and as many more as clients add.
    #[test]
    fn an_endpoint_holds_the_destinations_it_is_given() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            stub(40123),
            1,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
        )
        .expect("an endpoint");

        assert_eq!(
            0,
            endpoint.destination_count(),
            "a manual channel starts with none: its sources are the ones a client names"
        );

        let added = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40124"),
            stub(40124),
            &mut counters,
            &regions,
            8,
            endpoint.channel_status_counter_id(),
            1_000,
        )
        .expect("a destination");

        endpoint.add_destination(added);
        assert_eq!(1, endpoint.destination_count());

        let removed = endpoint
            .remove_destination(&channel("aeron:udp?endpoint=127.0.0.1:40124"))
            .expect("the destination that was added");

        assert_eq!(
            channel("aeron:udp?endpoint=127.0.0.1:40124").canonical_form,
            removed.channel().canonical_form,
            "and it is the one that was added"
        );
        assert_eq!(0, endpoint.destination_count());
        assert!(
            endpoint
                .remove_destination(&channel("aeron:udp?endpoint=127.0.0.1:40999"))
                .is_none(),
            "a channel no destination has removes nothing"
        );
    }

    /// An endpoint of a test's own, with a transport that keeps what leaves.
    fn endpoint_that_records(
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) -> (ReceiveChannelEndpoint, Sent) {
        let sent = Sent::default();

        let endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            Box::new(sent.clone()),
            1234,
            16,
            counters,
            regions,
            7,
            1_000,
        )
        .expect("an endpoint");

        (endpoint, sent)
    }

    /// The frame a rejected image puts on the wire, byte for byte — the only
    /// way a publisher learns *why* its stream stopped.
    #[test]
    fn an_error_frame_carries_the_reason_and_the_receiver_that_refused_it() {
        use crate::protocol::{FrameHeader, frame_type};

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, sent) = endpoint_that_records(&mut counters, &regions);
        let system = system_counters::System::new(&counters, &regions);

        let reason = b"Needs to be closed";
        let length = endpoint
            .send_error_frame(
                "127.0.0.1:40456".parse().expect("an address"),
                1001,
                42,
                deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
                reason,
                &system,
            )
            .expect("a send");

        assert_eq!(1, length, "one datagram left");
        assert_eq!(1, system.value(system_counters::id::ERROR_FRAMES_SENT));
        assert_eq!(0, system.value(system_counters::id::SHORT_SENDS));

        let frame = sent.last();
        let header = FrameHeader::read(&frame).expect("a header");

        assert_eq!(frame_type::ERR, header.frame_type);
        assert_eq!(
            i32::try_from(ErrorFrame::LENGTH + reason.len()).expect("a short frame"),
            header.frame_length,
            "the length covers the text as well as the header"
        );

        let error = ErrorFrame::read(&frame).expect("an ERR frame");

        assert_eq!(42, error.session_id);
        assert_eq!(1001, error.stream_id);
        assert_eq!(
            1234, error.receiver_id,
            "the endpoint's own id — the publisher's liveness signal"
        );
        assert_eq!(0, error.group_tag, "no group tag is set on this channel");
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
            error.error_code
        );
        assert_eq!(reason.len() as i32, error.error_length);
        assert_eq!(reason, error.text(&frame).expect("its text"));
    }

    /// The text is clipped to the frame's own bound, which is the one the
    /// reference measures with (`aeron_receive_channel_endpoint.c:479`).
    #[test]
    fn an_over_long_reason_is_clipped_to_the_frames_own_bound() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, sent) = endpoint_that_records(&mut counters, &regions);
        let system = system_counters::System::new(&counters, &regions);

        let reason = vec![b'x'; 2000];
        endpoint
            .send_error_frame(
                "127.0.0.1:40456".parse().expect("an address"),
                1001,
                42,
                deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
                &reason,
                &system,
            )
            .expect("a send");

        let frame = sent.last();
        let error = ErrorFrame::read(&frame).expect("an ERR frame");

        assert_eq!(MAX_ERROR_TEXT_LENGTH, error.error_length);
        assert_eq!(ErrorFrame::LENGTH + 1023, frame.len());
    }
}
