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

use super::interceptor::Incoming;
use crate::protocol::{
    ErrorFrame, MAX_ERROR_TEXT_LENGTH, NakFrame, RspSetupFrame, RttmFrame, StatusMessageFrame,
    header_flags,
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
    /// The **bind** failed, with the composition so far.
    ///
    /// Apart from the rest because this is the one failure whose message the
    /// reference builds out of facts from three different layers, each
    /// appending its own line (`AERON_APPEND_ERR`): the syscall's descriptor
    /// and address, the transport's affinity, this destination's channel. The
    /// two layers above append the correlation id and the subscription, which
    /// is why it travels as a value rather than being finished here.
    Bind(deepmsg_cnc::error_log::ErrorReport),
    /// The wildcard port manager had no port to give
    /// (`aeron_wildcard_port_manager_get_managed_port`, `aeron_port_manager.c:93-104`).
    ///
    /// The words are the manager's own, because they are what a client reads in
    /// its `RegistrationException` when the driver has run out of ports
    /// (`WildcardPortManagerSystemTest.java:90`).
    Port(crate::port_manager::PortError),
}

impl std::fmt::Display for ReceiveEndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCounter => f.write_str("could not allocate the receive channel status counter"),
            Self::Socket(error) => write!(f, "{error}"),
            Self::Bind(report) => f.write_str(report.text()),
            Self::Port(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReceiveEndpointError {}

/// The first three lines of what the reference records when a receive
/// destination cannot bind: `aeron_bind` set it, and two layers appended to it
/// on the way up (`aeron_socket.c:93`, `aeron_udp_channel_transport.c:151`,
/// `aeron_receive_destination.c:78`).
///
/// `affinity=1` is `AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER`
/// (`media/aeron_udp_channel_transport_bindings.h:26-30` — the sender's is 0),
/// which the reference passes as an enum and prints as its number.
fn bind_report(
    channel: &crate::udp_channel::UdpChannel,
    failure: &crate::sys::socket::BindFailure,
) -> deepmsg_cnc::error_log::ErrorReport {
    let errno = failure.source.raw_os_error().unwrap_or(libc::EINVAL);

    let mut report = deepmsg_cnc::error_log::ErrorReport::set(
        errno,
        "aeron_bind",
        "aeron_socket.c",
        93,
        &format!("failed to bind({}, {})", failure.fd, failure.address),
    );

    report.append(
        "aeron_udp_channel_transport_init",
        "aeron_udp_channel_transport.c",
        151,
        "unicast bind, affinity=1",
    );

    report.append(
        "aeron_receive_destination_create",
        "aeron_receive_destination.c",
        78,
        &format!("uri = {}", String::from_utf8_lossy(&channel.original_uri)),
    );

    report
}

/// A receive channel endpoint and its socket.
impl ReceiveDestination {
    /// The descriptor the poller watches for this destination (G4-3).
    pub fn descriptor(&self) -> Option<crate::sys::socket::Descriptor> {
        self.transport.descriptor()
    }

    /// A destination with a socket of its own
    /// (`aeron_receive_destination_create`,
    /// `media/aeron_receive_destination.c:30-139`).
    ///
    /// # Errors
    ///
    /// [`ReceiveEndpointError::Socket`] when the socket cannot be opened or
    /// bound, [`ReceiveEndpointError::NoCounter`] when the manager has no room
    /// for the counter that holds its address.
    #[allow(clippy::too_many_arguments)] // one per collaborator, not one per decision
    pub fn open(
        channel: UdpChannel,
        port_manager: &mut crate::port_manager::WildcardPortManager,
        params: &TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        channel_status_counter_id: i32,
        now_ms: i64,
        now_ns: i64,
        interceptors: &[super::interceptor::Interceptor],
    ) -> Result<Self, ReceiveEndpointError> {
        // `aeron_receive_destination.c:47-56`: the manager is asked for the
        // port **before** the socket is opened, on the address the channel
        // wrote as `endpoint=` — `remote_data`, the group for a group.
        //
        // A subscription is the case a managed range is for: a reader that
        // named `endpoint=127.0.0.1:0` is one the writer has to be *told*
        // about, and the port it got is the telling.
        let bind = port_manager
            .get_managed_port(&channel, channel.remote_data)
            .map_err(ReceiveEndpointError::Port)?;
        let managed_port = bind.port();

        // `:69-75`: the bind address is that one, the interface is the
        // channel's `local_data`, and a destination **never** connects, which
        // is why a subscriber's transport has one descriptor.
        let transport = match super::udp_transport::UdpTransport::open(
            bind,
            Some(channel.local_data),
            None,
            params,
        ) {
            Ok(transport) => transport,
            Err(super::udp_transport::OpenError::Bind(failure)) => {
                // The port goes back with the socket that could not have it:
                // the reference's create calls its own delete on every failure
                // path, and the managed port is freed there
                // (`:54`, `:79`, `:152-156`).
                port_manager.free_managed_port(managed_port);
                return Err(ReceiveEndpointError::Bind(bind_report(&channel, &failure)));
            }
            Err(super::udp_transport::OpenError::Io(error)) => {
                port_manager.free_managed_port(managed_port);
                return Err(ReceiveEndpointError::Socket(error));
            }
        };

        Self::attach(
            channel,
            managed_port,
            Box::new(transport),
            counters,
            regions,
            registration_id,
            channel_status_counter_id,
            now_ms,
            now_ns,
            interceptors,
        )
    }

    /// The same around a transport a caller built — the tests' seam, and the
    /// shape the conductor hands a destination over in.
    ///
    /// # Errors
    ///
    /// [`ReceiveEndpointError::NoCounter`] when the manager has no room, or the
    /// socket has no address to report.
    #[allow(clippy::too_many_arguments)] // one per collaborator, not one per decision
    fn attach(
        channel: UdpChannel,
        managed_port: u16,
        transport: Box<dyn Transport>,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        channel_status_counter_id: i32,
        now_ms: i64,
        now_ns: i64,
        interceptors: &[super::interceptor::Interceptor],
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

        // `:120-129`: a group's control address is its twin, an explicit
        // control is the one the channel named, and a destination that named
        // neither has none — which is the same flag that says it never asks
        // (`has_explicit_control`).
        let control_addr = if channel.is_multicast {
            channel.remote_control
        } else {
            channel.local_control
        };

        Ok(Self {
            channel,
            transport,
            interceptors: Incoming::new(interceptors),
            local_sockaddr_counter_id,
            managed_port,
            has_explicit_control,
            control_addr,
            time_of_last_activity_ns: now_ns,
        })
    }

    /// Data, a `SETUP` or an `RTTM` arrived for this destination, which is what
    /// "it is still there" means (`aeron_receive_channel_endpoint.c:589`).
    pub const fn on_activity(&mut self, now_ns: i64) {
        self.time_of_last_activity_ns = now_ns;
    }

    /// Whether this destination's control name has to be resolved again
    /// (`aeron_receive_destination_re_resolution_required`,
    /// `aeron_receive_destination.h:65-69`): it named one, and nothing has
    /// arrived for five seconds.
    pub fn re_resolution_required(&self, now_ns: i64) -> bool {
        self.has_explicit_control
            && now_ns > self.time_of_last_activity_ns + RECEIVE_DESTINATION_TIMEOUT_NS
    }

    /// The name to ask about, or [`None`] for a destination that named none.
    pub fn control_name(&self) -> Option<&str> {
        self.channel.control_name.as_deref()
    }

    /// Take the answer (`aeron_receive_channel_endpoint_update_control_address`,
    /// `:1064-1073`, which updates **only** a destination that named a control
    /// address — the others have no name and nothing to move).
    pub fn update_control_address(&mut self, address: SocketAddr) {
        if self.has_explicit_control {
            self.control_addr = address;
        }
    }

    /// Where this destination's own control address is now.
    pub const fn current_control_address(&self) -> SocketAddr {
        self.control_addr
    }

    /// The address this destination is actually bound to, as a counter a client
    /// can find by the channel status it belongs to
    /// (`rcv-local-sockaddr`, type 14).
    pub const fn local_sockaddr_counter_id(&self) -> i32 {
        self.local_sockaddr_counter_id
    }

    /// The port the wildcard port manager is holding for this destination, and
    /// zero when it holds none.
    pub const fn managed_port(&self) -> u16 {
        self.managed_port
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
        self.has_explicit_control.then_some(self.control_addr)
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

/// The handle a receive endpoint gives one of its destinations.
///
/// The reference names a destination by its **address**: an
/// `aeron_receive_destination_t *` lives in the endpoint's `destinations`
/// array, and everyone who has to answer *through* one carries that pointer —
/// each connection an image has (`aeron_publication_image.h:41`) and each
/// pending setup (`aeron_driver_receiver.h:41`). Sending then takes the
/// destination as an argument rather than choosing one
/// (`aeron_receive_channel_endpoint_send`,
/// `media/aeron_receive_channel_endpoint.c:225-257`).
///
/// A Rust endpoint cannot hand out pointers into its own `Vec` — it has to keep
/// writing to it — so the handle is a number. The one property it has to have
/// is the pointer's: **never reused**. A destination is taken off with
/// `swap_remove`, which moves the last entry into the hole, so a *position* is
/// not an identity at all here: an image still holding the number of a
/// destination that has gone would answer through whichever socket inherited
/// the slot. A number that is retired with its destination cannot do that; a
/// handle with no destination behind it sends nothing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct DestinationId(u64);

impl DestinationId {
    /// The handle of the first destination an endpoint is given — the one a
    /// unicast channel opens with, and the one a destination added to a manual
    /// channel gets if it is the first.
    pub const FIRST: Self = Self(0);

    /// A handle a test hands itself when it has no endpoint to mint one.
    ///
    /// Handles are the endpoint's to give and are never reused (see above), so
    /// this exists for the tests that build an image on its own and would
    /// otherwise have only [`Self::FIRST`] to name a second destination with.
    #[cfg(test)]
    pub const fn for_test(index: u64) -> Self {
        Self(index)
    }
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
/// `AERON_RECEIVE_DESTINATION_TIMEOUT_NS`
/// (`media/aeron_receive_destination.h:24`): how long a destination that named
/// a control address may go unheard-from before that name is resolved again.
pub const RECEIVE_DESTINATION_TIMEOUT_NS: i64 = 5 * 1000 * 1000 * 1000;

pub struct ReceiveDestination {
    /// The channel this destination is for.
    pub channel: UdpChannel,
    /// The socket it reads from and answers through.
    transport: Box<dyn Transport>,
    /// What this transport's datagrams pass through before the endpoint sees
    /// them ([`crate::media::interceptor`]).
    ///
    /// Per destination and not per driver, which is the reference's own
    /// granularity: it builds the chain in `aeron_udp_channel_data_paths_init`
    /// (`media/aeron_udp_channel_transport_bindings.c:186-260`), called from
    /// each transport's init, and two of the three built-in interceptors keep
    /// per-stream state that must not be shared between two sockets.
    interceptors: Incoming,
    /// `rcv-local-sockaddr` (type 14): where this destination is **actually**
    /// bound, which is not what the channel said when it named port zero.
    local_sockaddr_counter_id: i32,
    /// The port the wildcard port manager is holding for this destination, and
    /// zero when it holds none
    /// (`aeron_receive_destination.port_manager` + the bind address it was
    /// given, `media/aeron_receive_destination.c:47-58`).
    ///
    /// Non-zero only when the driver named a range and the channel named port
    /// zero. It travels back to the conductor with the counter when the
    /// destination is gone, because giving the port back is the conductor's —
    /// the manager is on its thread — and because the reference gives it back
    /// where the destination is deleted (`:152-156`), which is here.
    managed_port: u16,
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
    /// Where this destination's control frames go **now** (`current_control_addr`,
    /// `aeron_receive_destination.c:120-129`): the channel's own control address
    /// until a re-resolution moves it.
    ///
    /// It is stored rather than read off the channel because the channel is
    /// immutable and a name is not: a destination whose control name resolves
    /// somewhere else has to answer there, and this is the only place that
    /// knows.
    control_addr: SocketAddr,
    /// When this destination was last heard from
    /// (`time_of_last_activity_ns`, `:118` at creation): data, a `SETUP` or an
    /// `RTTM` (`aeron_receive_channel_endpoint.c:589`, `:631`, `:659`), and the
    /// check itself (`:1059`).
    ///
    /// Five seconds of silence from a destination that **named** a control
    /// address is a name to resolve again
    /// (`aeron_receive_destination_re_resolution_required`,
    /// `aeron_receive_destination.h:65-69`).
    time_of_last_activity_ns: i64,
}

pub struct ReceiveChannelEndpoint {
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// Where this endpoint reads from, one entry per destination
    /// (`destinations`, `aeron_receive_destination.h:26-44`), each under the
    /// [`DestinationId`] that names it.
    ///
    /// A unicast channel has one — the endpoint it named — and a
    /// multi-destination channel starts with none and gains them as clients add
    /// them (`aeron_driver_conductor.c:2099-2114`), which is why the callers
    /// handle the empty case rather than assuming a destination.
    ///
    /// The pair is the point: a position in this `Vec` is a **cursor** — good
    /// for one pass of the receive loop and nothing else — while the id beside
    /// it is what an image, a connection or a pending setup may hold on to.
    destinations: Vec<(DestinationId, ReceiveDestination)>,
    /// The next handle to give out.
    ///
    /// Monotonic and never reset, including across a removal, which is the
    /// whole of what makes a stale handle harmless (see [`DestinationId`]).
    next_destination_id: u64,
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
    /// The group tag this endpoint stamps into what it sends (`group_tag`):
    /// the channel's own `gtag=` when it named one, the driver's setting
    /// otherwise (`aeron_receive_channel_endpoint_set_group_tag`, `:32-53`).
    ///
    /// [`None`] is the difference the reference keeps between "no tag" and a
    /// tag of `-1`: the first sends a 36-byte status message and leaves the
    /// error frame's flag clear, the second sends 44 and sets it.
    group_tag: Option<i64>,
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
    destinations: &[(DestinationId, ReceiveDestination)],
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
) -> Result<(), ReceiveEndpointError> {
    let address = match destinations.first() {
        Some((_, destination)) => crate::udp_channel::format_source_identity(
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
    destinations: &[(DestinationId, ReceiveDestination)],
    channel_status_counter_id: i32,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    now_ms: i64,
) {
    for (_, destination) in destinations {
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
        group_tag: Option<i64>,
        port_manager: &mut crate::port_manager::WildcardPortManager,
        params: &TransportParams,
        receiver_id: i64,
        stream_session_limit: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
        now_ns: i64,
        interceptors: &[super::interceptor::Interceptor],
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
                port_manager,
                params,
                counters,
                regions,
                registration_id,
                channel_status_counter_id,
                now_ms,
                now_ns,
                interceptors,
            ) {
                Ok(destination) => vec![(DestinationId::FIRST, destination)],
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
            // The handles given out so far, which at creation is the one
            // destination a non-manual channel opens with.
            next_destination_id: destinations.len() as u64,
            destinations,
            channel,
            channel_status_counter_id,
            receiver_id,
            dispatcher: DataPacketDispatcher::new(stream_session_limit),
            stream_refcounts: Vec::new(),
            session_refcounts: Vec::new(),
            socket_rcvbuf: params.socket_rcvbuf,
            socket_sndbuf: params.socket_sndbuf,
            group_tag,
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
        group_tag: Option<i64>,
        transport: Box<dyn Transport>,
        receiver_id: i64,
        stream_session_limit: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
        now_ns: i64,
        interceptors: &[super::interceptor::Interceptor],
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
                // A transport a caller built was not bound by the manager, so
                // there is no port for it to hold — the tests' seam.
                0,
                transport,
                counters,
                regions,
                registration_id,
                channel_status_counter_id,
                now_ms,
                now_ns,
                interceptors,
            ) {
                Ok(destination) => vec![(DestinationId::FIRST, destination)],
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
            next_destination_id: destinations.len() as u64,
            destinations,
            channel,
            channel_status_counter_id,
            receiver_id,
            dispatcher: DataPacketDispatcher::new(stream_session_limit),
            stream_refcounts: Vec::new(),
            session_refcounts: Vec::new(),
            socket_rcvbuf: 0,
            socket_sndbuf: 0,
            group_tag,
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

    /// Whether a periodic ask for a `SETUP` is worth sending again
    /// (`aeron_receive_channel_endpoint_should_elicit_setup_message`,
    /// `media/aeron_receive_channel_endpoint.h:311-314`, which is the
    /// dispatcher's own answer).
    ///
    /// The receiver asks this before it re-sends, and only before it
    /// **re**-sends: the first ask a destination with a `control=` gets is
    /// unconditional (`aeron_receive_channel_endpoint_add_pending_setup_destination`,
    /// `:1127-1152`).
    pub fn should_elicit_setup_message(&self) -> bool {
        self.dispatcher.should_elicit_setup_message()
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
        self.incref_stream(stream_id);
    }

    /// A subscription that named a session arrived.
    ///
    /// The stream is **not** marked as reading every session — that is the
    /// whole difference from [`Self::add_subscription`], and it is what the
    /// reference's two dispatcher calls do: `on_add_subscription` marks the
    /// stream (`:839-843`) while `on_add_subscription_by_session` only names
    /// the session (`:851-855`). A response subscription is the one that
    /// arrives this way, and it reads the session its `RSP_SETUP` named and
    /// nothing else, so a `SETUP` carrying any other session on the same
    /// stream is silence rather than an image.
    ///
    /// The stream's **refcount** is still taken, because the endpoint is held
    /// for as long as the subscription is; the release path takes it back the
    /// same way (`remove_subscription_by_session` decrements both).
    pub fn add_subscription_by_session(&mut self, stream_id: i32, session_id: i32) {
        self.dispatcher
            .add_subscription_by_session(stream_id, session_id);
        self.incref_stream(stream_id);

        match self
            .session_refcounts
            .iter_mut()
            .find(|((stream, session), _)| *stream == stream_id && *session == session_id)
        {
            Some((_, count)) => *count += 1,
            None => self.session_refcounts.push(((stream_id, session_id), 1)),
        }
    }

    /// One more holder of a stream, which is what the endpoint is released on.
    fn incref_stream(&mut self, stream_id: i32) {
        match self
            .stream_refcounts
            .iter_mut()
            .find(|(id, _)| *id == stream_id)
        {
            Some((_, count)) => *count += 1,
            None => self.stream_refcounts.push((stream_id, 1)),
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
    /// Whether this destination's interceptors drop a datagram that has just
    /// come off its socket
    /// (`aeron_udp_channel_incoming_interceptor_recv_func`,
    /// `media/aeron_udp_channel_transport_bindings.c:172-185`).
    ///
    /// Asked **after** the read and before the dispatcher, which is where the
    /// reference asks it too: the interceptor chain is what the transport hands
    /// each datagram to, and a dropped one never reaches the endpoint — so it
    /// moves no counter and wakes nobody, and the peer's loss detection is what
    /// notices.
    pub fn drops(&mut self, id: DestinationId, datagram: &[u8]) -> bool {
        self.destination_mut(id)
            .is_some_and(|destination| destination.interceptors.drops(datagram))
    }

    pub fn receive_from(
        &mut self,
        id: DestinationId,
        buffers: &mut [Vec<u8>],
        datagrams: &mut crate::sys::socket::Datagrams,
    ) -> io::Result<usize> {
        match self.destination_mut(id) {
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
    /// Attach a client's destination under a handle of this endpoint's own,
    /// and answer with that handle.
    ///
    /// The handle is what the caller passes on to everything that will have to
    /// answer through this socket — the images on the endpoint, and the
    /// pending setup that asks its source for a stream.
    pub fn add_destination(&mut self, destination: ReceiveDestination) -> DestinationId {
        let id = DestinationId(self.next_destination_id);
        self.next_destination_id += 1;

        self.destinations.push((id, destination));

        id
    }

    /// Take a destination off, answering with it and its handle when there was
    /// one.
    ///
    /// A destination is identified by its channel, which is how the reference
    /// compares two (`media/aeron_receive_channel_endpoint.c:877-905`). The
    /// counters it holds are the caller's to give back, because the caller
    /// allocated them — and the handle comes back too, because it is what tells
    /// the images on this endpoint which connection to let go
    /// (`aeron_publication_image_remove_destination`,
    /// `aeron_driver_receiver.c:527`).
    ///
    /// The handle is retired here and never handed out again, so a caller that
    /// forgets to tell everyone is left with a connection that sends nothing
    /// rather than one that sends through a stranger's socket.
    pub fn remove_destination(
        &mut self,
        channel: &UdpChannel,
    ) -> Option<(DestinationId, ReceiveDestination)> {
        let index = self.destinations.iter().position(|(_, destination)| {
            destination.channel.canonical_form == channel.canonical_form
        })?;

        Some(self.destinations.swap_remove(index))
    }

    /// The destinations, in the order they were added.
    pub fn destinations(&self) -> &[(DestinationId, ReceiveDestination)] {
        &self.destinations
    }

    /// Ask a destination's source for a `SETUP`, and give back the periodic
    /// entry that keeps asking until it answers
    /// (`aeron_receive_channel_endpoint_add_pending_setup_destination`,
    /// `media/aeron_receive_channel_endpoint.c:1127-1152`).
    ///
    /// Two halves in one call, and both matter: the entry makes the ask
    /// **periodic**, and the status message sent beside it is the *first* ask —
    /// a source with nothing else to wait for hears it at once rather than a
    /// second later. Session and stream are zero, as they are there: the ask is
    /// not about a stream yet, and what it is for is the answer, a `SETUP`
    /// describing whatever the far end publishes.
    ///
    /// [`None`] for a destination whose channel named no `control=`: the sender
    /// already knows about it, and there is nothing to ask
    /// (`:1136`, the same condition).
    pub(crate) fn ask_for_setup(
        &mut self,
        endpoint_id: u64,
        destination: DestinationId,
        now_ns: i64,
    ) -> Option<crate::receiver::PendingSetup> {
        let address = self.destination(destination)?.setup_address()?;

        let setup = crate::receiver::PendingSetup {
            endpoint_id,
            destination,
            stream_id: 0,
            session_id: 0,
            control_address: Some(address),
            time_of_status_message_ns: now_ns,
        };

        // The channel that is not multicast and did name a control sends to
        // `local_control`, which is where the entry above will send too
        // (`media/aeron_receive_destination.c:117-124`).
        let _ = self.send_sm(destination, address, 0, 0, 0, 0, 0, Self::send_setup_flag());

        Some(setup)
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

    /// The descriptor of the destination at `index`, for the poller that says
    /// which of them have anything (G4-3), or `None` for a destination whose
    /// transport has no socket of its own.
    pub fn destination_descriptor(&self, index: usize) -> Option<crate::sys::socket::Descriptor> {
        self.destinations
            .get(index)
            .and_then(|(_, destination)| destination.descriptor())
    }

    /// The handle of the destination at `index`, for a caller walking the list.
    ///
    /// The position is a cursor for one pass and nothing more — a caller that
    /// keeps one is keeping something that moves (`remove_destination` is a
    /// `swap_remove`). What a caller keeps is this.
    pub fn destination_id(&self, index: usize) -> Option<DestinationId> {
        self.destinations.get(index).map(|(id, _)| *id)
    }

    /// The address this endpoint's socket is bound to, which is the channel's
    /// endpoint parameter — and what the channel status reports as local.
    ///
    /// # Errors
    ///
    /// The error from `getsockname(2)`.
    pub fn local_address(&self) -> io::Result<SocketAddr> {
        match self
            .destinations
            .first()
            .map(|(_, destination)| destination)
        {
            Some(destination) => destination.transport.local_address(),
            None => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "a channel with no destinations is bound to nothing",
            )),
        }
    }

    /// The destinations that named a control address and have gone quiet long
    /// enough for it to be resolved again
    /// (`aeron_receive_channel_endpoint_check_for_re_resolution`, `:1046-1062`).
    ///
    /// Each is `(id, name, address-it-has-now)`, and asking **stamps** it
    /// (`:1059`), so one destination is asked about once per timeout rather
    /// than once per pass.
    pub fn destinations_to_re_resolve(
        &self,
        now_ns: i64,
    ) -> Vec<(DestinationId, String, SocketAddr)> {
        self.destinations
            .iter()
            .filter(|(_, destination)| destination.re_resolution_required(now_ns))
            .filter_map(|(id, destination)| {
                destination
                    .control_name()
                    .map(|name| (*id, name.to_owned(), destination.current_control_address()))
            })
            .collect()
    }

    /// Stamp a destination as just asked about (`:1059`,
    /// `aeron_receive_destination_update_last_activity_ns`).
    pub fn mark_re_resolution_checked(&mut self, id: DestinationId, now_ns: i64) {
        if let Some((_, destination)) = self.destinations.iter_mut().find(|(key, _)| *key == id) {
            destination.on_activity(now_ns);
        }
    }

    /// A frame arrived for a destination, which is what keeps it from being
    /// asked about (`aeron_receive_channel_endpoint.c:589`, `:631`, `:659`).
    pub fn on_activity(&mut self, id: DestinationId, now_ns: i64) {
        if let Some((_, destination)) = self.destinations.iter_mut().find(|(key, _)| *key == id) {
            destination.on_activity(now_ns);
        }
    }

    /// Take the answer to a re-resolution
    /// (`aeron_receive_channel_endpoint_update_control_address`, `:1064-1073`).
    ///
    /// The id may be gone — a client can remove a destination while its name is
    /// being resolved — and that is not an error.
    pub fn on_resolution_change(&mut self, id: DestinationId, new_addr: SocketAddr) {
        if let Some((_, destination)) = self.destinations.iter_mut().find(|(key, _)| *key == id) {
            destination.update_control_address(new_addr);
        }
    }

    /// The address a status message about `source` should go
    /// (`aeron_receive_destination.c:120-129`): a group's control twin, the
    /// control address a channel named, or else the source of the data being
    /// answered.
    ///
    /// It is the **destination's** channel that answers, not the endpoint's
    /// ([`UdpChannel::control_address`], asked of the destination's own
    /// channel): a multi-destination endpoint's destinations are channels of
    /// their own, and one of them may be a group while the endpoint is not.
    pub fn control_address(&self, id: DestinationId, source: SocketAddr) -> SocketAddr {
        self.destination(id).map_or(source, |destination| {
            // A destination that named a control address answers there — and
            // that address is the one a re-resolution moves, so it is read off
            // the destination rather than off its channel
            // (`current_control_addr`, `aeron_receive_destination.c:120-129`).
            if destination.has_explicit_control {
                destination.current_control_address()
            } else {
                destination.channel.control_address(source)
            }
        })
    }

    /// Send a status message through the destination it is about
    /// (`aeron_receive_channel_endpoint_send_sm`, `:291-338`).
    ///
    /// The destination is the caller's to name, and that is the reference's
    /// shape too: it passes the `aeron_receive_destination_t` the answer
    /// belongs to (`aeron_receive_channel_endpoint_send`, `:225-257`, reached
    /// through `destination->data_paths->send_func`). A datagram arrives on
    /// **one** destination's socket and the answer has to leave through that
    /// one — a status message out of another destination's socket is a receiver
    /// reporting a position through a socket the sender never wrote to, and for
    /// a unicast publication, whose sending socket is `connect`ed, that
    /// datagram is dropped by the far end's kernel without a word.
    ///
    /// # Errors
    ///
    /// The socket's error.
    #[allow(clippy::too_many_arguments)] // the status message's fields
    pub fn send_sm(
        &mut self,
        id: DestinationId,
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

        // `:302-325`: a tag makes the frame eight bytes longer, and that length
        // is the whole of what tells the sender there is one — there is no
        // flag for it.
        let mut buffer =
            [0u8; StatusMessageFrame::LENGTH + StatusMessageFrame::OPTIONAL_GROUP_TAG_LENGTH];

        let length = match self.group_tag {
            Some(group_tag) => frame
                .write_with_group_tag(&mut buffer, flags, group_tag)
                .map(|()| buffer.len()),
            None => frame
                .write_with_flags(&mut buffer, flags)
                .map(|()| StatusMessageFrame::LENGTH),
        };

        let Some(length) = length else {
            return Ok(0);
        };

        match self.destination_mut(id) {
            Some(entry) => entry
                .transport
                .send(Some(destination), &[&buffer[..length]]),
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
    /// status message would ([`Self::send_sm`]).
    ///
    /// # Errors
    ///
    /// The socket's error.
    pub fn send_response_setup(
        &mut self,
        id: DestinationId,
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

        match self.destination_mut(id) {
            Some(entry) => entry.transport.send(Some(destination), &[&buffer]),
            None => Ok(0),
        }
    }

    /// Send a NAK through the destination that reported the gap
    /// (`aeron_receive_channel_endpoint_send_nak`, `:340-375`).
    ///
    /// The destination is the caller's to name here for the same reason a
    /// status message's is: a gap is a fact about what arrived on **one**
    /// socket, and the sender that has to fill it is the one behind that
    /// socket.
    ///
    /// # Errors
    ///
    /// The socket's error.
    #[allow(clippy::too_many_arguments)] // the frame's fields, and where it leaves through
    pub fn send_nak(
        &mut self,
        id: DestinationId,
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

        match self.destination_mut(id) {
            Some(entry) => entry.transport.send(Some(destination), &[&buffer]),
            None => Ok(0),
        }
    }

    /// Send an RTTM through the destination it measures
    /// (`aeron_receive_channel_endpoint_send_rttm`, `:377-430`).
    ///
    /// # Errors
    ///
    /// The socket's error.
    #[allow(clippy::too_many_arguments)] // the frame's fields
    pub fn send_rttm(
        &mut self,
        id: DestinationId,
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

        match self.destination_mut(id) {
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
    /// made. It goes to the connection's control address through the
    /// connection's destination, like the rest — a refusal that left through
    /// another socket is a publisher that never learns why its image is not
    /// there.
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
    #[allow(clippy::too_many_arguments)] // the frame's fields, and where it leaves through
    pub fn send_error_frame(
        &mut self,
        id: DestinationId,
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
            // reference does (`:488-489`).
            group_tag: self.group_tag.unwrap_or(0),
            error_code,
            error_length,
        };

        // `:483`: the flag is what makes the field mean anything, and an
        // endpoint with no tag leaves it clear.
        let flags = match self.group_tag {
            Some(_) => header_flags::ERR_HAS_GROUP_TAG,
            None => 0,
        };

        let mut buffer = [0u8; ErrorFrame::LENGTH + MAX_ERROR_TEXT_LENGTH as usize];
        if frame
            .write_with_flags(&mut buffer[..ErrorFrame::LENGTH], flags)
            .is_none()
        {
            return Ok(0);
        }

        buffer[ErrorFrame::LENGTH..ErrorFrame::LENGTH + reason.len()].copy_from_slice(reason);

        let length = ErrorFrame::LENGTH + reason.len();
        let sent = match self.destination_mut(id) {
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

    /// The destination a handle names, or [`None`] when it names one that has
    /// gone.
    ///
    /// A retired handle is not an error and not a reason to fall back to some
    /// other destination: it is a connection whose socket is no longer there,
    /// and what it gets is nothing.
    /// The destination an id names — what the receiver's packet path needs to
    /// ask about the channel it arrived on, which the id alone does not carry.
    pub fn destination(&self, id: DestinationId) -> Option<&ReceiveDestination> {
        self.destinations
            .iter()
            .find(|(candidate, _)| *candidate == id)
            .map(|(_, destination)| destination)
    }

    /// The same, mutably — sending is what needs it.
    fn destination_mut(&mut self, id: DestinationId) -> Option<&mut ReceiveDestination> {
        self.destinations
            .iter_mut()
            .find(|(candidate, _)| *candidate == id)
            .map(|(_, destination)| destination)
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
            let Some((id, address)) = self
                .destinations
                .get(index)
                .and_then(|(id, destination)| Some((*id, destination.setup_address()?)))
            else {
                continue;
            };

            if self
                .send_sm(
                    id,
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

    use crate::media::dispatcher::SetupInterest;

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

        fn reconnect(&mut self, _address: std::net::SocketAddr) -> std::io::Result<()> {
            Ok(())
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

        fn reconnect(&mut self, _address: std::net::SocketAddr) -> std::io::Result<()> {
            Ok(())
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
            0,
            stub(40123),
            &mut counters,
            &regions,
            7,
            0,
            1_000,
            1_000_000,
            &[],
        )
        .expect("a destination");

        let second = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40124"),
            0,
            stub(40124),
            &mut counters,
            &regions,
            8,
            0,
            1_000,
            1_000_000,
            &[],
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

    /// When the destinations below were created, so that the five-second
    /// timeout is a number and not a wall clock.
    const ACTIVITY_START_NS: i64 = 1_000_000_000;

    /// A destination whose channel named a `control=` is one whose **name** can
    /// go stale, and five seconds of nothing is what says so
    /// (`aeron_receive_destination_re_resolution_required`,
    /// `aeron_receive_destination.h:65-69`). A destination that named none has
    /// no name to ask about, however quiet it is.
    ///
    /// The answer moves where its control frames go — the ask and the answer
    /// both read it off the destination now
    /// (`current_control_addr`, `aeron_receive_destination.c:120-129`).
    #[test]
    fn only_a_quiet_destination_with_a_control_name_is_resolved_again() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut with_control = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124"),
            0,
            stub(40123),
            &mut counters,
            &regions,
            7,
            0,
            1_000,
            ACTIVITY_START_NS,
            &[],
        )
        .expect("a destination");

        let without = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40125"),
            0,
            stub(40125),
            &mut counters,
            &regions,
            7,
            0,
            1_000,
            ACTIVITY_START_NS,
            &[],
        )
        .expect("a destination");

        let inside = ACTIVITY_START_NS + RECEIVE_DESTINATION_TIMEOUT_NS;
        let past = ACTIVITY_START_NS + RECEIVE_DESTINATION_TIMEOUT_NS + 1;

        assert!(
            !with_control.re_resolution_required(inside),
            "five seconds is not enough"
        );
        assert!(with_control.re_resolution_required(past));
        assert!(
            !without.re_resolution_required(past),
            "a destination that named no control address has no name to ask about"
        );

        // A frame resets the clock (`aeron_receive_channel_endpoint.c:589`).
        with_control.on_activity(past);
        assert!(!with_control.re_resolution_required(past + RECEIVE_DESTINATION_TIMEOUT_NS));

        assert_eq!(
            Some("127.0.0.1:40124"),
            with_control.control_name(),
            "and the name asked about is the one the channel wrote"
        );

        // The answer moves the address the ask goes to, and nothing else.
        let moved: SocketAddr = "127.0.0.2:40124".parse().expect("an address");
        with_control.update_control_address(moved);
        assert_eq!(Some(moved), with_control.setup_address());

        let mut without = without;
        without.update_control_address(moved);
        assert_eq!(
            None,
            without.setup_address(),
            "a destination that named no control address is not moved by an answer"
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
            0,
            stub(40123),
            &mut counters,
            &regions,
            7,
            0,
            1_000,
            1_000_000,
            &[],
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
            0,
            stub(40125),
            &mut counters,
            &regions,
            8,
            0,
            1_000,
            1_000_000,
            &[],
        )
        .expect("a destination");

        assert!(!without.has_explicit_control());
        assert_eq!(None, without.setup_address(), "nothing to ask");
    }

    /// A subscription that named a session does **not** make the whole stream
    /// readable, which is the difference between the endpoint's two add calls
    /// and the reference's (`aeron_receive_channel_endpoint_on_add_subscription`
    /// marks the stream, `:839-843`; `..._on_add_subscription_by_session` names
    /// one session and nothing else, `:851-855`).
    ///
    /// It is a distinction with a wire consequence: a **response** subscription
    /// arrives by session, and a `SETUP` carrying any other session on the same
    /// stream has to be silence rather than an image — which is what
    /// `tests/interop/response_channel.rs::a_response_subscription_is_not_a_reader`
    /// is about, and what this build got wrong by routing the by-session add
    /// through the whole-stream one.
    #[test]
    fn a_subscription_that_named_a_session_does_not_open_the_whole_stream() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40223|control-mode=manual"),
            None,
            stub(40223),
            1,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        endpoint.add_subscription_by_session(1001, 7);

        assert_eq!(
            SetupInterest::CreateImage,
            endpoint.dispatcher_mut().on_setup(1001, 7),
            "the session it named is wanted"
        );
        assert_eq!(
            SetupInterest::None,
            endpoint.dispatcher_mut().on_setup(1001, 8),
            "and another session on the same stream is not: a response \
             subscription reads only what its RSP_SETUP named"
        );
        assert_eq!(
            SetupInterest::None,
            endpoint.dispatcher_mut().on_setup(1002, 7),
            "nor is a stream nothing subscribed to"
        );

        // The whole-stream call is the one that opens every session, and the
        // endpoint still makes it for an ordinary subscription.
        endpoint.add_subscription(1003);
        assert_eq!(
            SetupInterest::CreateImage,
            endpoint.dispatcher_mut().on_setup(1003, 9)
        );
    }

    /// An endpoint holds what it is given and reads from all of them: one on
    /// creation, and as many more as clients add.
    #[test]
    fn an_endpoint_holds_the_destinations_it_is_given() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            None,
            stub(40123),
            1,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        assert_eq!(
            0,
            endpoint.destination_count(),
            "a manual channel starts with none: its sources are the ones a client names"
        );

        let added = ReceiveDestination::attach(
            channel("aeron:udp?endpoint=127.0.0.1:40124"),
            0,
            stub(40124),
            &mut counters,
            &regions,
            8,
            endpoint.channel_status_counter_id(),
            1_000,
            1_000_000,
            &[],
        )
        .expect("a destination");

        let added_id = endpoint.add_destination(added);
        assert_eq!(1, endpoint.destination_count());
        assert_eq!(Some(added_id), endpoint.destination_id(0));

        let (removed_id, removed) = endpoint
            .remove_destination(&channel("aeron:udp?endpoint=127.0.0.1:40124"))
            .expect("the destination that was added");

        assert_eq!(
            channel("aeron:udp?endpoint=127.0.0.1:40124").canonical_form,
            removed.channel().canonical_form,
            "and it is the one that was added"
        );
        assert_eq!(
            added_id, removed_id,
            "and it answers with the handle it was given, so the caller can tell the images"
        );
        assert_eq!(0, endpoint.destination_count());
        assert!(
            endpoint
                .remove_destination(&channel("aeron:udp?endpoint=127.0.0.1:40999"))
                .is_none(),
            "a channel no destination has removes nothing"
        );
    }

    /// A hand that has gone sends nothing, and — the part that matters — it
    /// does not send through whatever destination took the slot.
    ///
    /// This is the property that makes a handle a handle: `remove_destination`
    /// is a `swap_remove`, so the *position* a removed destination had is the
    /// one the last destination moves into. Anything that kept the position
    /// would answer through a socket that never asked it anything.
    #[test]
    fn a_handle_whose_destination_is_gone_sends_through_nothing() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            None,
            stub(40123),
            1,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        let second = Sent::default();

        let first_id = endpoint.add_destination(
            ReceiveDestination::attach(
                channel("aeron:udp?endpoint=127.0.0.1:40124"),
                0,
                Box::new(Sent::default()),
                &mut counters,
                &regions,
                8,
                endpoint.channel_status_counter_id(),
                1_000,
                1_000_000,
                &[],
            )
            .expect("a destination"),
        );

        // The last one added, which is the one `swap_remove` moves into the
        // hole the first leaves.
        let last_id = endpoint.add_destination(
            ReceiveDestination::attach(
                channel("aeron:udp?endpoint=127.0.0.1:40125"),
                0,
                Box::new(second.clone()),
                &mut counters,
                &regions,
                9,
                endpoint.channel_status_counter_id(),
                1_000,
                1_000_000,
                &[],
            )
            .expect("a destination"),
        );

        let (removed, _) = endpoint
            .remove_destination(&channel("aeron:udp?endpoint=127.0.0.1:40124"))
            .expect("the first destination");
        assert_eq!(first_id, removed);
        assert_eq!(
            Some(last_id),
            endpoint.destination_id(0),
            "the destination that is left has moved into the slot the first one had"
        );

        let control = "127.0.0.1:40500".parse().expect("an address");
        let sent = endpoint
            .send_sm(first_id, control, 1_001, 42, 3, 4_096, 8_192, 0)
            .expect("nothing to send is not a failure");

        assert_eq!(0, sent);
        assert!(
            second.frames().is_empty(),
            "a handle that has been retired does not inherit the socket that moved into its slot"
        );
    }

    /// An answer leaves through the destination it is **about**, not through
    /// the endpoint's first one.
    ///
    /// The defect this pins: `send_sm` had no destination to name, so it sent
    /// through the destination at position zero whatever it was answering. A
    /// reader of a second destination therefore reported its position out of
    /// the first destination's socket — and a unicast publication's sending
    /// socket is `connect`ed, so the far end's kernel drops that datagram
    /// without a word: the publisher is never told its reader exists.
    #[test]
    fn an_answer_leaves_through_the_destination_it_is_about() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            None,
            stub(40123),
            1,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        let first = Sent::default();
        let second = Sent::default();

        let first_id = endpoint.add_destination(
            ReceiveDestination::attach(
                channel("aeron:udp?endpoint=127.0.0.1:40124"),
                0,
                Box::new(first.clone()),
                &mut counters,
                &regions,
                8,
                endpoint.channel_status_counter_id(),
                1_000,
                1_000_000,
                &[],
            )
            .expect("a destination"),
        );

        let second_id = endpoint.add_destination(
            ReceiveDestination::attach(
                channel("aeron:udp?endpoint=127.0.0.1:40125"),
                0,
                Box::new(second.clone()),
                &mut counters,
                &regions,
                9,
                endpoint.channel_status_counter_id(),
                1_000,
                1_000_000,
                &[],
            )
            .expect("a destination"),
        );

        assert_ne!(first_id, second_id, "two destinations, two handles");

        let control = "127.0.0.1:40500".parse().expect("an address");
        endpoint
            .send_sm(second_id, control, 1_001, 42, 3, 4_096, 8_192, 0)
            .expect("sent");

        assert!(
            first.frames().is_empty(),
            "the first destination's socket carried nothing"
        );
        assert_eq!(
            1,
            second.frames().len(),
            "and the second carried the answer it was about"
        );
    }

    /// The handle of the one destination an endpoint of a test's own has — a
    /// unicast channel opens with exactly one, under the first handle the
    /// endpoint gives out.
    fn only_destination(endpoint: &ReceiveChannelEndpoint) -> DestinationId {
        endpoint
            .destination_id(0)
            .expect("a unicast channel opens with one destination")
    }

    /// An endpoint of a test's own, with a transport that keeps what leaves.
    fn endpoint_that_records(
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) -> (ReceiveChannelEndpoint, Sent) {
        endpoint_that_records_with_tag(None, counters, regions)
    }

    fn endpoint_that_records_with_tag(
        group_tag: Option<i64>,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) -> (ReceiveChannelEndpoint, Sent) {
        let sent = Sent::default();

        let endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            group_tag,
            Box::new(sent.clone()),
            1234,
            16,
            counters,
            regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        (endpoint, sent)
    }

    /// A channel that named a `control=` is one the sender does not know
    /// about, so the endpoint asks — once now, and then once a second until it
    /// is answered (`aeron_driver_receiver.c:305-320`, which asks when the
    /// endpoint arrives, and `media/aeron_receive_channel_endpoint.c:1127-1152`,
    /// which is the two halves this is).
    ///
    /// It is the whole of who speaks on a dynamic channel: without it a
    /// subscription whose own channel named the control address says nothing,
    /// and the sender — which has nowhere to send until it is asked — says
    /// nothing either.
    #[test]
    fn a_channel_that_named_a_control_address_asks_for_a_setup_at_once() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let sent = Sent::default();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40124|control=127.0.0.1:40125"),
            None,
            Box::new(sent.clone()),
            1234,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        let setup = endpoint
            .ask_for_setup(9, only_destination(&endpoint), 5_000)
            .expect("a channel with a control address has something to ask");

        assert_eq!(9, setup.endpoint_id);
        assert_eq!(
            Some("127.0.0.1:40125".parse().expect("an address")),
            setup.control_address,
            "the ask goes to the control address, and so does the entry that keeps asking"
        );
        assert_eq!(0, setup.stream_id, "the ask is not about a stream yet");
        assert_eq!(0, setup.session_id);

        let frame = sent.last();
        let header = crate::protocol::FrameHeader::read(&frame).expect("a header");
        assert_eq!(
            header_flags::SM_SEND_SETUP,
            header.flags & header_flags::SM_SEND_SETUP,
            "and the first ask is sent beside the entry, not a second later"
        );
    }

    /// A channel that named no `control=` is one the sender already knows
    /// about: there is nothing to ask, and nothing is sent.
    #[test]
    fn a_channel_with_no_control_address_asks_for_nothing() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, sent) = endpoint_that_records(&mut counters, &regions);

        assert!(
            endpoint
                .ask_for_setup(9, only_destination(&endpoint), 5_000)
                .is_none()
        );
        assert!(sent.frames().is_empty(), "and nothing went out");
    }

    /// The receiver's gate reaches the dispatcher through the endpoint, and it
    /// is the *registration* that opens it — not the ask that has already gone
    /// out (`media/aeron_receive_channel_endpoint.h:311-314`).
    #[test]
    fn an_endpoint_no_subscription_registered_on_is_never_asked_again() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, _sent) = endpoint_that_records(&mut counters, &regions);

        assert!(
            !endpoint.should_elicit_setup_message(),
            "a fresh endpoint has registered no stream, so a second ask would \
             be addressed to nobody"
        );

        endpoint.dispatcher_mut().add_subscription(1_001);
        assert!(
            endpoint.should_elicit_setup_message(),
            "and a subscription that registers one opens it"
        );
    }

    /// The eight optional bytes a group tag travels in
    /// (`aeron_receive_channel_endpoint_send_sm`, `:302-325`).
    ///
    /// There is no flag for it: the frame's **length** is the whole of what
    /// tells the sender a tag is there, which is why an endpoint with no tag
    /// and an endpoint with a tag of `-1` send different frames.
    #[test]
    fn a_status_message_carries_the_group_tag_when_the_endpoint_has_one() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, sent) =
            endpoint_that_records_with_tag(Some(17), &mut counters, &regions);

        let control = "127.0.0.1:40456".parse().expect("an address");
        endpoint
            .send_sm(
                only_destination(&endpoint),
                control,
                1_001,
                42,
                3,
                4_096,
                8_192,
                0,
            )
            .expect("sent");

        let frame = sent.last();
        let header = crate::protocol::FrameHeader::read(&frame).expect("a header");
        assert_eq!(
            i32::try_from(
                StatusMessageFrame::LENGTH + StatusMessageFrame::OPTIONAL_GROUP_TAG_LENGTH
            )
            .expect("a short frame"),
            header.frame_length
        );

        let status = StatusMessageFrame::read(&frame).expect("a status message");
        assert_eq!(Some(17), status.group_tag(&frame));
        assert_eq!(42, status.session_id);
        assert_eq!(8_192, status.receiver_window);
    }

    #[test]
    fn a_status_message_of_an_endpoint_with_no_tag_is_the_short_one() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, sent) = endpoint_that_records(&mut counters, &regions);

        let control = "127.0.0.1:40456".parse().expect("an address");
        endpoint
            .send_sm(
                only_destination(&endpoint),
                control,
                1_001,
                42,
                3,
                4_096,
                8_192,
                0,
            )
            .expect("sent");

        let frame = sent.last();
        let header = crate::protocol::FrameHeader::read(&frame).expect("a header");
        assert_eq!(
            i32::try_from(StatusMessageFrame::LENGTH).expect("a short frame"),
            header.frame_length,
            "no tag, no eight bytes"
        );

        let status = StatusMessageFrame::read(&frame).expect("a status message");
        assert_eq!(None, status.group_tag(&frame));
    }

    /// And the error frame says so with a **flag** instead
    /// (`aeron_receive_channel_endpoint.c:483-489`), with the field written
    /// whether or not the flag is set.
    #[test]
    fn an_error_frame_flags_its_group_tag_when_the_endpoint_has_one() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let (mut endpoint, sent) =
            endpoint_that_records_with_tag(Some(17), &mut counters, &regions);
        let system = system_counters::System::new(&counters, &regions);
        endpoint
            .send_error_frame(
                only_destination(&endpoint),
                "127.0.0.1:40456".parse().expect("an address"),
                1_001,
                42,
                deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
                b"nope",
                &system,
            )
            .expect("sent");

        let frame = sent.last();
        let header = crate::protocol::FrameHeader::read(&frame).expect("a header");
        assert_eq!(header_flags::ERR_HAS_GROUP_TAG, header.flags);
        assert_eq!(
            17,
            ErrorFrame::read(&frame).expect("an ERR frame").group_tag
        );

        // Without one, the field is still written — and ignored, because the
        // flag is clear.
        let (mut endpoint, sent) = endpoint_that_records(&mut counters, &regions);
        let system = system_counters::System::new(&counters, &regions);
        endpoint
            .send_error_frame(
                only_destination(&endpoint),
                "127.0.0.1:40456".parse().expect("an address"),
                1_001,
                42,
                deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
                b"nope",
                &system,
            )
            .expect("sent");

        let frame = sent.last();
        let header = crate::protocol::FrameHeader::read(&frame).expect("a header");
        assert_eq!(0, header.flags);
        assert_eq!(0, ErrorFrame::read(&frame).expect("an ERR frame").group_tag);
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
                only_destination(&endpoint),
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
                only_destination(&endpoint),
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

    /// A removed destination hands back the port the manager held for it,
    /// which is what the receiver puts in
    /// [`crate::receiver::ReleasedDestination`] for the conductor to give back.
    ///
    /// The port has to travel with the removal because the conductor cannot
    /// work it out: it knows the channel the client named, and a channel that
    /// named port zero does not say which port it was given — that is the whole
    /// reason the manager exists.
    #[test]
    fn a_removed_destination_hands_back_the_port_it_was_bound_with() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = ReceiveChannelEndpoint::with_transport(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            None,
            stub(40123),
            1,
            16,
            &mut counters,
            &regions,
            7,
            1_000,
            1_000_000,
            &[],
        )
        .expect("an endpoint");

        let uri = "aeron:udp?endpoint=127.0.0.1:40124";
        let id = endpoint.add_destination(
            ReceiveDestination::attach(
                channel(uri),
                40310,
                Box::new(Sent::default()),
                &mut counters,
                &regions,
                8,
                endpoint.channel_status_counter_id(),
                1_000,
                1_000_000,
                &[],
            )
            .expect("a destination"),
        );

        let (removed_id, destination) = endpoint
            .remove_destination(&channel(uri))
            .expect("the destination it was given");

        assert_eq!(id, removed_id);
        assert_eq!(40310, destination.managed_port());

        // And a destination the manager held nothing for says zero, which is
        // not a port: the caller gives back nothing.
        let kernel_chosen = endpoint.add_destination(
            ReceiveDestination::attach(
                channel("aeron:udp?endpoint=127.0.0.1:40125"),
                0,
                Box::new(Sent::default()),
                &mut counters,
                &regions,
                9,
                endpoint.channel_status_counter_id(),
                1_000,
                1_000_000,
                &[],
            )
            .expect("a destination"),
        );

        let (removed_id, destination) = endpoint
            .remove_destination(&channel("aeron:udp?endpoint=127.0.0.1:40125"))
            .expect("the destination it was given");

        assert_eq!(kernel_chosen, removed_id);
        assert_eq!(0, destination.managed_port());
    }
}
