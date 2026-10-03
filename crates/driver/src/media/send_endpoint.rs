//! A send channel endpoint: one socket, its channel status, and the
//! publications that send through it.
//!
//! Mirrors `aeron-driver/src/main/c/media/aeron_send_channel_endpoint.c`. An
//! endpoint is shared by every publication whose channel *canonicalises* to the
//! same form (`aeron_driver_conductor.c:1961-2030`), which is why it exists as
//! a thing of its own: two publications to the same address are one socket, one
//! channel-status counter and one place for the answers to arrive.
//!
//! # What arrives at a send endpoint, and why
//!
//! A publication sends and, unlike a reply-less protocol, it also **receives**:
//! status messages (the subscriber's flow control), NAKs (loss reports), RTTMs
//! (measurement replies) and ERR frames all come back to the same socket, and
//! `aeron_send_channel_endpoint_dispatch` (`:466-513`) sorts them by the frame
//! header's type and hands each to the publication the frame names.
//!
//! # Which socket a publication sends through
//!
//! A channel with one destination sends to `current_data_addr`, which starts as
//! the channel's `remote_data` and is where a connected transport already points
//! (`aeron_send_channel_send`, `:383-414`). A multi-destination channel sends to
//! each entry of its [`DestinationTracker`] instead, and a response publication
//! sends only where it was asked to; [`SendChannelEndpoint::send_to`] is the one
//! place those answers are turned into a syscall.

use std::io;
use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::position as counter_position;
use crate::udp_channel::{ControlMode, UdpChannel};

use super::destination_tracker::{DESTINATION_TIMEOUT_NS, DestinationTracker};
use super::loss_generator::LossGenerator;
use super::{Transport, TransportParams};

/// Where an endpoint is in its life
/// (`aeron_send_channel_endpoint_status_t`,
/// `aeron-driver/src/main/c/media/aeron_send_channel_endpoint.h:32-38`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointStatus {
    /// Open and usable.
    Active,
    /// Being torn down: a channel that canonicalises here is refused
    /// (`aeron_driver_conductor.c:275-284`).
    Closing,
    /// Gone.
    Closed,
}

impl EndpointStatus {
    /// The value the channel-status counter holds while the endpoint is in
    /// this state.
    pub const fn counter_value(self) -> i64 {
        match self {
            Self::Active => counter_position::channel_status::ACTIVE,
            Self::Closing => counter_position::channel_status::CLOSING,
            Self::Closed => counter_position::channel_status::CLOSING,
        }
    }
}

/// One publication reachable through this endpoint, keyed the way the dispatch
/// map is: by stream **and** session
/// (`aeron_map_compound_key`, `aeron-client/src/main/c/collections/aeron_map.h:24`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationDispatch {
    /// The stream the publication writes.
    pub stream_id: i32,
    /// The session it carries.
    pub session_id: i32,
    /// The publication's registration id, which is what a control frame's
    /// arrival is reported against.
    pub registration_id: i64,
}

/// The first three lines of what the reference records when a **send**
/// endpoint cannot bind: `aeron_bind` set it, and two layers appended to it on
/// the way up (`aeron_socket.c:93`, `aeron_udp_channel_transport.c:151`,
/// `media/aeron_send_channel_endpoint.c:142`).
///
/// The receive side's twin is `receive_endpoint.rs::bind_report`, and the two
/// differ in exactly the ways the probe shows: the affinity is the **sender's**
/// (`0`, `media/aeron_udp_channel_transport_bindings.h:26-30`), the appending
/// function is the send endpoint's, and its line writes `uri=` with no space
/// where the receive one writes `uri = `.
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
        "unicast bind, affinity=0",
    );

    report.append(
        "aeron_send_channel_endpoint_create",
        "aeron_send_channel_endpoint.c",
        142,
        &format!("uri={}", String::from_utf8_lossy(&channel.original_uri)),
    );

    report
}

/// A send channel endpoint and its socket.
pub struct SendChannelEndpoint {
    /// The channel it was created for, canonical form included.
    pub channel: UdpChannel,
    /// The socket every publication on this channel sends through.
    transport: Box<dyn Transport>,
    /// The frames this endpoint withholds on purpose, when a test asked it to
    /// (`data_loss_generator`,
    /// `aeron-driver/src/main/c/media/aeron_send_channel_endpoint.h:73`).
    ///
    /// `None` — the driver nobody configured — is every deployment.
    data_loss_generator: Option<Box<dyn LossGenerator>>,
    /// The `snd-channel` counter, whose value is the endpoint's
    /// [`EndpointStatus`].
    channel_status_counter_id: i32,
    /// The `snd-local-sockaddr` counter, whose **key** is the address the
    /// socket was actually bound to (`aeron_send_channel_endpoint.c:211-228`).
    ///
    /// The port is the reason it exists: a channel is allowed to name port
    /// zero, and then the port the kernel chose is readable from nowhere else.
    /// A client that has to tell someone where to reply finds it here.
    local_sockaddr_counter_id: i32,
    /// The port the wildcard port manager gave this endpoint, and zero when it
    /// gave none (`aeron_wildcard_port_manager_get_managed_port`,
    /// `aeron_port_manager.c:121-163`).
    ///
    /// Zero is the ordinary case: a channel that named a port keeps it, and a
    /// channel that named none keeps the kernel's — and the kernel's port is
    /// not the manager's to take back (`free_managed_port`, `:170-173`). What
    /// a non-zero port here means is that the manager is holding it for this
    /// endpoint, and that the endpoint owes it back when it is deleted.
    ///
    /// The reference keeps the whole address for this (`endpoint->bind_addr`)
    /// and hands it back whole; the port is the only part of it the table has.
    managed_port: u16,
    /// Where data is sent: the channel's remote address, unless a
    /// re-resolution has moved it (`current_data_addr`,
    /// `aeron_send_channel_endpoint.h:52`).
    current_data_addr: SocketAddr,
    /// When the last status message for one of this endpoint's publications
    /// arrived (`time_of_last_sm_ns`, `:232` at creation, `:658` on a status
    /// message).
    ///
    /// It is how "this endpoint has no connection" is spelled: a channel with
    /// an explicit endpoint that has heard nothing for
    /// [`DESTINATION_TIMEOUT_NS`] has its name resolved again
    /// (`:754-774`).
    time_of_last_sm_ns: i64,
    /// Where this endpoint sends, when its channel has several destinations
    /// (`destination_tracker`, `aeron_send_channel_endpoint.h:62`).
    ///
    /// A unicast endpoint has none: it sends to the one address its channel
    /// named. A multi-destination one sends to each of these, and a dynamic one
    /// learns them from the status messages that come back.
    destination_tracker: Option<DestinationTracker>,
    /// The publications reachable here, in insertion order.
    publications: Vec<PublicationDispatch>,
    /// `SO_RCVBUF` this endpoint asked for, zero meaning the driver's default
    /// — the value a later channel on the same canonical form has to agree
    /// with (`aeron_driver_conductor.c:1936-1956`).
    pub socket_rcvbuf: usize,
    /// `SO_SNDBUF`, likewise.
    pub socket_sndbuf: usize,
}

impl SendChannelEndpoint {
    /// Create the endpoint: allocate its channel-status counter, open its
    /// socket, and leave the status `INITIALIZING`
    /// (`aeron_send_channel_endpoint_create`,
    /// `aeron-driver/src/main/c/media/aeron_send_channel_endpoint.c:42-235`).
    ///
    /// The counter comes first, because the reference's create allocates it
    /// before the transport and a failure to allocate is a failure to create.
    /// The caller sets the status to `ACTIVE` once the endpoint is registered
    /// — the reference does it in the conductor, right after the create
    /// (`aeron_driver_conductor.c:2013`), and a channel that is looked up
    /// before that is a channel that is not there yet.
    ///
    /// # Errors
    ///
    /// [`SendEndpointError::NoCounter`] when the manager is full, or the
    /// syscall's error when the socket cannot be opened.
    #[allow(clippy::too_many_arguments)] // one per collaborator, not one per decision
    #[allow(clippy::too_many_arguments)] // one per setting the endpoint is built with
    pub fn create(
        channel: UdpChannel,
        port_manager: &mut crate::port_manager::WildcardPortManager,
        params: &TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        connect_enabled: bool,
        now_ms: i64,
        now_ns: i64,
    ) -> Result<Self, SendEndpointError> {
        let channel_status_counter_id = counter_position::allocate_channel_status_counter(
            counters,
            regions,
            counter_position::SEND_CHANNEL_STATUS_NAME,
            counter_position::channel_type_id::SEND_CHANNEL_STATUS,
            registration_id,
            &channel.original_uri,
            now_ms,
        )
        .ok_or(SendEndpointError::NoCounter)?;

        // A multi-destination channel is **never** connected
        // (`aeron_send_channel_endpoint_create`, `:76-88`): the reference's
        // `else if` puts the connect on the branch the tracker is not on, and it
        // has to be there — a connected UDP socket has one peer, and a send to
        // any other address is refused by the kernel, which would leave every
        // destination but the channel's own address unreachable.
        //
        // The second half of that `else if` is `aeron.driver.connect`
        // (`:89`): a deployment can turn the connect off, and then the socket
        // stays unconnected and every send names its address. Same packets,
        // different socket — which is a thing a system with a firewall or a
        // strange route between the two ends wants to be able to say.
        let connect_to =
            (channel.has_explicit_endpoint && !channel.is_multi_destination() && connect_enabled)
                .then_some(channel.remote_data);

        // `aeron_send_channel_endpoint.c:115-135`: a multicast endpoint binds
        // the group's **control** twin — it is the group it joins and hears
        // NAKs on — and names `local_control` as the interface every multicast
        // send leaves by. A unicast endpoint binds its local side and the
        // interface parameter is never read.
        let bind = if channel.is_multicast {
            channel.remote_control
        } else {
            channel.local_control
        };

        // `:112-120`: the port the channel named, or the one the manager hands
        // it, or the kernel's — asked **before** the socket is opened, because
        // this is where a driver that has run out of ports refuses the
        // publication rather than binding something nobody will find.
        let bind = port_manager
            .get_managed_port(&channel, bind)
            .map_err(SendEndpointError::Port)?;
        let managed_port = bind.port();

        let transport = match super::udp_transport::UdpTransport::open(
            bind,
            Some(channel.local_control),
            connect_to,
            params,
        ) {
            Ok(transport) => transport,
            Err(error) => {
                // The counter was allocated for an endpoint that will not
                // exist; leaving it behind would be a counter nobody owns.
                counters.free(regions, channel_status_counter_id, now_ms);
                // And the port, which the manager is holding for an endpoint
                // that will not exist either: the reference gives it back in
                // the delete its `goto error` reaches
                // (`media/aeron_send_channel_endpoint.c:229-232`, whose delete
                // frees the managed port at `:278-281`).
                port_manager.free_managed_port(managed_port);
                // A bind failure is the one the reference composes a chain
                // for on this side too (`media/aeron_send_channel_endpoint.c:142`
                // and the two layers above it); anything else keeps its error.
                let error = match error {
                    super::udp_transport::OpenError::Bind(failure) => {
                        SendEndpointError::Bind(Box::new(bind_report(&channel, &failure)))
                    }
                    super::udp_transport::OpenError::Io(error) => SendEndpointError::Socket(error),
                };
                return Err(error);
            }
        };

        let destination_tracker =
            match destination_tracker_for(&channel, counters, regions, registration_id, now_ms) {
                Ok(tracker) => tracker,
                Err(error) => {
                    // The counter was allocated for an endpoint that will not
                    // exist, exactly as for a socket that would not open — and
                    // the port goes back with it.
                    counters.free(regions, channel_status_counter_id, now_ms);
                    port_manager.free_managed_port(managed_port);
                    return Err(error);
                }
            };

        let local_sockaddr_counter_id = match publish_local_sockaddr(
            channel_status_counter_id,
            &channel.original_uri,
            &transport,
            counters,
            regions,
            registration_id,
            now_ms,
        ) {
            Ok(counter_id) => counter_id,
            Err(error) => {
                if let Some(tracker) = &destination_tracker {
                    counters.free(regions, tracker.num_destinations_counter_id(), now_ms);
                }
                counters.free(regions, channel_status_counter_id, now_ms);
                return Err(error);
            }
        };

        Ok(Self {
            current_data_addr: channel.remote_data,
            time_of_last_sm_ns: now_ns,
            channel,
            transport: Box::new(transport),
            data_loss_generator: None,
            destination_tracker,
            channel_status_counter_id,
            local_sockaddr_counter_id,
            managed_port,
            publications: Vec::new(),
            socket_rcvbuf: params.socket_rcvbuf,
            socket_sndbuf: params.socket_sndbuf,
        })
    }

    /// Wrap an endpoint around a transport a caller built itself, for the
    /// tests that want a stub rather than a socket. Loss is injected into an
    /// endpoint, not under it: [`Self::set_data_loss_generator`].
    ///
    /// # Errors
    ///
    /// [`SendEndpointError::NoCounter`] when the manager is full.
    #[allow(clippy::too_many_arguments)] // one per collaborator, not one per decision
    pub fn with_transport(
        channel: UdpChannel,
        transport: Box<dyn Transport>,
        params: &TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
        now_ns: i64,
    ) -> Result<Self, SendEndpointError> {
        let channel_status_counter_id = counter_position::allocate_channel_status_counter(
            counters,
            regions,
            counter_position::SEND_CHANNEL_STATUS_NAME,
            counter_position::channel_type_id::SEND_CHANNEL_STATUS,
            registration_id,
            &channel.original_uri,
            now_ms,
        )
        .ok_or(SendEndpointError::NoCounter)?;

        let destination_tracker =
            match destination_tracker_for(&channel, counters, regions, registration_id, now_ms) {
                Ok(tracker) => tracker,
                Err(error) => {
                    counters.free(regions, channel_status_counter_id, now_ms);
                    return Err(error);
                }
            };

        let local_sockaddr_counter_id = match publish_local_sockaddr(
            channel_status_counter_id,
            &channel.original_uri,
            &*transport,
            counters,
            regions,
            registration_id,
            now_ms,
        ) {
            Ok(counter_id) => counter_id,
            Err(error) => {
                if let Some(tracker) = &destination_tracker {
                    counters.free(regions, tracker.num_destinations_counter_id(), now_ms);
                }
                counters.free(regions, channel_status_counter_id, now_ms);
                return Err(error);
            }
        };

        Ok(Self {
            current_data_addr: channel.remote_data,
            time_of_last_sm_ns: now_ns,
            channel,
            transport,
            data_loss_generator: None,
            destination_tracker,
            channel_status_counter_id,
            local_sockaddr_counter_id,
            // A transport a caller built was not bound by the manager, so
            // there is no port to give back — the tests' seam, and the one
            // place a zero here is not a kernel-chosen port.
            managed_port: 0,
            publications: Vec::new(),
            socket_rcvbuf: params.socket_rcvbuf,
            socket_sndbuf: params.socket_sndbuf,
        })
    }

    /// The channel-status counter a client reads.
    pub const fn channel_status_counter_id(&self) -> i32 {
        self.channel_status_counter_id
    }

    /// A status message arrived for one of this endpoint's publications, which
    /// is the whole of what "this endpoint has a connection" means
    /// (`aeron_send_channel_endpoint.c:658`).
    pub const fn on_status_message(&mut self, now_ns: i64) {
        self.time_of_last_sm_ns = now_ns;
    }

    /// Whether this endpoint's channel has to be resolved again
    /// (`aeron_send_channel_endpoint_check_for_re_resolution`, `:754-774`).
    ///
    /// Three conditions and a clock, in the reference's order:
    ///
    /// * a **manual** channel is the destination tracker's business, not this
    ///   endpoint's — it has no single address to reconnect (`:757-761`);
    /// * a multicast channel is left alone: its endpoint is a group, and a name
    ///   that resolved to a group is not re-resolved at all;
    /// * only a channel that **named** an endpoint has a name to ask about
    ///   (`:763`), and a response channel is not one of them (`:764`);
    /// * and only when nothing has been heard from the other side for
    ///   [`DESTINATION_TIMEOUT_NS`] (`:765`) — which is "this endpoint has no
    ///   connection", spelled as a clock.
    pub fn needs_re_resolution(&self, now_ns: i64) -> bool {
        self.channel.control_mode != ControlMode::Manual
            && !self.channel.is_multicast
            && self.channel.has_explicit_endpoint
            && self.channel.control_mode != ControlMode::Response
            && now_ns > self.time_of_last_sm_ns + DESTINATION_TIMEOUT_NS
    }

    /// What this endpoint's name is, for the resolver to be asked about it
    /// (`endpoint_name`, `:768`).
    pub fn endpoint_name(&self) -> Option<&str> {
        self.channel.endpoint_name.as_deref()
    }

    /// The address a re-resolution is measured against, so that an answer that
    /// is the same address is not a change (`:6932-6938`, the `memcmp` that
    /// decides whether anything happens at all).
    pub const fn remote_data_addr(&self) -> SocketAddr {
        self.current_data_addr
    }

    /// Take the answer (`aeron_send_channel_endpoint_resolution_change`,
    /// `:776-800`).
    ///
    /// A channel with several destinations hands it to the tracker, which
    /// matches destinations by name; a channel with one **reconnects** its
    /// transport, because that is the only thing its address was.
    ///
    /// # Errors
    ///
    /// The syscall's error when the transport cannot be reconnected.
    pub fn on_resolution_change(
        &mut self,
        endpoint_name: &str,
        new_addr: SocketAddr,
    ) -> std::io::Result<()> {
        if let Some(tracker) = self.destination_tracker.as_mut() {
            tracker.on_resolution_change(endpoint_name, new_addr);

            return Ok(());
        }

        self.transport.reconnect(new_addr)?;
        self.current_data_addr = new_addr;

        Ok(())
    }

    /// Where this endpoint sends when its channel has several destinations, so
    /// that the sender can hand an arriving status message to it
    /// (`aeron_send_channel_endpoint_on_status_message`, `:636-645`).
    ///
    /// [`None`] for a unicast channel: there is nothing to learn from a status
    /// message when there is one address, and it is the one the channel named.
    pub fn destination_tracker_mut(&mut self) -> Option<&mut DestinationTracker> {
        self.destination_tracker.as_mut()
    }

    /// The same, for the destinations an error frame is attributed to
    /// (`aeron_send_channel_endpoint_on_error`, `:688-692`).
    pub fn destination_tracker(&self) -> Option<&DestinationTracker> {
        self.destination_tracker.as_ref()
    }

    /// Write the channel status
    /// (`aeron_counter_set_release(endpoint->channel_status.value_addr, ...)`,
    /// `aeron_driver_conductor.c:2013`).
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
        counters.set_value(
            regions,
            self.channel_status_counter_id,
            status.counter_value(),
        )
    }

    /// Hand this endpoint's counters back, when the endpoint is gone
    /// (`aeron_send_channel_endpoint_delete`,
    /// `aeron-driver/src/main/c/media/aeron_send_channel_endpoint.c:250-269`,
    /// which frees the channel status, the local sockaddr and the destination
    /// count, in that order). The answer is the channel status's, which is the
    /// one callers act on.
    pub fn free_counter(
        &self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> bool {
        let channel_status = counters.free(regions, self.channel_status_counter_id, now_ms);
        counters.free(regions, self.local_sockaddr_counter_id, now_ms);
        channel_status
    }

    /// The `snd-local-sockaddr` counter a client reads to learn the address
    /// this endpoint's socket was bound to.
    pub const fn local_sockaddr_counter_id(&self) -> i32 {
        self.local_sockaddr_counter_id
    }

    /// The port the wildcard port manager is holding for this endpoint, and
    /// zero when it holds none.
    ///
    /// The registry reads this to put it on the entry beside the counters, so
    /// that the port goes back when the sender has let the endpoint go
    /// (`aeron_send_channel_endpoint_delete`, `:278-281`) — the endpoint itself
    /// is on the sender's thread by then, and the manager is not.
    pub const fn managed_port(&self) -> u16 {
        self.managed_port
    }

    /// Add a publication to the dispatch map
    /// (`aeron_send_channel_endpoint_add_publication`, `:434-445`).
    ///
    /// Returns whether the `(stream, session)` pair was free: the reference's
    /// map `put` overwrites, and a second publication for the same pair on one
    /// endpoint is a driver bug rather than a client's.
    pub fn add_publication(&mut self, dispatch: PublicationDispatch) -> bool {
        if self
            .find_publication(dispatch.stream_id, dispatch.session_id)
            .is_some()
        {
            return false;
        }

        self.publications.push(dispatch);
        true
    }

    /// Remove a publication from the dispatch map
    /// (`aeron_send_channel_endpoint_remove_publication`, `:447-456`).
    pub fn remove_publication(
        &mut self,
        stream_id: i32,
        session_id: i32,
    ) -> Option<PublicationDispatch> {
        let index = self
            .publications
            .iter()
            .position(|entry| entry.stream_id == stream_id && entry.session_id == session_id)?;

        Some(self.publications.swap_remove(index))
    }

    /// The publication a control frame names
    /// (`aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map,
    /// aeron_map_compound_key(stream_id, session_id))`, `:557-560`).
    pub fn find_publication(&self, stream_id: i32, session_id: i32) -> Option<i64> {
        self.publications
            .iter()
            .find(|entry| entry.stream_id == stream_id && entry.session_id == session_id)
            .map(|entry| entry.registration_id)
    }

    /// Every publication attached, in insertion order.
    pub fn publications(&self) -> &[PublicationDispatch] {
        &self.publications
    }

    /// How many publications send through this endpoint. The registry's
    /// reference count is this number (`aeron_driver_conductor.c:1995-2010`).
    pub fn publication_count(&self) -> usize {
        self.publications.len()
    }

    /// Where data is sent.
    pub const fn current_data_addr(&self) -> SocketAddr {
        self.current_data_addr
    }

    /// Give this endpoint a generator that withholds some of what it sends.
    ///
    /// The reference fills the slot from the driver context's supplier, which
    /// the endpoint's own create calls
    /// (`aeron_send_channel_endpoint.c:237-240`) — so it is set once, before
    /// anything sends, and `None` is the deployment nobody configured.
    pub fn set_data_loss_generator(&mut self, generator: Box<dyn LossGenerator>) {
        self.data_loss_generator = Some(generator);
    }

    /// Send datagrams through the endpoint's socket
    /// (`aeron_send_channel_send`, `:383-414`).
    ///
    /// With a generator attached, each **datagram** is offered to it first and
    /// the ones it refuses never reach the socket. They are still **counted as
    /// handed over**, which is the whole point: a sender advances `snd-pos` by
    /// what the endpoint took (`aeron_network_publication.c:555-562`), so a
    /// datagram reported as sent is one the sender believes is on the wire —
    /// and one the receiver never saw, which is what a gap is.
    ///
    /// A datagram is what the caller hands over here, not a frame: the unit is
    /// whatever `send_data` scanned out of the term buffer, and it may carry
    /// several frames (see [`crate::media::loss_generator`]).
    ///
    /// The datagrams that are kept are sent as one batch, so an injected loss
    /// costs an allocation on the way past. Only the generator's presence
    /// makes that path run at all; a driver without one sends the caller's
    /// slice untouched.
    ///
    /// # Errors
    ///
    /// The transport's error; back pressure is `Ok(0)`.
    pub fn send(
        &mut self,
        buffers: &[&[u8]],
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> io::Result<usize> {
        let address = self.current_data_addr;

        let kept: Vec<&[u8]> = {
            let Some(generator) = self.data_loss_generator.as_mut() else {
                return self.send_to_all(buffers, counters, regions, now_ns, address);
            };

            buffers
                .iter()
                .copied()
                .filter(|buffer| !generator.should_drop(address, buffer, buffer.len()))
                .collect()
        };

        let dropped = buffers.len() - kept.len();

        if dropped == 0 {
            return self.send_to_all(buffers, counters, regions, now_ns, address);
        }

        let sent = if kept.is_empty() {
            0
        } else {
            self.send_to_all(&kept, counters, regions, now_ns, address)?
        };

        Ok(sent + dropped)
    }

    /// Hand one batch to one address, and to nothing else
    /// (`aeron_send_channel_send_endpoint_address`, `:424-438`).
    ///
    /// This is the whole of a response publication's output. It has no
    /// destinations to fan out to, and it may not use the endpoint's own
    /// address: the only peer entitled to its data is the one that asked for
    /// the channel.
    ///
    /// The reference's version consults neither the loss generator nor the
    /// destination tracker, and neither does this one — the frame goes to the
    /// address it was given, or nowhere.
    ///
    /// # Errors
    ///
    /// The socket's error.
    pub fn send_to(&mut self, address: SocketAddr, buffers: &[&[u8]]) -> io::Result<usize> {
        if buffers.is_empty() {
            return Ok(0);
        }

        self.transport.send(Some(address), buffers)
    }

    /// Hand one batch to whoever this endpoint sends through
    /// (`aeron_send_channel_send`, `:410-425`).
    ///
    /// With a destination tracker the batch goes to **every** destination, and
    /// what comes back is the reference's answer: the batch size, or zero if
    /// any destination turned it away — which reads to the caller exactly like
    /// back pressure, and is meant to: nothing advanced, send it again.
    ///
    /// The generator is consulted **once**, against the channel's own remote
    /// address, before the fan-out. The reference's sits under the transport
    /// and would be consulted per destination instead. The two differ only for
    /// a multi-destination channel with loss injection configured, which is a
    /// combination no test makes: injection exists for the unicast interop
    /// tests, where there is one address.
    fn send_to_all(
        &mut self,
        buffers: &[&[u8]],
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        address: SocketAddr,
    ) -> io::Result<usize> {
        let Some(tracker) = self.destination_tracker.as_mut() else {
            if buffers.is_empty() {
                return Ok(0);
            }

            return self.transport.send(Some(address), buffers);
        };

        Ok(tracker.send(self.transport.as_mut(), buffers, counters, regions, now_ns))
    }

    /// The address this endpoint's socket is bound to, which is what the
    /// channel status reports as local.
    ///
    /// # Errors
    ///
    /// The error from `getsockname(2)`.
    pub fn local_address(&self) -> io::Result<SocketAddr> {
        self.transport.local_address()
    }

    /// The socket's receive buffer, as the kernel reports it — what the
    /// reference checks an agreed `so-rcvbuf` against
    /// (`aeron_driver_conductor.c:1936-1956`).
    ///
    /// # Errors
    ///
    /// The error from `getsockopt(2)`.
    pub fn receive_buffer_size(&self) -> io::Result<usize> {
        self.transport.receive_buffer_size()
    }

    /// The transport, for the sender's poller.
    pub fn transport_mut(&mut self) -> &mut Box<dyn Transport> {
        &mut self.transport
    }
}

impl std::fmt::Debug for SendChannelEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendChannelEndpoint")
            .field("canonical_form", &self.channel.canonical_form)
            .field("channel_status_counter_id", &self.channel_status_counter_id)
            .field("publications", &self.publications.len())
            .field(
                "data_loss_generator",
                &self.data_loss_generator.as_ref().map(|_| "attached"),
            )
            .finish_non_exhaustive()
    }
}

/// Why a send endpoint could not be made.
#[derive(Debug)]
pub enum SendEndpointError {
    /// The counter manager had no room for the channel-status counter.
    NoCounter,
    /// The socket would not bind, with the chain the reference records for a
    /// send endpoint's `EADDRINUSE` (`bind_report`).
    Bind(Box<deepmsg_cnc::error_log::ErrorReport>),
    /// The socket could not be opened or connected.
    Socket(io::Error),
    /// The wildcard port manager had no port left to give
    /// (`aeron_wildcard_port_manager_get_managed_port`, `aeron_port_manager.c:93-104`).
    ///
    /// The words are the manager's own and they travel as they are: a client
    /// that has run a driver out of ports reads them in its
    /// `RegistrationException` (`WildcardPortManagerSystemTest.java:90`).
    Port(crate::port_manager::PortError),
}

impl std::fmt::Display for SendEndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCounter => f.write_str("could not allocate the send channel status counter"),
            Self::Bind(report) => f.write_str(report.text()),
            Self::Socket(error) => write!(f, "{error}"),
            Self::Port(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SendEndpointError {}

/// Publish the address the socket was bound to, the two ways the reference
/// publishes it (`aeron_send_channel_endpoint.c:158-228`): the channel status's
/// label gains the address, and a counter of type 14 is allocated whose **key**
/// is the address.
///
/// The socket is asked rather than the channel believed. A channel may name
/// port zero — a multi-destination one usually does — and the kernel's answer
/// is the only true one.
///
/// # Errors
///
/// [`SendEndpointError::Socket`] when the socket has no address to report, or
/// [`SendEndpointError::NoCounter`] when the manager is full. The caller is
/// left holding the counter it already allocated, and frees it.
fn publish_local_sockaddr(
    channel_status_counter_id: i32,
    channel: &[u8],
    transport: &dyn Transport,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    registration_id: i64,
    now_ms: i64,
) -> Result<i32, SendEndpointError> {
    let local_sockaddr = crate::udp_channel::format_source_identity(
        transport
            .local_address()
            .map_err(SendEndpointError::Socket)?,
    )
    .map_err(|error| SendEndpointError::Socket(io::Error::other(error)))?;

    // `"%s: %.*s %.*s"` — the name, the channel, the address
    // (`aeron_position.c:229-244`), replacing the label the counter was
    // allocated with. The channel is bytes rather than text, as it is there:
    // it is whatever the client sent, and this end never decodes it.
    let mut label = format!("{}: ", counter_position::SEND_CHANNEL_STATUS_NAME).into_bytes();
    label.extend_from_slice(channel);
    label.push(b' ');
    label.extend_from_slice(local_sockaddr.as_bytes());

    counters
        .update_label(regions, channel_status_counter_id, &label)
        .ok_or(SendEndpointError::NoCounter)?;

    let counter_id = counter_position::allocate_local_sockaddr_counter(
        counters,
        regions,
        counter_position::SEND_LOCAL_SOCKADDR_NAME,
        registration_id,
        channel_status_counter_id,
        &local_sockaddr,
        now_ms,
    )
    .ok_or(SendEndpointError::NoCounter)?;

    // The value is the endpoint's state, as the channel status's is: the
    // counter being there is the news, and a reader reads the key.
    if counters
        .set_value(
            regions,
            counter_id,
            counter_position::channel_status::ACTIVE,
        )
        .is_none()
    {
        counters.free(regions, counter_id, now_ms);
        return Err(SendEndpointError::NoCounter);
    }

    Ok(counter_id)
}

/// The destination tracker a channel gets, and the `mdc-num-dest` counter that
/// goes with it (`aeron_send_channel_endpoint_create`, `:61-88`, `:182-188`).
///
/// Only a multi-destination channel has one — a channel whose control mode is
/// `manual` or `dynamic`
/// ([`crate::udp_channel::UdpChannel::is_multi_destination`]). A unicast
/// endpoint sends to the one address its channel named, and a `connect`ed
/// socket is what holds it there.
///
/// Which of the two control modes it is decides whether the destinations can
/// ever expire: a manual channel's were named by a client, so they stay
/// ([`DestinationTracker`]).
///
/// # Errors
///
/// [`SendEndpointError::NoCounter`] when the manager has no room for the
/// `mdc-num-dest` counter.
fn destination_tracker_for(
    channel: &UdpChannel,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    registration_id: i64,
    now_ms: i64,
) -> Result<Option<DestinationTracker>, SendEndpointError> {
    if !channel.is_multi_destination() {
        return Ok(None);
    }

    let counter_id = counter_position::allocate_channel_status_counter(
        counters,
        regions,
        counter_position::MDC_NUM_DESTINATIONS_NAME,
        counter_position::channel_type_id::MDC_NUM_DESTINATIONS,
        registration_id,
        &channel.original_uri,
        now_ms,
    )
    .ok_or(SendEndpointError::NoCounter)?;

    Ok(Some(DestinationTracker::new(
        ControlMode::Manual == channel.control_mode,
        DESTINATION_TIMEOUT_NS,
        counter_id,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::port_manager::{PortRange, WildcardPortManager};
    use deepmsg_core::buffer::AtomicBuffer;

    /// The manager a test that is not about ports wants: a sender's with no
    /// range set, which hands nothing out and leaves the kernel to pick — the
    /// behaviour every endpoint had before the manager existed.
    fn ports() -> WildcardPortManager {
        WildcardPortManager::sender()
    }

    #[repr(align(64))]
    struct Region(Vec<u8>);

    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            const VALUES_LENGTH: usize = 64 * 1024;
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
            let manager = CounterManager::new(64 * 1024, 1_000).expect("room");

            (manager, regions)
        }
    }

    /// When the endpoints below were created, so that the five-second timeout
    /// is a number and not a wall clock.
    const TIMEOUT_START_NS: i64 = 1_000_000_000;

    fn channel(uri: &str) -> UdpChannel {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel")
    }

    fn dispatch(stream_id: i32, session_id: i32, registration_id: i64) -> PublicationDispatch {
        PublicationDispatch {
            stream_id,
            session_id,
            registration_id,
        }
    }

    /// An endpoint's own clock: only a **unicast** channel that named an
    /// endpoint, is not a response channel, and has heard nothing for five
    /// seconds has a name to resolve again
    /// (`aeron_send_channel_endpoint_check_for_re_resolution`, `:754-774`).
    ///
    /// The four other shapes are the reference's own exclusions, and each one is
    /// a different reason: a manual channel's addresses are its destinations'
    /// (which have their own check), a multicast channel's endpoint is a group,
    /// a channel that named no endpoint has no name, and a response channel's
    /// address belongs to the stream that asked for it.
    #[test]
    fn only_a_quiet_unicast_endpoint_is_resolved_again() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let build = |counters: &mut CounterManager, regions: &CounterRegions<'_>, uri: &str| {
            SendChannelEndpoint::create(
                channel(uri),
                &mut ports(),
                &TransportParams::default(),
                counters,
                regions,
                7,
                true,
                1_000_000,
                TIMEOUT_START_NS,
            )
            .expect("an endpoint")
        };

        let mut unicast = build(
            &mut counters,
            &regions,
            "aeron:udp?endpoint=127.0.0.1:40123",
        );
        let manual = build(
            &mut counters,
            &regions,
            "aeron:udp?endpoint=127.0.0.1:40124|control-mode=manual",
        );
        let multicast = build(
            &mut counters,
            &regions,
            "aeron:udp?endpoint=224.0.1.1:40125",
        );
        let response = build(
            &mut counters,
            &regions,
            "aeron:udp?endpoint=127.0.0.1:40126|control-mode=response",
        );
        let unaddressed = build(
            &mut counters,
            &regions,
            "aeron:udp?control=127.0.0.1:40127|control-mode=manual",
        );

        let just_inside = TIMEOUT_START_NS + DESTINATION_TIMEOUT_NS;
        let just_past = TIMEOUT_START_NS + DESTINATION_TIMEOUT_NS + 1;

        assert!(
            !unicast.needs_re_resolution(just_inside),
            "five seconds is not enough"
        );
        assert!(
            unicast.needs_re_resolution(just_past),
            "and five seconds and a nanosecond is"
        );

        assert!(
            !manual.needs_re_resolution(just_past),
            "manual is the tracker's"
        );
        assert!(
            !multicast.needs_re_resolution(just_past),
            "a group has no name to ask about"
        );
        assert!(
            !response.needs_re_resolution(just_past),
            "a response channel is not one"
        );
        assert!(
            !unaddressed.needs_re_resolution(just_past),
            "a channel with destinations is the tracker's business too"
        );

        // And a status message is what "it is connected" means: the clock
        // starts again from it (`aeron_send_channel_endpoint.c:658`).
        unicast.on_status_message(just_past);
        assert!(
            !unicast.needs_re_resolution(just_past + DESTINATION_TIMEOUT_NS),
            "ten seconds after a status message is five seconds after one"
        );
        assert!(unicast.needs_re_resolution(just_past + DESTINATION_TIMEOUT_NS + 1));
    }

    /// The answer to a re-resolution reaches a **unicast** endpoint by
    /// reconnecting its transport, which is a thing a socket can be asked about
    /// (`aeron_send_channel_endpoint_resolution_change`, `:776-800`).
    #[test]
    fn an_answer_moves_a_unicast_endpoint() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1_000_000,
            TIMEOUT_START_NS,
        )
        .expect("an endpoint");

        let moved: SocketAddr = "127.0.0.2:40123".parse().expect("an address");
        endpoint
            .on_resolution_change("somewhere:40123", moved)
            .expect("the transport reconnects");

        assert_eq!(moved, endpoint.remote_data_addr());
        assert_eq!(
            Some("127.0.0.1:40123"),
            endpoint.endpoint_name(),
            "and the name it was parsed with is kept, which is what a re-resolution asks about"
        );
    }

    #[test]
    fn a_publication_is_reachable_by_its_stream_and_session() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        assert!(endpoint.add_publication(dispatch(1001, 99, 42)));
        assert!(
            !endpoint.add_publication(dispatch(1001, 99, 43)),
            "one stream and session, one publication"
        );

        // Another stream on the same endpoint is a different entry, and the
        // same stream with another session is a third.
        assert!(endpoint.add_publication(dispatch(1002, 99, 44)));
        assert!(endpoint.add_publication(dispatch(1001, 100, 45)));

        assert_eq!(Some(42), endpoint.find_publication(1001, 99));
        assert_eq!(Some(44), endpoint.find_publication(1002, 99));
        assert_eq!(None, endpoint.find_publication(1001, 101));
        assert_eq!(3, endpoint.publication_count());

        assert_eq!(
            Some(42),
            endpoint
                .remove_publication(1001, 99)
                .map(|entry| entry.registration_id)
        );
        assert_eq!(None, endpoint.find_publication(1001, 99));
    }

    #[test]
    fn an_endpoints_status_is_the_counter_a_client_reads() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        assert_eq!(
            Some(counter_position::channel_status::INITIALIZING),
            counters.value(&regions, endpoint.channel_status_counter_id())
        );

        endpoint
            .set_status(&counters, &regions, EndpointStatus::Active)
            .expect("in range");

        assert_eq!(
            Some(counter_position::channel_status::ACTIVE),
            counters.value(&regions, endpoint.channel_status_counter_id())
        );
    }

    /// A socket bound to a port the kernel picked, with the address it got.
    fn bound_listener() -> crate::sys::socket::DatagramSocket {
        let socket = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        socket
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        socket.set_nonblocking().expect("non-blocking");

        socket
    }

    #[test]
    fn what_an_endpoint_sends_leaves_its_socket() {
        // The endpoint is bound to the channel's local control address — the
        // wildcard, so the kernel picks — and sends to the endpoint
        // parameter. A plain socket bound to that address is the other end.
        let listener = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        listener
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        listener.set_nonblocking().expect("non-blocking");
        let bound = listener.local_address().expect("a bound address");

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel(&format!("aeron:udp?endpoint={bound}")),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        assert_eq!(
            1,
            endpoint
                .send(&[b"setup"], &counters, &regions, 0)
                .expect("a send")
        );

        let mut buffers = vec![vec![0u8; 1408]];
        let mut datagrams = crate::sys::socket::Datagrams::new();
        assert_eq!(
            1,
            listener
                .receive_batch(&mut buffers, &mut datagrams)
                .expect("a receive")
        );
        assert_eq!(b"setup", &buffers[0][..5]);
    }

    /// `aeron.driver.connect=false`: the socket is **not** connected, so a
    /// datagram can be sent somewhere the channel never named — which is what
    /// the setting is for (`aeron_send_channel_endpoint.c:89`) and what a
    /// connected socket refuses.
    #[test]
    fn an_endpoint_that_is_not_connected_can_send_somewhere_else() {
        let named = bound_listener().local_address().expect("a bound address");
        let elsewhere = bound_listener();
        let elsewhere_address = elsewhere.local_address().expect("a bound address");

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel(&format!("aeron:udp?endpoint={named}")),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            false,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        assert_eq!(
            1,
            endpoint
                .send_to(elsewhere_address, &[b"elsewhere"])
                .expect("a send")
        );

        let mut buffers = vec![vec![0_u8; 1408]];
        let mut datagrams = crate::sys::socket::Datagrams::new();
        assert_eq!(
            1,
            elsewhere
                .receive_batch(&mut buffers, &mut datagrams)
                .expect("a receive"),
            "an unconnected socket reaches an address the channel did not name"
        );
        assert_eq!(b"elsewhere", &buffers[0][..9]);
    }

    /// The same send with `aeron.driver.connect` left at its default: the
    /// socket is connected to the one address the channel named, and a
    /// datagram does not arrive anywhere else.
    ///
    /// Not asserted as an error: what the kernel does with an address handed
    /// to `sendto` on a connected socket is its business — it may refuse it or
    /// hand it to the connected peer — and what the setting is *for* is where
    /// the packets can go.
    #[test]
    fn a_connected_endpoint_sends_only_where_its_channel_points() {
        let named = bound_listener();
        let named_address = named.local_address().expect("a bound address");
        let elsewhere = bound_listener();
        let elsewhere_address = elsewhere.local_address().expect("a bound address");

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel(&format!("aeron:udp?endpoint={named_address}")),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        assert_eq!(
            1,
            endpoint
                .send(&[b"where it points"], &counters, &regions, 0)
                .expect("a send"),
            "the channel's own address is where this goes"
        );

        let mut buffers = vec![vec![0_u8; 1408]];
        let mut datagrams = crate::sys::socket::Datagrams::new();
        assert_eq!(
            1,
            named
                .receive_batch(&mut buffers, &mut datagrams)
                .expect("a receive")
        );
        assert_eq!(b"where it points", &buffers[0][..15]);

        let _ = endpoint.send_to(elsewhere_address, &[b"elsewhere"]);
        let mut buffers = vec![vec![0_u8; 1408]];
        let mut datagrams = crate::sys::socket::Datagrams::new();
        let arrived = elsewhere.receive_batch(&mut buffers, &mut datagrams);

        assert!(
            matches!(arrived, Ok(0))
                || arrived
                    .as_ref()
                    .err()
                    .is_some_and(|error| io::ErrorKind::WouldBlock == error.kind()),
            "nothing reaches an address the channel did not name: {arrived:?}"
        );
    }

    #[test]
    fn a_withheld_datagram_is_handed_over_without_leaving_the_socket() {
        // This is where the gap a receiver sees comes from: a datagram the
        // generator refuses still counts as handed over, so the sender
        // advances `snd-pos` past it and never sends it again — and the
        // datagram that is missing on the far side is what a NAK comes back
        // for.
        let listener = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        listener
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        listener.set_nonblocking().expect("non-blocking");
        let bound = listener.local_address().expect("a bound address");

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel(&format!("aeron:udp?endpoint={bound}")),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        endpoint.set_data_loss_generator(Box::new(crate::media::EveryNthDatagram::new(3)));

        for marker in 0..6u8 {
            assert_eq!(
                1,
                endpoint
                    .send(&[&[marker]], &counters, &regions, 0)
                    .expect("a send"),
                "a withheld datagram is still one the endpoint took"
            );
        }

        let mut buffers = vec![vec![0u8; 1408]; 8];
        let mut datagrams = crate::sys::socket::Datagrams::new();
        let received = listener
            .receive_batch(&mut buffers, &mut datagrams)
            .expect("a receive");

        assert_eq!(
            4, received,
            "the third datagram and the sixth are the refused ones"
        );

        let arrived: Vec<u8> = buffers[..received].iter().map(|buffer| buffer[0]).collect();
        assert_eq!(vec![0, 1, 3, 4], arrived);
    }

    /// The file descriptor the report's second line names, read back out of it:
    /// the descriptor is the kernel's, so a test cannot know it in advance.
    fn report_fd(report: &deepmsg_cnc::error_log::ErrorReport) -> String {
        report
            .text()
            .lines()
            .find_map(|line| line.strip_prefix("[aeron_bind, aeron_socket.c:93] failed to bind("))
            .and_then(|rest| rest.split(',').next())
            .unwrap_or_default()
            .to_owned()
    }

    #[test]
    fn a_socket_that_cannot_be_made_leaves_no_counter_behind() {
        // A channel with an explicit control address binds *that* address
        // (`aeron_udp_channel.c:442-464`), so one already in use is a bind
        // failure — the case the create's cleanup exists for.
        let taken = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        taken
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        let bound = taken.local_address().expect("a bound address");

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let free_before = counters.free_list_len();

        let error = SendChannelEndpoint::create(
            channel(&format!(
                "aeron:udp?endpoint=127.0.0.1:40123|control={bound}"
            )),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect_err("the address is taken");

        // The chain the reference records for this: the errno, where it was
        // set, the two layers that appended to it — and the sender's affinity,
        // which is where this differs from the receive side's twin
        // (`receive_endpoint.rs::bind_report`).
        let SendEndpointError::Bind(report) = error else {
            panic!("a bind failure is a bind report, not {error}");
        };

        assert!(
            report.text().starts_with(&format!(
                "(98) Address already in use\n[aeron_bind, aeron_socket.c:93] failed to bind({}, {bound})\n",
                report_fd(&report),
            )),
            "{}",
            report.text()
        );
        assert!(
            report.text().contains(
                "[aeron_udp_channel_transport_init, aeron_udp_channel_transport.c:151] unicast bind, affinity=0"
            ),
            "{}",
            report.text()
        );
        assert!(
            report.text().contains(&format!(
                "[aeron_send_channel_endpoint_create, aeron_send_channel_endpoint.c:142] uri=aeron:udp?endpoint=127.0.0.1:40123|control={bound}"
            )),
            "{}",
            report.text()
        );

        assert_eq!(
            free_before + 1,
            counters.free_list_len(),
            "the counter allocated for an endpoint that will not exist goes back"
        );
    }

    /// The port the manager names is the port the socket binds, and a socket
    /// that will not bind gives it back.
    ///
    /// Both halves are proved by the same trick: the port is taken first, so a
    /// `create` that ignored the manager's answer would bind a **free**
    /// kernel-chosen port and succeed. It does not — it reports `EADDRINUSE`
    /// for the managed port — and once the port is free again the same manager
    /// hands it out a second time, which it could only do if the failed
    /// create had given it back.
    #[test]
    fn the_managed_port_is_the_one_the_socket_binds_and_a_failure_gives_it_back() {
        let taken = crate::sys::socket::DatagramSocket::open(crate::sys::AddressFamily::Inet)
            .expect("a socket");
        taken
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        let managed = taken.local_address().expect("a bound address").port();

        let mut manager = WildcardPortManager::sender();
        manager.set_range(PortRange {
            low: managed,
            high: managed,
        });

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let error = SendChannelEndpoint::create(
            channel("aeron:udp?control=127.0.0.1:0|control-mode=dynamic"),
            &mut manager,
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect_err("the managed port is taken");

        let SendEndpointError::Bind(report) = error else {
            panic!("a bind failure is a bind report, not {error}");
        };

        assert!(
            report.text().contains(&format!(
                "] failed to bind({}, 127.0.0.1:{managed})\n",
                report_fd(&report)
            )),
            "the socket tried to bind the managed port: {}",
            report.text()
        );
        assert_eq!(98, report.code());

        // The probe goes, and the port comes back round — which is the free in
        // the create's error path (`aeron_send_channel_endpoint_delete`,
        // `:278-281`, reached by the create's `goto error`).
        drop(taken);

        let endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?control=127.0.0.1:0|control-mode=dynamic"),
            &mut manager,
            &TransportParams::default(),
            &mut counters,
            &regions,
            8,
            true,
            2,
            2_000_000,
        )
        .expect("the port came back");

        assert_eq!(managed, endpoint.managed_port());
    }

    /// A unicast endpoint has nowhere to fan out to: it sends to the one
    /// address its channel named
    /// (`aeron_send_channel_endpoint_create`, `:76-88`).
    #[test]
    fn a_unicast_endpoint_has_no_destinations() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        assert!(endpoint.destination_tracker().is_none());
        assert!(endpoint.destination_tracker_mut().is_none());
    }

    /// A multi-destination one does, and with the counter that counts them
    /// (`:182-188`).
    #[test]
    fn a_multi_destination_endpoint_counts_its_destinations() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        let counter_id = endpoint
            .destination_tracker()
            .expect("a multi-destination channel has one")
            .num_destinations_counter_id();

        assert_eq!(Some(0), counters.value(&regions, counter_id), "none yet");

        endpoint
            .destination_tracker_mut()
            .expect("a tracker")
            .manual_add(
                &counters,
                &regions,
                0,
                channel("aeron:udp?endpoint=127.0.0.1:40124"),
                Some("127.0.0.1:40124".parse().expect("an address")),
                42,
            );

        assert_eq!(Some(1), counters.value(&regions, counter_id));
    }

    /// The datagram goes to the destinations, not to the address the channel
    /// named (`aeron_send_channel_send`, `:410-418`).
    ///
    /// The destination here is a socket the test holds, so what arrives can be
    /// read back. That the channel's own address is a *different* socket is the
    /// point: a unicast endpoint would have sent there.
    #[test]
    fn a_multi_destination_endpoint_sends_to_its_destinations() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let listener = std::net::UdpSocket::bind("127.0.0.1:0").expect("a socket");
        listener.set_nonblocking(true).expect("non-blocking");
        let destination = listener.local_addr().expect("a bound address");

        let mut endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual"),
            &mut ports(),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        endpoint
            .destination_tracker_mut()
            .expect("a tracker")
            .manual_add(
                &counters,
                &regions,
                0,
                channel("aeron:udp?endpoint=127.0.0.1:40124"),
                Some(destination),
                42,
            );

        let sent = endpoint
            .send(&[b"payload"], &counters, &regions, 0)
            .expect("a send");

        let mut buffer = [0u8; 64];
        let (length, _) = listener.recv_from(&mut buffer).expect("the datagram");

        assert_eq!(b"payload", &buffer[..length]);
        assert_eq!(1, sent, "the batch was handed over");
    }
}
