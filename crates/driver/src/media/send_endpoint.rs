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
//! Unicast, no destinations tracker: `current_data_addr`, which starts as the
//! channel's `remote_data` and is where a connected transport already points
//! (`aeron_send_channel_send`, `:383-414`). Multi-destination channels — the
//! `destination_tracker` branch — are P1-5.

use std::io;
use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::udp_channel::UdpChannel;
use crate::{position as counter_position, sys};

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

/// A send channel endpoint and its socket.
pub struct SendChannelEndpoint {
    /// The channel it was created for, canonical form included.
    pub channel: UdpChannel,
    /// The socket every publication on this channel sends through.
    transport: Box<dyn Transport>,
    /// The `snd-channel` counter, whose value is the endpoint's
    /// [`EndpointStatus`].
    channel_status_counter_id: i32,
    /// Where data is sent. The channel's remote address until a re-resolution
    /// moves it (P1-5).
    current_data_addr: SocketAddr,
    /// The publications reachable here, in insertion order.
    publications: Vec<PublicationDispatch>,
    /// `SO_RCVBUF` this endpoint asked for, zero meaning the driver's default
    /// — the value a later channel on the same canonical form has to agree
    /// with (`aeron_driver_conductor.c:1948-1966`).
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
    pub fn create(
        channel: UdpChannel,
        params: &TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
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

        let transport = match super::udp_transport::UdpTransport::open(
            channel.local_control,
            channel.has_explicit_endpoint.then_some(channel.remote_data),
            params,
        ) {
            Ok(transport) => transport,
            Err(error) => {
                // The counter was allocated for an endpoint that will not
                // exist; leaving it behind would be a counter nobody owns.
                counters.free(regions, channel_status_counter_id, now_ms);
                return Err(SendEndpointError::Socket(error));
            }
        };

        Ok(Self {
            current_data_addr: channel.remote_data,
            channel,
            transport: Box::new(transport),
            channel_status_counter_id,
            publications: Vec::new(),
            socket_rcvbuf: params.socket_rcvbuf,
            socket_sndbuf: params.socket_sndbuf,
        })
    }

    /// Wrap an endpoint around a transport a caller built itself, for the
    /// tests that inject loss or a stub.
    ///
    /// # Errors
    ///
    /// [`SendEndpointError::NoCounter`] when the manager is full.
    pub fn with_transport(
        channel: UdpChannel,
        transport: Box<dyn Transport>,
        params: &TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
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

        Ok(Self {
            current_data_addr: channel.remote_data,
            channel,
            transport,
            channel_status_counter_id,
            publications: Vec::new(),
            socket_rcvbuf: params.socket_rcvbuf,
            socket_sndbuf: params.socket_sndbuf,
        })
    }

    /// The channel-status counter a client reads.
    pub const fn channel_status_counter_id(&self) -> i32 {
        self.channel_status_counter_id
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

    /// Hand this endpoint's freed counter back, when the endpoint is gone
    /// (`aeron_send_channel_endpoint_delete`,
    /// `aeron-driver/src/main/c/media/aeron_send_channel_endpoint.c:268-290`).
    pub fn free_counter(
        &self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> bool {
        counters.free(regions, self.channel_status_counter_id, now_ms)
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

    /// Send datagrams through the endpoint's socket
    /// (`aeron_send_channel_send`, `:383-414`).
    ///
    /// # Errors
    ///
    /// The transport's error; back pressure is `Ok(0)`.
    pub fn send(&mut self, buffers: &[&[u8]]) -> io::Result<usize> {
        self.transport.send(Some(self.current_data_addr), buffers)
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
    /// (`aeron_driver_conductor.c:1948-1966`).
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
            .finish_non_exhaustive()
    }
}

/// Why a send endpoint could not be made.
#[derive(Debug)]
pub enum SendEndpointError {
    /// The counter manager had no room for the channel-status counter.
    NoCounter,
    /// The socket could not be opened, bound or connected.
    Socket(io::Error),
}

impl std::fmt::Display for SendEndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCounter => f.write_str("could not allocate the send channel status counter"),
            Self::Socket(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SendEndpointError {}

/// The buffer parameter two channels on one endpoint have to agree on
/// (`aeron_driver_conductor_validate_channel_against_send_channel_endpoint`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:1907-1953`).
///
/// A second channel that canonicalises to an existing endpoint is **shared**,
/// so its buffer parameters have to describe the socket that exists: the
/// reference refuses a mismatch (`:520-556`) rather than silently reusing a
/// socket with the wrong buffers.
///
/// A parameter the new channel did not name is no disagreement — it is the
/// existing socket's configuration, which is what the client agreed to by
/// naming the same channel.
///
/// Returns the parameter that disagreed, with what was asked for and what
/// exists, or `None` when both agree.
pub fn buffer_mismatch(
    channel: &UdpChannel,
    socket_rcvbuf: usize,
    socket_sndbuf: usize,
    defaults: sys::SocketBufferLengths,
) -> Option<(&'static str, u64, u64)> {
    let existing_rcvbuf = if socket_rcvbuf != 0 {
        socket_rcvbuf
    } else {
        #[allow(clippy::cast_sign_loss)] // a buffer length is not negative
        {
            defaults.rcvbuf as usize
        }
    };

    let existing_sndbuf = if socket_sndbuf != 0 {
        socket_sndbuf
    } else {
        #[allow(clippy::cast_sign_loss)]
        {
            defaults.sndbuf as usize
        }
    };

    if channel.socket_rcvbuf_length != 0 && channel.socket_rcvbuf_length != existing_rcvbuf {
        return Some((
            "so-rcvbuf",
            channel.socket_rcvbuf_length as u64,
            existing_rcvbuf as u64,
        ));
    }

    if channel.socket_sndbuf_length != 0 && channel.socket_sndbuf_length != existing_sndbuf {
        return Some((
            "so-sndbuf",
            channel.socket_sndbuf_length as u64,
            existing_sndbuf as u64,
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use deepmsg_core::buffer::AtomicBuffer;

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

    #[test]
    fn a_publication_is_reachable_by_its_stream_and_session() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let mut endpoint = SendChannelEndpoint::create(
            channel("aeron:udp?endpoint=127.0.0.1:40123"),
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            1,
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
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            1,
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
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            1,
        )
        .expect("an endpoint");

        assert_eq!(1, endpoint.send(&[b"setup"]).expect("a send"));

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
            &TransportParams::default(),
            &mut counters,
            &regions,
            7,
            1,
        )
        .expect_err("the address is taken");

        assert!(matches!(error, SendEndpointError::Socket(_)), "{error}");
        assert_eq!(
            free_before + 1,
            counters.free_list_len(),
            "the counter allocated for an endpoint that will not exist goes back"
        );
    }
}
