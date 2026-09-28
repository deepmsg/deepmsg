//! The receive channel endpoints a driver owns, one per channel that shares.
//!
//! Mirrors the conductor's other half of
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! `get_or_add_receive_channel_endpoint` (`:2046-2115`) and the lookups that
//! decide whether a subscription joins an endpoint that exists
//! (`find_existing_receive_channel_endpoint`, `:288-340`).
//!
//! The sharing rule is the send side's, for the same reason: two subscriptions
//! to one channel are one socket, and a driver that made two would read every
//! datagram twice and answer each of them twice. The difference is what the
//! *channel* means on this side. A send endpoint binds the interface; a receive
//! endpoint binds the endpoint parameter — so `aeron:udp?endpoint=localhost:40123`
//! and `aeron:udp?endpoint=127.0.0.1:40123` are one receiving socket, and a
//! subscription that names `localhost:40124` is a *different* one even though
//! it is the same publisher's port as its counterpart's.
//!
//! # Reference counts
//!
//! An endpoint lives while something reads through it: the count here is
//! subscriptions, and the images an endpoint serves are counted separately —
//! an endpoint with no subscription but a live image is still an endpoint a
//! client is reading (`image_ref_count`,
//! `aeron-driver/src/main/c/media/aeron_receive_channel_endpoint.h:52-56`).

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::media::receive_endpoint::{
    EndpointStatus, ReceiveChannelEndpoint, ReceiveEndpointError,
};
use crate::sys;
use crate::udp_channel::UdpChannel;

/// One endpoint, as the conductor sees it.
#[derive(Debug)]
pub struct ReceiveChannelEndpointEntry {
    /// The id the receiver thread knows this endpoint by.
    pub id: u64,
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// The `rcv-channel` counter whose value is its state.
    pub channel_status_counter_id: i32,
    /// Where it is in its life.
    pub status: EndpointStatus,
    /// How many subscriptions read through it.
    pub refcount: i32,
    /// How many images it serves (`image_ref_count`).
    pub image_refcount: i32,
    /// Whether the receiver thread has already let it go.
    pub receiver_released: bool,
}

/// Why a receive endpoint could not be had.
#[derive(Debug)]
pub enum ReceiveEndpointErrorKind {
    /// The counter manager is full.
    NoCounter,
    /// The socket could not be opened or bound — most often because another
    /// process already holds the port, which for a subscriber is the ordinary
    /// case of two clients naming the same channel.
    Socket(std::io::Error),
}

impl std::fmt::Display for ReceiveEndpointErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCounter => f.write_str("could not allocate the receive channel status counter"),
            Self::Socket(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReceiveEndpointErrorKind {}

/// The endpoints a driver receives through, and the receiver ids they hold.
#[derive(Debug, Default)]
pub struct ReceiveChannelEndpoints {
    entries: Vec<ReceiveChannelEndpointEntry>,
    next_id: u64,
    /// The id the next endpoint introduces itself with
    /// (`context->next_receiver_id++`,
    /// `aeron-driver/src/main/c/media/aeron_receive_channel_endpoint.c:96`).
    next_receiver_id: i64,
}

impl ReceiveChannelEndpoints {
    /// No endpoints, and the first receiver id.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
            next_receiver_id: 0,
        }
    }

    /// The endpoints, in the order they were created.
    pub fn entries(&self) -> &[ReceiveChannelEndpointEntry] {
        &self.entries
    }

    /// The entry for an id.
    pub fn get(&self, id: u64) -> Option<&ReceiveChannelEndpointEntry> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// The mutable entry for an id.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut ReceiveChannelEndpointEntry> {
        self.entries.iter_mut().find(|entry| entry.id == id)
    }

    /// The endpoint a channel canonicalises to, if there is one
    /// (`find_existing_receive_channel_endpoint`, `:288-340`).
    pub fn find(&self, channel: &UdpChannel) -> Option<u64> {
        self.entries
            .iter()
            .find(|entry| entry.channel.canonical_form == channel.canonical_form)
            .map(|entry| entry.id)
    }

    /// Create the endpoint for a channel, or find the one it shares.
    ///
    /// # Errors
    ///
    /// The socket's error, when one has to be opened and cannot be.
    #[allow(clippy::too_many_arguments)] // the collaborators a create needs
    pub fn get_or_add(
        &mut self,
        channel: UdpChannel,
        params: &crate::media::TransportParams,
        config: &crate::config::DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
    ) -> Result<(u64, i32, Option<Box<ReceiveChannelEndpoint>>), ReceiveEndpointErrorKind> {
        if let Some(id) = self.find(&channel) {
            let entry = self.get(id).expect("just found");

            return Ok((id, entry.channel_status_counter_id, None));
        }

        let receiver_id = self.next_receiver_id;
        self.next_receiver_id += 1;

        let endpoint = ReceiveChannelEndpoint::create(
            channel,
            params,
            receiver_id,
            config.stream_session_limit,
            counters,
            regions,
            registration_id,
            now_ms,
        )
        .map_err(|error| match error {
            ReceiveEndpointError::NoCounter => ReceiveEndpointErrorKind::NoCounter,
            ReceiveEndpointError::Socket(error) => ReceiveEndpointErrorKind::Socket(error),
        })?;

        // The same status the send side writes, for the same reason: a
        // subscription's counter is how a client learns its socket is up
        // (`aeron_driver_conductor.c:2160`).
        endpoint.set_status(counters, regions, EndpointStatus::Active);

        let id = self.next_id;
        self.next_id += 1;

        let channel_status_counter_id = endpoint.channel_status_counter_id();

        self.entries.push(ReceiveChannelEndpointEntry {
            id,
            channel: endpoint.channel.clone(),
            channel_status_counter_id,
            status: EndpointStatus::Active,
            refcount: 0,
            image_refcount: 0,
            receiver_released: false,
        });

        Ok((id, channel_status_counter_id, Some(Box::new(endpoint))))
    }

    /// A subscription joined an endpoint.
    pub fn attach_subscription(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.refcount += 1;
        }
    }

    /// A subscription left. Returns whether the endpoint is now idle — no
    /// subscriptions *and* no images, which is when it may be released
    /// (`try_remove_endpoint`).
    pub fn detach_subscription(&mut self, id: u64) -> bool {
        let Some(entry) = self.get_mut(id) else {
            return false;
        };

        entry.refcount -= 1;

        entry.refcount <= 0 && entry.image_refcount <= 0
    }

    /// An image was added to an endpoint.
    pub fn attach_image(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.image_refcount += 1;
        }
    }

    /// An image left an endpoint.
    pub fn detach_image(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.image_refcount -= 1;
        }
    }

    /// Forget an endpoint both sides have let go.
    pub fn remove(&mut self, id: u64) -> Option<ReceiveChannelEndpointEntry> {
        let index = self.entries.iter().position(|entry| entry.id == id)?;

        Some(self.entries.swap_remove(index))
    }

    /// The socket buffer lengths a driver opens a receive endpoint with, from
    /// its settings and the channel's parameters.
    pub fn transport_params(
        config: &crate::config::DriverConfig,
        channel: &UdpChannel,
    ) -> crate::media::TransportParams {
        crate::media::TransportParams {
            socket_rcvbuf: if channel.socket_rcvbuf_length != 0 {
                channel.socket_rcvbuf_length
            } else {
                usize::try_from(config.socket_so_rcvbuf).unwrap_or(0)
            },
            socket_sndbuf: if channel.socket_sndbuf_length != 0 {
                channel.socket_sndbuf_length
            } else {
                usize::try_from(config.socket_so_sndbuf).unwrap_or(0)
            },
            ttl: 0,
        }
    }

    /// The kernel's default socket buffers, for a caller comparing a channel's
    /// parameters against them. Zeroes when the kernel cannot be asked.
    pub fn os_defaults() -> sys::SocketBufferLengths {
        sys::default_socket_buffers().unwrap_or(sys::SocketBufferLengths {
            rcvbuf: 0,
            sndbuf: 0,
        })
    }
}
