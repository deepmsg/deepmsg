//! UDP transport layer and name resolution.
//!
//! Mirrors `aeron-driver/src/main/c/media/` (M12), in the reference's own
//! two-part shape: a set of **bindings** — a vtable there
//! (`aeron_udp_channel_transport_bindings.h:110-126`), a trait here — and an
//! implementation of them over the kernel's own calls
//! (`aeron_udp_channel_transport.c`).
//!
//! # Why the seam exists at all
//!
//! The reference keeps the send and receive path behind function pointers so
//! that a kernel-bypass transport can replace it without the endpoints, the
//! publications or the images knowing. Two things fall out of that seam that
//! this build uses before any kernel-bypass implementation exists:
//!
//! * **The loss injector.** A test that has to prove retransmission works
//!   cannot wait for a real network to lose a datagram — loopback does not —
//!   so the reference ships generators that drop them on purpose and so does
//!   this ([`loss_generator`]). They hang on the endpoints, which is where the
//!   reference hangs them, and not on a transport.
//! * **One place to be wrong about a syscall.** Every socket call in the
//!   driver goes through [`udp_transport`], which is the only module above
//!   [`crate::sys`] that touches one.
//!
//! # What this build carries
//!
//! One file descriptor per transport, or two where a multicast channel both
//! joins and sends — the reference's `fd`/`recv_fd` pair (`:141-164`), which
//! [`udp_transport::UdpTransport`] describes. The interceptors and the
//! transport-level timestamps are still out, and are refused at the channel
//! rather than ignored (`docs/compat.md`). The poller is the linear one: the
//! reference switches to `epoll`/`poll` above five transports
//! (`aeron_udp_transport_poller.c:183-196`, `aeron_udp_transport_poller.h:22`),
//! a threshold this build's channels do not reach on any machine it is tested
//! on, so the switch is left out rather than written and unproven.

use std::io;
use std::net::SocketAddr;

pub mod destination_tracker;
pub mod dispatcher;
pub mod loss_generator;
pub mod poller;
pub mod receive_endpoint;
pub mod send_endpoint;
pub mod udp_transport;

pub use crate::sys::socket::Datagrams;
pub use loss_generator::{EveryNthDatagram, LossGenerator};
pub use send_endpoint::SendChannelEndpoint;
pub use udp_transport::UdpTransport;

/// The knobs a transport is opened with
/// (`aeron_udp_channel_transport_params_t`,
/// `aeron-driver/src/main/c/media/aeron_udp_channel_transport.h:39-47`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportParams {
    /// `SO_RCVBUF`, in bytes; zero leaves the kernel's default.
    pub socket_rcvbuf: usize,
    /// `SO_SNDBUF`, in bytes; zero leaves the kernel's default.
    pub socket_sndbuf: usize,
    /// The channel's interface index, which the IPv6 multicast options take in
    /// place of an address (`aeron_udp_channel_transport.c:213-227`, `:230-235`).
    pub multicast_if_index: u32,
    /// The `ttl=` parameter, for the sockets that honour it
    /// (`0 != channel->multicast_ttl ? channel->multicast_ttl : context->multicast_ttl`,
    /// `media/aeron_send_channel_endpoint.c:129`). Zero leaves the kernel's
    /// own hop limit, because the reference and the channel agree on zero
    /// meaning "not set".
    pub ttl: u8,
}

/// What the endpoints, the publications and the images are allowed to ask of
/// whatever is underneath them
/// (`aeron_udp_channel_transport_bindings_t`, `aeron_udp_channel_transport_bindings.h:110-126`).
///
/// The methods are the ones this slice calls; `reconnect` and the poller's
/// add/remove arrive with the features that need them (a re-resolving address,
/// and the epoll poller).
///
/// `Send` because an endpoint is created on the conductor and then *moved* to
/// the sender or the receiver, which is where its socket lives from then on
/// (`aeron-driver/src/main/c/aeron_driver_conductor.c:2015` hands it over the
/// same way).
pub trait Transport: Send {
    /// Send up to sixteen datagrams, one per buffer, and answer with how many
    /// left the machine.
    ///
    /// `address` is where an unconnected transport sends; a connected one
    /// ignores it and uses the address it was connected to, which is what
    /// `aeron_udp_channel_transport_send` decides between
    /// (`aeron_udp_channel_transport.c:818-902`).
    ///
    /// # Errors
    ///
    /// An error the caller should record. Back pressure is **not** one: a
    /// full socket buffer reads as `Ok(0)`, because a datagram that did not
    /// leave is a datagram the publication still has to send.
    fn send(&mut self, address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize>;

    /// The descriptor a [`crate::media::poller::TransportPoller`] watches for
    /// this transport's readability, or `None` for a transport that has no
    /// socket of its own — a test's, or one that is a buffer.
    ///
    /// The reference's poller keeps the same thing per transport
    /// (`transport->recv_fd`, `media/aeron_udp_transport_poller.c:157`).
    fn descriptor(&self) -> Option<crate::sys::socket::Descriptor> {
        None
    }

    /// Receive everything queued into `buffers`, up to their number, and
    /// describe what arrived in `datagrams`.
    ///
    /// # Errors
    ///
    /// An error the caller should record; nothing queued is `Ok(0)`.
    fn receive(&mut self, buffers: &mut [Vec<u8>], datagrams: &mut Datagrams) -> io::Result<usize>;

    /// Point the socket at a different address, which is what a name that
    /// resolved somewhere else needs
    /// (`aeron_udp_channel_transport_reconnect`, `:376-392`).
    ///
    /// Deliberately **not** a defaulted method: a transport that quietly
    /// ignored this would leave a re-resolution looking applied.
    ///
    /// # Errors
    ///
    /// The error from `connect(2)`.
    fn reconnect(&mut self, address: SocketAddr) -> io::Result<()>;

    /// The local address the socket is bound to, which the channel status
    /// counter reports (`aeron_udp_channel_transport_bind_addr_and_port`,
    /// `aeron_udp_channel_transport.c:917-930`).
    ///
    /// # Errors
    ///
    /// The error from `getsockname(2)`.
    fn local_address(&self) -> io::Result<SocketAddr>;

    /// The socket's receive buffer, as the kernel reports it — what a
    /// subscription's window has to fit inside
    /// (`aeron_udp_channel_transport_get_so_rcvbuf`, `:904-915`).
    ///
    /// # Errors
    ///
    /// The error from `getsockopt(2)`.
    fn receive_buffer_size(&self) -> io::Result<usize>;
}
