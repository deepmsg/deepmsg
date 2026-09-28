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
//!   cannot wait for a real network to lose a packet — loopback does not — so
//!   the reference ships transports that drop datagrams on purpose
//!   (`media/aeron_udp_channel_transport_fixed_loss.c`) and so does this
//!   ([`loss_transport`]).
//! * **One place to be wrong about a syscall.** Every socket call in the
//!   driver goes through [`udp_transport`], which is the only module above
//!   [`crate::sys`] that touches one.
//!
//! # What P1-4 carries
//!
//! A single file descriptor per transport — the unicast case. The reference's
//! dual-fd shape (a separate `recv_fd` for multicast, `:141-163`) arrives with
//! multicast in P1-5, as do the interceptors and the transport-level
//! timestamps. The poller is the linear one: the reference switches to
//! `epoll`/`poll` above five transports
//! (`aeron_udp_transport_poller.c:183-196`, `aeron_udp_transport_poller.h:22`),
//! which is P1-5 too.

use std::io;
use std::net::SocketAddr;

pub mod loss_transport;
pub mod send_endpoint;
pub mod udp_transport;

pub use crate::sys::socket::Datagrams;
pub use loss_transport::LossTransport;
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
    /// The `ttl=` parameter, for the sockets that honour it.
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

    /// Receive everything queued into `buffers`, up to their number, and
    /// describe what arrived in `datagrams`.
    ///
    /// # Errors
    ///
    /// An error the caller should record; nothing queued is `Ok(0)`.
    fn receive(&mut self, buffers: &mut [Vec<u8>], datagrams: &mut Datagrams) -> io::Result<usize>;

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
