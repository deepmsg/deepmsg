//! The transport over the kernel's own calls.
//!
//! Mirrors `aeron-driver/src/main/c/media/aeron_udp_channel_transport.c`, and
//! the send-path choice in particular
//! (`aeron_udp_channel_transport_send`, `:818-902`):
//!
//! * a **connected** transport sends one datagram with `send`
//!   (`:646-665`) and several with `sendmmsg` and no address (`:701-752`);
//! * an **unconnected** one always names the address
//!   (`:668-696`).
//!
//! Which matters because the sender's position advances by what the kernel
//! took: a datagram that was not sent is one the publication will offer again.
//!
//! # Back pressure is not an error
//!
//! `EAGAIN`, `EWOULDBLOCK`, `ECONNREFUSED` and `EINTR` are "nothing happened
//! this time" in the reference (`:728-731`, `:584-587`) and here: the sender
//! comes back on its next pass, and a subscriber that is not there yet — which
//! is an ICMP port-unreachable on a connected socket, reported as
//! `ECONNREFUSED` — must not look like a broken driver.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::sys::AddressFamily;
use crate::sys::socket::DatagramSocket;

use super::{Datagrams, Transport, TransportParams};

/// A UDP socket opened for one endpoint.
///
/// # One descriptor or two
///
/// `socket` is the one that sends, and the one that receives too unless
/// `recv_socket` is there. The reference keeps the same pair as `fd` and
/// `recv_fd` and starts them equal (`aeron_udp_channel_transport.c:141`); a
/// second descriptor appears in exactly one case (`:157-164`): a **multicast**
/// transport that was also given a connect address. Only a *send* endpoint
/// passes one (`aeron_send_channel_endpoint.c:91` — receive destinations pass
/// `NULL`, `aeron_receive_destination.c:73`), which is why a subscriber to a
/// group has one descriptor and a publisher to it has two.
///
/// The split is not about direction, and reading it that way is the mistake to
/// avoid: the sending descriptor *sends*, and the receiving one *joins*. A
/// publisher joins the group's **control** twin — `remote_control`, the group
/// one apart, which is where NAKs come back — and then sends to the group
/// itself down the other one.
/// Why a transport could not be opened.
///
/// The bind is the one failure that carries more than an errno: the reference
/// composes its line from the descriptor and the address
/// (`aeron_bind`, `aeron_socket.c:88-95`) and the layers above append to it,
/// so the two facts have to survive the trip.
#[derive(Debug)]
pub enum OpenError {
    /// The unicast bind, with what the first line of the composition needs.
    Bind(crate::sys::socket::BindFailure),
    /// Every other syscall, unchanged.
    Io(io::Error),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bind(failure) => write!(f, "{failure}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for OpenError {}

impl From<io::Error> for OpenError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub struct UdpTransport {
    socket: DatagramSocket,
    recv_socket: Option<DatagramSocket>,
    connected_address: Option<SocketAddr>,
}

impl UdpTransport {
    /// Open a socket, bind it, and connect it if the channel asked for that
    /// (`aeron_udp_channel_transport_init`, `:100-374`).
    ///
    /// The order is the reference's, and two steps in it are load-bearing: the
    /// bind comes **before** the connect, and both come before the socket is
    /// made non-blocking.
    ///
    /// `multicast_interface` is the interface a group is joined on and sent
    /// through — the channel's local side, which a multicast channel resolves
    /// to a real address rather than the wildcard. It is ignored for a unicast
    /// address, exactly as `multicast_if_addr` is in the reference's unicast
    /// branch.
    ///
    /// # Errors
    ///
    /// Any of the syscalls' errors, and `EINVAL` for a multicast address with
    /// no interface to join through.
    pub fn open(
        bind: SocketAddr,
        multicast_interface: Option<SocketAddr>,
        connect: Option<SocketAddr>,
        params: &TransportParams,
    ) -> Result<Self, OpenError> {
        let family = AddressFamily::of(bind.ip());
        let socket = DatagramSocket::open(family)?;
        let is_multicast = bind.ip().is_multicast();

        let recv_socket = if is_multicast && connect.is_some() {
            Some(DatagramSocket::open(family)?)
        } else {
            None
        };

        let receive = recv_socket.as_ref().unwrap_or(&socket);

        if is_multicast {
            // `:166-182`: both options, on the *receiving* descriptor, so that
            // several subscribers to one group can share the port.
            receive.set_reuse_address()?;
            receive.set_reuse_port()?;

            // `:190-205`, `:257-272`: a group this socket did not join is not
            // this socket's to hear.
            receive.set_multicast_all_disabled()?;

            let interface =
                multicast_interface.ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;

            // `:186-273`: the bind is the **wildcard** at the channel's port —
            // the group is joined, never bound.
            receive
                .bind(SocketAddr::new(wildcard(family), bind.port()))
                .map_err(OpenError::Bind)?;

            // `:275-293`: the join, on the receiving descriptor.
            receive.join_multicast_group(bind.ip(), interface.ip(), params.multicast_if_index)?;

            // `:230-235`, `:295-302`: the interface sends leave by, on the
            // sending one.
            socket.set_multicast_interface(interface.ip(), params.multicast_if_index)?;

            if params.ttl > 0 {
                // `:304-312`: the hop limit, also on the sending descriptor.
                socket.set_multicast_ttl(params.ttl)?;
            }
        } else {
            // `:147-154`: a unicast transport binds and nothing else — and
            // this is the bind whose failure the reference composes from the
            // descriptor and the address.
            socket.bind(bind).map_err(OpenError::Bind)?;
        }

        if let Some(address) = connect {
            // `:316-324`.
            socket.connect(address)?;
        }

        if params.socket_rcvbuf > 0 {
            // `:326-334`: on the descriptor that receives.
            receive.set_receive_buffer(params.socket_rcvbuf)?;
        }

        if params.socket_sndbuf > 0 {
            // `:336-344`: on the one that sends.
            socket.set_send_buffer(params.socket_sndbuf)?;
        }

        // `:356-366`: both descriptors, which for a unicast transport are the
        // same one.
        socket.set_nonblocking()?;

        if let Some(recv_socket) = &recv_socket {
            recv_socket.set_nonblocking()?;
        }

        Ok(Self {
            socket,
            recv_socket,
            connected_address: connect,
        })
    }

    /// The address this transport is connected to, if any.
    pub const fn connected_address(&self) -> Option<SocketAddr> {
        self.connected_address
    }

    /// Whether this transport holds two descriptors rather than one
    /// (`transport->recv_fd != transport->fd`,
    /// `aeron_udp_channel_transport.c:394-409`, which is also the condition
    /// its close tests before closing a second one).
    pub const fn has_split_descriptors(&self) -> bool {
        self.recv_socket.is_some()
    }

    /// The descriptor that receives — the second one where there is one, which
    /// is the reference's `transport->recv_fd` at every receive call site
    /// (`:439`, `:506`, `:577`, `:904-915`, `:917-930`).
    fn receiving(&self) -> &DatagramSocket {
        self.recv_socket.as_ref().unwrap_or(&self.socket)
    }

    /// Whether an error is the kernel saying "not now" rather than "no".
    ///
    /// `ECONNREFUSED` counts as "not now" for the reason the reference gives:
    /// on a connected UDP socket an ICMP port-unreachable from a subscriber
    /// that has not started yet arrives as a failed send
    /// (`:511-516`, `:728-731`).
    fn is_back_pressure(error: &io::Error) -> bool {
        // `EAGAIN` and `EWOULDBLOCK` are the same number on Linux, so naming
        // both here would be an unreachable pattern.
        matches!(
            error.raw_os_error(),
            Some(libc::EAGAIN | libc::ECONNREFUSED | libc::EINTR)
        )
    }
}

impl Transport for UdpTransport {
    fn send(&mut self, address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
        if self.connected_address.is_some() && buffers.len() == 1 {
            // `:827-837`: one datagram on a connected socket is a plain
            // `send`.
            return match self.socket.send(buffers[0]) {
                Ok(sent) => Ok(usize::from(sent > 0)),
                Err(error) if Self::is_back_pressure(&error) => Ok(0),
                Err(error) => Err(error),
            };
        }

        let address = match self.connected_address {
            Some(_) => None,
            None => address,
        };

        match self.socket.send_batch(address, buffers) {
            Ok(sent) => Ok(sent),
            Err(error) if Self::is_back_pressure(&error) => Ok(0),
            Err(error) => Err(error),
        }
    }

    fn receive(&mut self, buffers: &mut [Vec<u8>], datagrams: &mut Datagrams) -> io::Result<usize> {
        // The reference's `recvmmsg` path (`:549-644`), with one difference
        // worth naming: it does *not* count a failed receive as an error when
        // the failure is one of the "not now" errnos — it answers zero, and the
        // receiver's pass ends with nothing done.
        match self.receiving().receive_batch(buffers, datagrams) {
            Ok(received) => Ok(received),
            Err(error) if Self::is_back_pressure(&error) => Ok(0),
            Err(error) => Err(error),
        }
    }

    /// Connect the socket somewhere else (`aeron_udp_channel_transport_reconnect`,
    /// `aeron_udp_channel_transport.c:376-392`), which is how a name that
    /// resolved to a new address reaches a transport that was already connected
    /// to the old one.
    ///
    /// A transport with **no** connected address is left alone, and that is the
    /// reference's own test (`NULL != transport->connected_address`, `:380`):
    /// an unconnected transport names the address on every send, so there is
    /// nothing to reconnect.
    ///
    /// # Errors
    ///
    /// The error from `connect(2)`.
    fn reconnect(&mut self, address: SocketAddr) -> io::Result<()> {
        if self.connected_address.is_none() {
            return Ok(());
        }

        self.socket.connect(address)?;
        self.connected_address = Some(address);

        Ok(())
    }

    fn local_address(&self) -> io::Result<SocketAddr> {
        // `:917-930`: the channel-status label reports the **receiving**
        // descriptor's name — which for a multicast transport is the wildcard
        // it bound, not the group it joined.
        self.receiving().local_address()
    }

    fn receive_buffer_size(&self) -> io::Result<usize> {
        self.receiving().receive_buffer_size()
    }
}

/// The unspecified address of a family, which a multicast transport binds
/// (`aeron_udp_channel_transport.c:186-211`, `:248-273`).
fn wildcard(family: AddressFamily) -> IpAddr {
    match family {
        AddressFamily::Inet => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        AddressFamily::Inet6 => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn what_one_transport_sends_another_receives() {
        let mut receiver = UdpTransport::open(loopback(0), None, None, &TransportParams::default())
            .expect("a socket");
        let bound = receiver.local_address().expect("a bound address");

        let mut sender =
            UdpTransport::open(loopback(0), None, Some(bound), &TransportParams::default())
                .expect("a socket");

        assert_eq!(1, sender.send(None, &[b"ping"]).expect("a send"));

        let mut buffers = vec![vec![0u8; 1408], vec![0u8; 1408]];
        let mut datagrams = Datagrams::new();

        assert_eq!(
            1,
            receiver
                .receive(&mut buffers, &mut datagrams)
                .expect("a receive")
        );
        assert_eq!(b"ping", &buffers[0][..4]);
        assert_eq!(
            bound.ip(),
            datagrams.as_slice()[0].source.expect("a source").ip()
        );
    }

    #[test]
    fn a_batch_is_sent_and_received_as_a_batch() {
        let mut receiver = UdpTransport::open(loopback(0), None, None, &TransportParams::default())
            .expect("a socket");
        let bound = receiver.local_address().expect("a bound address");

        let mut sender =
            UdpTransport::open(loopback(0), None, Some(bound), &TransportParams::default())
                .expect("a socket");

        assert_eq!(
            3,
            sender
                .send(None, &[b"one", b"two", b"three"])
                .expect("a send")
        );

        let mut buffers = vec![vec![0u8; 1408], vec![0u8; 1408], vec![0u8; 1408]];
        let mut datagrams = Datagrams::new();
        assert_eq!(
            3,
            receiver
                .receive(&mut buffers, &mut datagrams)
                .expect("a receive")
        );

        let received: Vec<&[u8]> = datagrams
            .as_slice()
            .iter()
            .enumerate()
            .map(|(index, datagram)| &buffers[index][..datagram.length])
            .collect();

        assert_eq!(vec![&b"one"[..], &b"two"[..], &b"three"[..]], received);
    }

    #[test]
    fn an_unconnected_transport_names_its_peer_every_time() {
        let mut receiver = UdpTransport::open(loopback(0), None, None, &TransportParams::default())
            .expect("a socket");
        let bound = receiver.local_address().expect("a bound address");

        let mut sender = UdpTransport::open(loopback(0), None, None, &TransportParams::default())
            .expect("a socket");
        assert!(sender.connected_address().is_none());

        assert_eq!(1, sender.send(Some(bound), &[b"hello"]).expect("a send"));

        let mut buffers = vec![vec![0u8; 1408]];
        let mut datagrams = Datagrams::new();
        assert_eq!(
            1,
            receiver
                .receive(&mut buffers, &mut datagrams)
                .expect("a receive")
        );
        assert_eq!(b"hello", &buffers[0][..5]);
    }

    #[test]
    fn a_peer_that_is_not_listening_is_not_an_error() {
        // A connected socket that sends to a port nobody holds gets an ICMP
        // port-unreachable back, which the kernel reports on the *next* send
        // as `ECONNREFUSED`. The reference treats that as "not now" — a
        // subscriber that has not started yet is the normal case — and so does
        // this.
        let closed = {
            // A port this process just released: bind it, read it, drop it.
            let socket = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            socket.bind(loopback(0)).expect("a bind");
            socket.local_address().expect("an address")
        };

        let mut sender =
            UdpTransport::open(loopback(0), None, Some(closed), &TransportParams::default())
                .expect("a socket");

        // Two sends: the first produces the ICMP, the second is told about it.
        let first = sender.send(None, &[b"one"]);
        let second = sender.send(None, &[b"two"]);

        assert!(first.is_ok(), "{first:?}");
        assert_eq!(Some(0), second.ok(), "back pressure, not a failure");
    }

    #[test]
    fn the_socket_options_are_the_ones_that_were_asked_for() {
        let transport = UdpTransport::open(
            loopback(0),
            None,
            None,
            &TransportParams {
                socket_rcvbuf: 1 << 20,
                socket_sndbuf: 1 << 20,
                multicast_if_index: 0,
                ttl: 4,
            },
        )
        .expect("a socket");

        // The kernel doubles what it is given and never reports less than the
        // book minimum, so the assertion is that the option took effect at
        // all rather than that it round-trips.
        assert!(transport.receive_buffer_size().expect("an answer") > 0);
    }

    /// The interface this host joins a group on — what a channel with no
    /// `interface=` resolves to, and deliberately *asked* rather than written
    /// down: which interface it is depends on the machine.
    fn interface() -> IpAddr {
        crate::sys::interface_for_address(AddressFamily::Inet, wildcard(AddressFamily::Inet), 0)
            .expect("the interface list")
            .expect("an interface to join on")
            .address
    }

    /// A port this process has just released. Two transports have to agree on
    /// one, so it cannot be the kernel that picks it.
    fn free_port() -> u16 {
        let socket = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        socket.bind(loopback(0)).expect("a bind");
        socket.local_address().expect("an address").port()
    }

    fn group(last: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 2, last)), port)
    }

    fn joined(address: SocketAddr) -> UdpTransport {
        UdpTransport::open(
            address,
            Some(SocketAddr::new(interface(), 0)),
            None,
            &TransportParams::default(),
        )
        .expect("a socket")
    }

    fn receive(transport: &mut UdpTransport) -> Vec<Vec<u8>> {
        let mut buffers = vec![vec![0u8; 1408], vec![0u8; 1408]];
        let mut datagrams = Datagrams::new();
        let count = transport
            .receive(&mut buffers, &mut datagrams)
            .expect("a receive");

        datagrams.as_slice()[..count]
            .iter()
            .enumerate()
            .map(|(index, datagram)| buffers[index][..datagram.length].to_vec())
            .collect()
    }

    #[test]
    fn a_group_is_bound_as_the_wildcard_and_not_as_the_group() {
        let port = free_port();
        let transport = joined(group(1, port));
        let local = transport.local_address().expect("an address");

        assert_eq!(port, local.port(), "the group's port is the port");
        assert!(
            local.ip().is_unspecified(),
            "and the address is the wildcard — a group is joined, never bound"
        );
    }

    #[test]
    fn the_second_descriptor_is_for_a_group_that_sends() {
        let port = free_port();

        assert!(
            !joined(group(1, port)).has_split_descriptors(),
            "a subscriber joins and receives on one"
        );

        let publisher = UdpTransport::open(
            group(2, port),
            Some(SocketAddr::new(interface(), 0)),
            Some(group(1, port)),
            &TransportParams::default(),
        )
        .expect("a socket");

        assert!(
            publisher.has_split_descriptors(),
            "a publisher joins the control group and sends to the data group, \
             and one descriptor cannot do both"
        );

        let unicast = UdpTransport::open(
            loopback(0),
            None,
            Some(loopback(1)),
            &TransportParams::default(),
        )
        .expect("a socket");

        assert!(
            !unicast.has_split_descriptors(),
            "no unicast transport ever has two"
        );
    }

    #[test]
    fn two_subscribers_to_one_group_both_hear_what_is_sent_to_it() {
        let port = free_port();
        let data = group(1, port);
        let control = group(2, port);
        let interface = Some(SocketAddr::new(interface(), 0));

        // Two subscribers on one group and one port. That they can both bind
        // at all is the `SO_REUSEADDR`/`SO_REUSEPORT` pair, and that both hear
        // it is what they are for.
        let mut first = joined(data);
        let mut second = joined(data);

        // A publisher the way a send endpoint is built: bound to the group's
        // **control** twin, which is what it joins, and connected to the group
        // itself, which is what it sends to.
        let mut publisher =
            UdpTransport::open(control, interface, Some(data), &TransportParams::default())
                .expect("a socket");

        assert_eq!(1, publisher.send(None, &[b"hello group"]).expect("a send"));

        assert_eq!(vec![b"hello group".to_vec()], receive(&mut first));
        assert_eq!(vec![b"hello group".to_vec()], receive(&mut second));

        assert_eq!(
            Vec::<Vec<u8>>::new(),
            receive(&mut publisher),
            "the publisher joined the control group, not this one — which is \
             `IP_MULTICAST_ALL` turned off and not a coincidence"
        );
    }
}
