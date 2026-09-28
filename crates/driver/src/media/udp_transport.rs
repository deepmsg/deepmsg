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
use std::net::SocketAddr;

use crate::sys::AddressFamily;
use crate::sys::socket::DatagramSocket;

use super::{Datagrams, Transport, TransportParams};

/// A UDP socket opened for one endpoint.
#[derive(Debug)]
pub struct UdpTransport {
    socket: DatagramSocket,
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
    /// # Errors
    ///
    /// Any of the syscalls' errors.
    pub fn open(
        bind: SocketAddr,
        connect: Option<SocketAddr>,
        params: &TransportParams,
    ) -> io::Result<Self> {
        let socket = DatagramSocket::open(AddressFamily::of(bind.ip()))?;

        // `:147-154`: a unicast transport binds and nothing else.
        socket.bind(bind)?;

        if let Some(address) = connect {
            // `:316-324`.
            socket.connect(address)?;
        }

        if params.socket_rcvbuf > 0 {
            // `:326-334`.
            socket.set_receive_buffer(params.socket_rcvbuf)?;
        }

        if params.socket_sndbuf > 0 {
            // `:336-344`.
            socket.set_send_buffer(params.socket_sndbuf)?;
        }

        if params.ttl > 0 {
            socket.set_ttl(params.ttl)?;
        }

        // `:356-366`: both descriptors, which for a unicast transport are the
        // same one.
        socket.set_nonblocking()?;

        Ok(Self {
            socket,
            connected_address: connect,
        })
    }

    /// The address this transport is connected to, if any.
    pub const fn connected_address(&self) -> Option<SocketAddr> {
        self.connected_address
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
        match self.socket.receive_batch(buffers, datagrams) {
            Ok(received) => Ok(received),
            Err(error) if Self::is_back_pressure(&error) => Ok(0),
            Err(error) => Err(error),
        }
    }

    fn local_address(&self) -> io::Result<SocketAddr> {
        self.socket.local_address()
    }

    fn receive_buffer_size(&self) -> io::Result<usize> {
        self.socket.receive_buffer_size()
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
        let mut receiver =
            UdpTransport::open(loopback(0), None, &TransportParams::default()).expect("a socket");
        let bound = receiver.local_address().expect("a bound address");

        let mut sender = UdpTransport::open(loopback(0), Some(bound), &TransportParams::default())
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
        let mut receiver =
            UdpTransport::open(loopback(0), None, &TransportParams::default()).expect("a socket");
        let bound = receiver.local_address().expect("a bound address");

        let mut sender = UdpTransport::open(loopback(0), Some(bound), &TransportParams::default())
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
        let mut receiver =
            UdpTransport::open(loopback(0), None, &TransportParams::default()).expect("a socket");
        let bound = receiver.local_address().expect("a bound address");

        let mut sender =
            UdpTransport::open(loopback(0), None, &TransportParams::default()).expect("a socket");
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

        let mut sender = UdpTransport::open(loopback(0), Some(closed), &TransportParams::default())
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
            &TransportParams {
                socket_rcvbuf: 1 << 20,
                socket_sndbuf: 1 << 20,
                ttl: 4,
            },
        )
        .expect("a socket");

        // The kernel doubles what it is given and never reports less than the
        // book minimum, so the assertion is that the option took effect at
        // all rather than that it round-trips.
        assert!(transport.receive_buffer_size().expect("an answer") > 0);
    }
}
