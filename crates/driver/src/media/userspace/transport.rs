//! The transport the driver sees: one bound `(address, port)` on one stack.
//!
//! Everything the endpoints, the publications and the images ask of a
//! transport is asked of this — and the answers are the kernel's answers,
//! because the layers above were written against them (G1–G12 of the design's
//! list). What is different is underneath: a datagram is one frame out of the
//! port, and `receive` is a copy out of a ring rather than a `recvmmsg`.
//!
//! # What a transport holds, and what that buys
//!
//! It holds the stack, its own ring — as an `Arc`, so that closing it returns
//! the port at once rather than when the last stack reference goes (G12) — and
//! the binding that names it. It does **not** hold the port's lock or the
//! stack's: a transport is one of many on a machine, and the two threads that
//! use it reach the ports through the stack.
//!
//! # Sending, until the neighbour table arrives
//!
//! A frame cannot leave without a destination MAC, and this slice has no
//! neighbour table yet: the next one adds it. Until then `send` answers `Ok(0)`
//! — back pressure, which is exactly what Q3 says an unresolved next hop is
//! — rather than an error, so a caller that runs before the table exists
//! behaves the way it will when the table has no entry.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use crate::media::udp_transport::OpenError;
use crate::media::{Affinity, Transport};
use crate::sys::socket::{Datagram, Datagrams, MAX_BATCH};

use super::frame::{self, Received};
use super::ports::Binding;
use super::stack::{ReadyRing, Stack};

/// A transport over a [`Stack`]: one bound port, and the frames addressed to
/// it.
pub struct UserspaceTransport {
    stack: Arc<Stack>,
    ring: Arc<ReadyRing>,
    binding: Binding,
    /// The address the channel asked to be bound to, as the kernel would
    /// report it: the wildcard stays the wildcard, and a port of zero is the
    /// one that was allocated.
    local: SocketAddr,
    /// The peer this transport was connected to, if it was, which is who it
    /// hears from and — with a neighbour table — who it sends to.
    peer: Option<SocketAddr>,
    /// The thread that owns this transport, which is the queue its frames
    /// leave by.
    affinity: Affinity,
}

impl UserspaceTransport {
    /// Bind a port on `stack` and answer the transport that holds it.
    ///
    /// `bind` is the address the channel asked for, with a port of zero asking
    /// for one from the ephemeral range; `connect` is the peer a connected
    /// channel names.
    ///
    /// # Errors
    ///
    /// [`OpenError::Bind`] with the address and the errno, which is the chain
    /// the endpoints compose their report from, or [`OpenError::Io`] for a
    /// family this stack does not serve.
    pub fn open(
        stack: Arc<Stack>,
        bind: SocketAddr,
        connect: Option<SocketAddr>,
        affinity: Affinity,
    ) -> Result<Self, OpenError> {
        let address = match bind.ip() {
            // `0.0.0.0` is what a channel means by the wildcard, and it is the
            // one address a bind cannot mean literally.
            IpAddr::V4(address) if address.is_unspecified() => None,
            IpAddr::V4(address) => Some(address),
            // No IPv6: the stack has none, and the design refuses it at the
            // channel rather than dropping its datagrams (§4.11).
            IpAddr::V6(_) => {
                return Err(OpenError::Io(io::Error::from_raw_os_error(
                    libc::EAFNOSUPPORT,
                )));
            }
        };

        if let Some(peer) = connect {
            if !matches!(peer.ip(), IpAddr::V4(_)) {
                return Err(OpenError::Io(io::Error::from_raw_os_error(
                    libc::EAFNOSUPPORT,
                )));
            }
        }

        let (binding, ring) = stack
            .bind(address, bind.port(), connect, bind)
            .map_err(OpenError::Bind)?;
        // The address as it was asked for — the wildcard stays the wildcard,
        // which is what a kernel socket bound to `0.0.0.0` reports too — and
        // the port that came of it.
        let local = SocketAddr::from((bind.ip(), binding.port));

        Ok(Self {
            stack,
            ring,
            binding,
            local,
            peer: connect,
            affinity,
        })
    }

    /// The binding this transport holds.
    pub const fn binding(&self) -> Binding {
        self.binding
    }

    /// The thread that owns this transport, and therefore the transmit queue
    /// it sends out of.
    pub const fn affinity(&self) -> Affinity {
        self.affinity
    }

    /// The peer it is connected to, if any.
    pub const fn connected_address(&self) -> Option<SocketAddr> {
        self.peer
    }

    /// The address a frame for this transport would be sent to: the connected
    /// peer when there is one, and the caller's argument otherwise — which is
    /// the kernel's own choice (`aeron_udp_channel_transport_send`,
    /// `aeron_udp_channel_transport.c:818-902`).
    fn destination(&self, address: Option<SocketAddr>) -> Option<Ipv4Addr> {
        let destination = self.peer.or(address)?;

        match destination.ip() {
            IpAddr::V4(address) => Some(address),
            IpAddr::V6(_) => None,
        }
    }
}

impl Transport for UserspaceTransport {
    fn send(&mut self, address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
        // The neighbour table arrives with the next slice. An address with no
        // MAC behind it is back pressure, not a failure (Q3) — the caller
        // comes back on its next pass, which is what it does for a full socket
        // buffer.
        // The destination is worked out here even though nothing is sent with
        // it, because the shape of this call is what the next slice fills in:
        // a connected transport sends where it was connected and ignores the
        // argument, an unconnected one uses it (`media/mod.rs:90-93`).
        let _ = self.destination(address);
        let _ = buffers;

        Ok(0)
    }

    fn receive(&mut self, buffers: &mut [Vec<u8>], datagrams: &mut Datagrams) -> io::Result<usize> {
        let mut received = 0;

        while received < buffers.len().min(MAX_BATCH) {
            let Some(frame) = self.ring.pop() else {
                break;
            };

            let Ok(Received::Udp { ipv4, datagram }) =
                frame::read(frame.as_bytes(), frame::Checksums::Software)
            else {
                // Only UDP frames are ever filed, so this is a bug in the
                // routing rather than a frame to hand up; it is dropped rather
                // than panicked on, because a driver that stops is worse than
                // one that loses a datagram it should not have had.
                continue;
            };

            let buffer = &mut buffers[received];
            let length = datagram.payload.len();
            let copied = length.min(buffer.len());
            buffer[..copied].copy_from_slice(&datagram.payload[..copied]);

            // The datagram's length is what arrived, not what fit: the
            // reference's receiver compares it against the MTU and refuses a
            // datagram that did not (a kernel `recvmmsg` with `MSG_TRUNC`
            // reports the same way).
            datagrams.push(Datagram {
                length,
                source: Some(SocketAddr::from((ipv4.source, datagram.source_port))),
            });
            received += 1;
        }

        Ok(received)
    }

    /// Point a connected transport at a different peer, which is what a name
    /// that resolved somewhere else needs.
    ///
    /// An unconnected transport is left alone, and that is the reference's own
    /// test (`NULL != transport->connected_address`,
    /// `aeron_udp_channel_transport.c:380`): it names the address on every
    /// send, so there is nothing to reconnect.
    fn reconnect(&mut self, address: SocketAddr) -> io::Result<()> {
        if self.peer.is_none() {
            return Ok(());
        }

        if !matches!(address.ip(), IpAddr::V4(_)) {
            return Err(io::Error::from_raw_os_error(libc::EAFNOSUPPORT));
        }

        self.stack.reconnect(self.binding, address);
        self.peer = Some(address);

        Ok(())
    }

    fn local_address(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }

    /// What this transport can hold, in bytes: its ring's capacity times the
    /// port's MTU.
    ///
    /// The kernel answers `getsockopt(SO_RCVBUF)`, which is the same question
    /// — how much this transport can hold — and the number matters to the
    /// layer above, which sizes a subscription's window against it (§4.11).
    fn receive_buffer_size(&self) -> io::Result<usize> {
        Ok(self.ring.capacity() * self.stack.params().mtu)
    }
}

impl std::fmt::Debug for UserspaceTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Everything about the transport but the stack, which has no useful
        // single-line form and is shared with every other transport on the
        // machine anyway.
        f.debug_struct("UserspaceTransport")
            .field("binding", &self.binding)
            .field("local", &self.local)
            .field("peer", &self.peer)
            .field("affinity", &self.affinity)
            .finish_non_exhaustive()
    }
}

impl Drop for UserspaceTransport {
    fn drop(&mut self) {
        // The port goes back the moment the transport does (G12), and with it
        // whatever was waiting in its ring.
        self.stack.release(self.binding);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::userspace::fake_port::{FakePort, Side, Wire};
    use crate::media::userspace::frame::{IPV4_UDP_HEADER_LENGTH, Ipv4Write, UdpWrite};
    use crate::media::userspace::packet_io::TxPort;
    use crate::media::userspace::stack::StackParams;

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);

    fn stack(wire: &Wire) -> Arc<Stack> {
        Stack::over_a_fake_wire(
            wire,
            Side::A,
            StackParams::new(OWN, 24, [0x02, 0, 0, 0, 0, 0x01]),
            super::super::stack::Clock::Monotonic,
        )
    }

    fn open(stack: &Arc<Stack>, port: u16) -> UserspaceTransport {
        UserspaceTransport::open(
            Arc::clone(stack),
            SocketAddr::from((OWN, port)),
            None,
            Affinity::Receiver,
        )
        .expect("a transport")
    }

    fn send_udp(port: &mut FakePort, to: u16, payload: &[u8]) {
        let mut bytes = vec![0u8; IPV4_UDP_HEADER_LENGTH + payload.len()];
        bytes[IPV4_UDP_HEADER_LENGTH..].copy_from_slice(payload);
        let written = crate::media::userspace::frame::write_ipv4_udp(
            &mut bytes,
            &UdpWrite {
                ipv4: Ipv4Write {
                    destination_hardware: [0x02, 0, 0, 0, 0, 0x01],
                    source_hardware: [0x02, 0, 0, 0, 0, 0x02],
                    source: PEER,
                    destination: OWN,
                    identification: 1,
                },
                source_port: 40000,
                destination_port: to,
                payload_length: payload.len(),
            },
        )
        .expect("a frame");

        port.tx_burst(1, &mut |buffer| {
            buffer[..written].copy_from_slice(&bytes[..written]);
            written
        });
    }

    fn receive(transport: &mut UserspaceTransport) -> Vec<(SocketAddr, Vec<u8>)> {
        let mut buffers = vec![vec![0u8; 1408]];
        let mut datagrams = Datagrams::new();
        let received = transport
            .receive(&mut buffers, &mut datagrams)
            .expect("a receive");

        datagrams.as_slice()[..received]
            .iter()
            .enumerate()
            .map(|(index, datagram)| {
                (
                    datagram.source.expect("a source"),
                    buffers[index][..datagram.length].to_vec(),
                )
            })
            .collect()
    }

    /// G1: a channel that asks for port zero is given one, and the address it
    /// reports back is the one it can be reached at.
    #[test]
    fn a_port_of_zero_comes_back_in_the_local_address() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let transport = UserspaceTransport::open(
            Arc::clone(&stack),
            SocketAddr::from((OWN, 0)),
            None,
            Affinity::Receiver,
        )
        .expect("a transport");

        let local = transport.local_address().expect("an address");
        assert_eq!(
            OWN,
            match local.ip() {
                IpAddr::V4(address) => address,
                IpAddr::V6(_) => panic!("an IPv4 stack"),
            }
        );
        assert!(
            local.port() >= 32768 && local.port() <= 60999,
            "{}",
            local.port()
        );
    }

    /// A wildcard bind reports the wildcard, as a kernel socket bound to
    /// `0.0.0.0` does — the port is the part that was chosen.
    #[test]
    fn a_wildcard_bind_reports_the_wildcard() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let transport = UserspaceTransport::open(
            Arc::clone(&stack),
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 40470)),
            None,
            Affinity::Receiver,
        )
        .expect("a transport");

        assert!(
            transport
                .local_address()
                .expect("an address")
                .ip()
                .is_unspecified()
        );
    }

    /// G2 through the transport: a second bind of one address fails with the
    /// chain the endpoints report, and the descriptor is the `-1` that says
    /// there is none.
    #[test]
    fn a_second_bind_is_eaddr_inuse() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let _first = open(&stack, 40471);

        let failure = UserspaceTransport::open(
            Arc::clone(&stack),
            SocketAddr::from((OWN, 40471)),
            None,
            Affinity::Receiver,
        )
        .expect_err("the second bind");

        let OpenError::Bind(failure) = failure else {
            panic!("a bind failure, not an io error");
        };
        assert_eq!(Some(libc::EADDRINUSE), failure.source.raw_os_error());
        assert_eq!(SocketAddr::from((OWN, 40471)), failure.address);
        assert_eq!(-1, failure.fd);
    }

    /// What arrives at the port comes out of `receive` as a datagram with its
    /// source — and only the Aeron frame behind the 42 bytes, unchanged (G14).
    #[test]
    fn a_frame_arrives_as_a_datagram_with_its_source() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40472);

        send_udp(
            &mut wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500),
            40472,
            b"aeron frame",
        );
        stack.pump();

        let received = receive(&mut transport);
        assert_eq!(1, received.len());
        assert_eq!(SocketAddr::from((PEER, 40000)), received[0].0);
        assert_eq!(b"aeron frame", &received[0].1[..]);
    }

    /// G14: an Aeron frame crosses with not one byte changed, whatever it
    /// contains — a payload that is not text, and not a multiple of anything.
    #[test]
    fn an_aeron_frame_crosses_unchanged() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40473);

        let aeron: Vec<u8> = (0..255u8).collect();
        send_udp(
            &mut wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500),
            40473,
            &aeron,
        );
        stack.pump();

        assert_eq!(aeron, receive(&mut transport)[0].1);
    }

    /// A datagram longer than the buffer is reported at its own length, not at
    /// what fit — which is what the receiver compares against the MTU.
    #[test]
    fn a_datagram_reports_the_length_that_arrived() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40474);

        let long = vec![0x5a; 100];
        send_udp(
            &mut wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500),
            40474,
            &long,
        );
        stack.pump();

        let mut buffers = vec![vec![0u8; 32]];
        let mut datagrams = Datagrams::new();
        assert_eq!(
            1,
            transport
                .receive(&mut buffers, &mut datagrams)
                .expect("a receive")
        );
        assert_eq!(
            100,
            datagrams.as_slice()[0].length,
            "the length, not the fit"
        );
        assert_eq!(&long[..32], &buffers[0][..32], "and what fit is the front");
    }

    /// G11: what the transport says it can hold is its ring, in bytes.
    #[test]
    fn the_receive_buffer_is_the_ring_in_bytes() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let transport = open(&stack, 40475);

        let expected = stack.params().ring_capacity() * stack.params().mtu;
        assert_eq!(expected, transport.receive_buffer_size().expect("a size"));
        assert!(
            expected > 128 * 1024,
            "and it is not smaller than the window"
        );
    }

    /// G12: the port is free the moment the transport goes, which is what
    /// `Drop` does and why the ring's `Arc` is the transport's own.
    #[test]
    fn the_port_comes_back_the_moment_the_transport_goes() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let transport = open(&stack, 40476);
        assert_eq!(1, stack.bound_ports());

        drop(transport);

        assert_eq!(0, stack.bound_ports());
        let _again = open(&stack, 40476);
    }

    /// Frames waiting in a ring are dropped with the transport that owned it:
    /// a new transport on the same port never sees them (§3.2's ABA).
    #[test]
    fn a_new_transport_does_not_inherit_the_frames_of_the_old_one() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let first = open(&stack, 40477);

        let slot = first.binding().slot;
        send_udp(
            &mut wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500),
            40477,
            b"for the old one",
        );
        stack.pump();
        assert_eq!(
            1,
            stack.ring(slot).expect("a ring").len(),
            "waiting, and not read yet"
        );

        drop(first);
        let mut second = open(&stack, 40477);

        assert_eq!(0, stack.ring(slot).expect("a ring").len(), "gone with it");
        assert!(receive(&mut second).is_empty());
    }

    /// An unconnected transport is not moved by `reconnect` — the reference's
    /// own test — and a connected one changes who it hears from.
    #[test]
    fn reconnect_moves_a_connected_transport_only() {
        let wire = Wire::new();
        let stack = stack(&wire);

        let mut unconnected = open(&stack, 40478);
        assert!(unconnected.connected_address().is_none());
        unconnected
            .reconnect(SocketAddr::from((PEER, 40000)))
            .expect("a reconnect");
        assert!(unconnected.connected_address().is_none(), "left alone");

        let mut connected = UserspaceTransport::open(
            Arc::clone(&stack),
            SocketAddr::from((OWN, 40479)),
            Some(SocketAddr::from((PEER, 40000))),
            Affinity::Receiver,
        )
        .expect("a transport");

        let elsewhere = SocketAddr::from((Ipv4Addr::new(10, 10, 0, 8), 40001));
        connected.reconnect(elsewhere).expect("a reconnect");
        assert_eq!(Some(elsewhere), connected.connected_address());
    }

    /// Sending is back pressure until the neighbour table arrives: not an
    /// error, and not a frame.
    #[test]
    fn sending_without_a_neighbour_is_back_pressure() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40480);

        assert_eq!(
            0,
            transport
                .send(Some(SocketAddr::from((PEER, 40000))), &[b"nothing"])
                .expect("back pressure, not a failure")
        );
        assert_eq!(0, wire.queued(Side::A), "and nothing left");
    }
}
