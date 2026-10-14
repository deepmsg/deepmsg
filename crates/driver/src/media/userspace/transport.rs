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
use std::sync::atomic::Ordering;

use crate::media::udp_transport::OpenError;
use crate::media::{Affinity, Transport};
use crate::sys::socket::{Datagram, Datagrams, MAX_BATCH};

use super::frame::{self, IPV4_UDP_HEADER_LENGTH, Ipv4Write, Received, UdpWrite};
use super::neighbour::NextHop;
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
    #[allow(clippy::unused_self)]
    fn destination(&self, address: Option<SocketAddr>) -> Option<SocketAddr> {
        self.peer.or(address)
    }

    /// Write one frame in front of a payload, headers and checksums included.
    ///
    /// Answers the frame's length, or `None` when it does not fit the port — a
    /// datagram longer than the MTU, which the channel validation refuses
    /// before a channel is opened at all (§4.8). It is refused here rather
    /// than truncated: a short frame on the wire is one the peer counts, and
    /// reports as corruption.
    fn build(
        &self,
        buffer: &mut [u8],
        payload: &[u8],
        destination: Ipv4Addr,
        destination_port: u16,
        hardware: [u8; 6],
        identification: u16,
    ) -> Option<usize> {
        let params = self.stack.params();
        let end = IPV4_UDP_HEADER_LENGTH.checked_add(payload.len())?;
        if buffer.len() < end {
            return None;
        }

        buffer[IPV4_UDP_HEADER_LENGTH..end].copy_from_slice(payload);

        frame::write_ipv4_udp(
            buffer,
            &UdpWrite {
                ipv4: Ipv4Write {
                    destination_hardware: hardware,
                    source_hardware: params.mac,
                    source: params.address,
                    destination,
                    identification,
                },
                source_port: self.local.port(),
                destination_port,
                payload_length: payload.len(),
            },
        )
    }
}

impl Transport for UserspaceTransport {
    fn send(&mut self, address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
        let Some(destination) = self.destination(address) else {
            // Nowhere to send it: the kernel answers the same way for a
            // `sendto` with no address on an unconnected socket.
            return Err(io::Error::from_raw_os_error(libc::EDESTADDRREQ));
        };

        let IpAddr::V4(destination_address) = destination.ip() else {
            return Err(io::Error::from_raw_os_error(libc::EAFNOSUPPORT));
        };

        // Off this link and no gateway: there is nothing to send it to, which
        // the design refuses at the channel and which is this transport's
        // answer if one gets here anyway (§4.11).
        let Some(next_hop) = self.stack.next_hop(destination_address) else {
            return Err(io::Error::from_raw_os_error(libc::ENETUNREACH));
        };

        // An unresolved next hop is back pressure, not a failure (Q3): the
        // frame did not leave, the request for its MAC did, and the caller
        // comes back on its next pass — which is what it does for a full
        // socket buffer too.
        let NextHop::Known(hardware) = self.stack.resolve(next_hop, self.affinity) else {
            self.stack
                .counters()
                .arp_miss
                .fetch_add(1, Ordering::Relaxed);

            return Ok(0);
        };

        let count = buffers.len().min(MAX_BATCH);
        let identification = self.stack.next_identification();
        let mut next = 0;

        let taken = {
            let mut port = self
                .stack
                .transmit(self.affinity)
                .lock()
                .expect("the transmit port");

            port.tx_burst(count, &mut |buffer| {
                let Some(payload) = buffers.get(next) else {
                    return 0;
                };
                next += 1;

                self.build(
                    buffer,
                    payload,
                    destination_address,
                    destination.port(),
                    hardware,
                    identification,
                )
                .unwrap_or(0)
            })
        };

        // What the port would not take is what the caller still has to send,
        // and it is counted where the design counts it (78).
        if taken < count {
            self.stack
                .counters()
                .tx_eagain
                .fetch_add((count - taken) as u64, Ordering::Relaxed);
        }

        Ok(taken)
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
    use crate::media::userspace::frame::{
        ARP_REPLY, ARP_REQUEST, ArpPacket, BROADCAST, ETHERNET_TYPE_ARP, EthernetHeader,
        IPV4_UDP_HEADER_LENGTH, Ipv4Write, UdpWrite,
    };
    use crate::media::userspace::packet_io::{Disposition, RxPort, TxPort};
    use crate::media::userspace::stack::StackParams;

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);
    const OUR_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x01];

    fn stack(wire: &Wire) -> Arc<Stack> {
        Stack::over_a_fake_wire(
            wire,
            Side::A,
            StackParams::new(OWN, 24, OUR_MAC),
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

    /// What arrives teaches where it came from: a frame from an address on
    /// this link carries that address's MAC, and the next send to it needs no
    /// resolution of its own (the design's gleaning, §4.5).
    #[test]
    fn a_frame_from_a_peer_teaches_its_mac() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let _transport = open(&stack, 40481);

        send_udp(
            &mut wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500),
            40481,
            b"hello",
        );
        stack.pump();

        assert_eq!(
            NextHop::Known([0x02, 0, 0, 0, 0, 0x02]),
            stack.neighbour(PEER)
        );
    }

    /// An ARP request for this port's address is answered, and answered to the
    /// asker — which is the only way a kernel peer can ever resolve this
    /// stack.
    #[test]
    fn an_arp_request_from_the_wire_is_answered() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let _transport = open(&stack, 40482);

        let asker = [0x02, 0, 0, 0, 0, 0x02];
        let mut bytes = vec![0u8; 64];
        let written = frame::write_arp(
            &mut bytes,
            &EthernetHeader {
                destination: BROADCAST,
                source: asker,
                ether_type: ETHERNET_TYPE_ARP,
            },
            &ArpPacket {
                operation: ARP_REQUEST,
                sender_hardware: asker,
                sender_protocol: PEER,
                target_hardware: [0; 6],
                target_protocol: OWN,
            },
        )
        .expect("a frame");
        wire.port(Side::B, asker, 1500).tx_burst(1, &mut |buffer| {
            buffer[..written].copy_from_slice(&bytes[..written]);
            written
        });

        assert_eq!(1, stack.pump(), "the request was read");

        // And the answer is on the wire, addressed back to the asker.
        assert_eq!(1, wire.queued(Side::A));
        let mut kept = Vec::new();
        wire.port(Side::B, asker, 1500).rx_burst(
            1,
            &mut |frame| {
                let ethernet = frame::read_ethernet_header(frame.bytes).expect("a header");
                assert_eq!(asker, ethernet.destination, "straight back to the asker");

                let Ok(Received::Arp(packet)) = frame::read(frame.bytes, frame.checksums) else {
                    panic!("the frame that left is an ARP reply");
                };
                assert_eq!(ARP_REPLY, packet.operation);
                assert_eq!(OUR_MAC, packet.sender_hardware);
                assert_eq!(OWN, packet.sender_protocol);
                assert_eq!(PEER, packet.target_protocol, "the address that asked");

                Disposition::Consumed
            },
            &mut kept,
        );

        // And asking taught the table who the asker is, on the way past.
        assert_eq!(NextHop::Known(asker), stack.neighbour(PEER));
    }

    /// A request for somebody else's address is not this port's to answer.
    #[test]
    fn a_request_for_another_address_is_not_answered() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let _transport = open(&stack, 40483);

        let mut bytes = vec![0u8; 64];
        let written = frame::write_arp(
            &mut bytes,
            &EthernetHeader {
                destination: BROADCAST,
                source: [0x02, 0, 0, 0, 0, 0x02],
                ether_type: ETHERNET_TYPE_ARP,
            },
            &ArpPacket {
                operation: ARP_REQUEST,
                sender_hardware: [0x02, 0, 0, 0, 0, 0x02],
                sender_protocol: PEER,
                target_hardware: [0; 6],
                target_protocol: Ipv4Addr::new(10, 10, 0, 9),
            },
        )
        .expect("a frame");
        wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500)
            .tx_burst(1, &mut |buffer| {
                buffer[..written].copy_from_slice(&bytes[..written]);
                written
            });

        stack.pump();

        assert_eq!(
            0,
            wire.queued(Side::A),
            "no answer to somebody else's question"
        );
    }

    /// With a next hop known, a frame leaves with that MAC and the payload
    /// unchanged, addressed from this port's own address and port.
    #[test]
    fn a_frame_leaves_with_the_next_hops_mac() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40484);
        stack.insert_neighbour(PEER, [0x02, 0, 0, 0, 0, 0x02]);

        assert_eq!(
            1,
            transport
                .send(Some(SocketAddr::from((PEER, 40999))), &[b"aeron payload"])
                .expect("a send")
        );

        let mut kept = Vec::new();
        wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500).rx_burst(
            1,
            &mut |frame| {
                let ethernet = frame::read_ethernet_header(frame.bytes).expect("a header");
                assert_eq!([0x02, 0, 0, 0, 0, 0x02], ethernet.destination);
                assert_eq!(OUR_MAC, ethernet.source);

                let Ok(Received::Udp { ipv4, datagram }) =
                    frame::read(frame.bytes, frame.checksums)
                else {
                    panic!("a UDP frame");
                };
                assert_eq!(OWN, ipv4.source);
                assert_eq!(PEER, ipv4.destination);
                assert_eq!(40484, datagram.source_port, "from the port it bound");
                assert_eq!(40999, datagram.destination_port);
                assert_eq!(b"aeron payload", datagram.payload);

                Disposition::Consumed
            },
            &mut kept,
        );
    }

    /// A batch goes out as a batch, and what the port would not take is what
    /// the caller still has (G7).
    #[test]
    fn a_batch_goes_out_as_a_batch() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40485);
        stack.insert_neighbour(PEER, [0x02, 0, 0, 0, 0, 0x02]);

        assert_eq!(
            3,
            transport
                .send(
                    Some(SocketAddr::from((PEER, 40999))),
                    &[b"one", b"two", b"three"]
                )
                .expect("a send")
        );
        assert_eq!(3, wire.queued(Side::A));
    }

    /// An off-link destination with no gateway has nothing to be sent to, and
    /// that is the design's refusal rather than a drop (§4.11).
    #[test]
    fn an_off_link_destination_needs_a_gateway() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let mut transport = open(&stack, 40486);
        let far = SocketAddr::from((Ipv4Addr::new(10, 10, 9, 9), 40999));

        assert_eq!(
            Some(libc::ENETUNREACH),
            transport
                .send(Some(far), &[b"off the link"])
                .expect_err("no gateway")
                .raw_os_error()
        );

        // With a gateway, the frame goes to the gateway's MAC and carries the
        // far address as its destination.
        let gateway = Ipv4Addr::new(10, 10, 0, 254);
        let stack = Arc::new(Stack::new(
            StackParams {
                gateway: Some(gateway),
                ..StackParams::new(OWN, 24, OUR_MAC)
            },
            super::super::stack::Clock::Monotonic,
            Box::new(wire.port(Side::A, OUR_MAC, 1408)),
            [
                Box::new(wire.port(Side::A, OUR_MAC, 1408)),
                Box::new(wire.port(Side::A, OUR_MAC, 1408)),
            ],
        ));
        let mut transport = open(&stack, 40487);
        stack.insert_neighbour(gateway, [0x02, 0, 0, 0, 0, 0xfe]);

        assert_eq!(
            1,
            transport
                .send(Some(far), &[b"off the link"])
                .expect("a send")
        );

        let mut kept = Vec::new();
        wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500).rx_burst(
            1,
            &mut |frame| {
                let ethernet = frame::read_ethernet_header(frame.bytes).expect("a header");
                assert_eq!(
                    [0x02, 0, 0, 0, 0, 0xfe],
                    ethernet.destination,
                    "to the gateway"
                );

                let Ok(Received::Udp { ipv4, .. }) = frame::read(frame.bytes, frame.checksums)
                else {
                    panic!("a UDP frame");
                };
                assert_eq!(
                    Ipv4Addr::new(10, 10, 9, 9),
                    ipv4.destination,
                    "for the far address"
                );

                Disposition::Consumed
            },
            &mut kept,
        );
    }

    /// Sending is back pressure while a next hop is unresolved: not an error,
    /// and the frame does not leave.
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
        assert_eq!(1, stack.counters().arp_miss.load(Ordering::Relaxed));

        // What left is the question, not the frame: one broadcast asking who
        // holds the next hop.
        assert_eq!(1, wire.queued(Side::A));
        let mut far = wire.port(Side::B, [0x02, 0, 0, 0, 0, 0x02], 1500);
        let mut kept = Vec::new();
        far.rx_burst(
            1,
            &mut |frame| {
                let received = frame::read(frame.bytes, frame.checksums);
                let Ok(Received::Arp(packet)) = received else {
                    panic!("the frame that left is an ARP request");
                };
                assert_eq!(ARP_REQUEST, packet.operation);
                assert_eq!(PEER, packet.target_protocol);
                assert_eq!(OWN, packet.sender_protocol);

                Disposition::Consumed
            },
            &mut kept,
        );
    }
}
