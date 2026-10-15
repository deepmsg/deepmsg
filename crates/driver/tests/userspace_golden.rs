//! The userspace stack's own criteria, through the interface the driver uses.
//!
//! Every row of the design's §6 list — G1 to G14 — as one test, over
//! [`TransportBindings::Userspace`] and nothing else: a stack on a wire, the
//! transports it opens, and the frames that come out. Nothing here reaches
//! into the stack's internals, which is the point: what the driver is owed is
//! a `Transport`, and these are the answers it is owed by one.
//!
//! The halves that need more than this are `userspace_two_drivers.rs` (two
//! drivers talking) and the DPDK job (a real NIC).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use deepmsg_driver::media::udp_transport::OpenError;
use deepmsg_driver::media::userspace::fake_port::{Side, Wire};
use deepmsg_driver::media::userspace::frame::{self, Checksums, Received};
use deepmsg_driver::media::userspace::stack::{Clock, Stack, StackParams};
use deepmsg_driver::media::userspace::transport::UserspaceTransport;
use deepmsg_driver::media::{Affinity, Transport, TransportBindings, TransportParams};
use deepmsg_driver::sys::socket::Datagrams;

const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);
const OUR_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x01];
const PEER_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x02];

/// One machine's worth of stack on one side of a wire, and the bindings the
/// driver is handed.
struct Machine {
    wire: Wire,
    stack: Arc<Stack>,
    bindings: TransportBindings,
    address: Ipv4Addr,
    peer: Ipv4Addr,
    mac: [u8; 6],
    peer_mac: [u8; 6],
}

impl Machine {
    fn new(
        wire: &Wire,
        address: Ipv4Addr,
        peer: Ipv4Addr,
        mac: [u8; 6],
        peer_mac: [u8; 6],
    ) -> Self {
        let stack = Stack::over_a_fake_wire(
            wire,
            Side::A,
            StackParams::new(address, 24, mac),
            Clock::Monotonic,
        );

        Self {
            wire: wire.clone(),
            bindings: TransportBindings::Userspace(Arc::clone(&stack)),
            stack,
            address,
            peer,
            mac,
            peer_mac,
        }
    }

    fn open(&self, port: u16) -> Result<Box<dyn Transport>, OpenError> {
        self.bindings.open(
            SocketAddr::from((self.address, port)),
            None,
            None,
            &TransportParams::default(),
            Affinity::Receiver,
        )
    }

    fn open_connected(&self, port: u16, peer: SocketAddr) -> Result<Box<dyn Transport>, OpenError> {
        self.bindings.open(
            SocketAddr::from((self.address, port)),
            None,
            Some(peer),
            &TransportParams::default(),
            Affinity::Receiver,
        )
    }

    /// A transport bound to the wildcard, **asked of the stack and not of the
    /// bindings**.
    ///
    /// That is the ownership rule rather than a shortcut around it: a channel
    /// that names no address at all is the kernel's
    /// (`media/bindings.rs::a_channel_that_names_no_address_is_the_kernels`),
    /// so a wildcard binding reached through a *channel's* shape is a kernel
    /// socket and never this stack. G3 is about what this stack does with a
    /// wildcard — hears what is addressed to the port's own address and nothing
    /// else — so it asks the stack, which is the layer that question belongs to.
    fn wildcard(&self, port: u16) -> Result<Box<dyn Transport>, OpenError> {
        Ok(Box::new(UserspaceTransport::open(
            Arc::clone(&self.stack),
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)),
            None,
            Affinity::Receiver,
        )?))
    }

    /// Everything the far end has been sent, in order.
    fn sent(&self) -> Vec<Vec<u8>> {
        self.wire
            .take(Side::B, 64)
            .into_iter()
            .map(|frame| frame.into_vec())
            .collect()
    }

    /// Put one frame on the wire, as the far end.
    fn deliver(&self, bytes: &[u8]) {
        self.wire.send(Side::B, bytes);
    }

    /// One UDP frame from the far end to `(this machine's address, port)`.
    fn deliver_udp(&self, to: u16, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0u8; frame::IPV4_UDP_HEADER_LENGTH + payload.len()];
        bytes[frame::IPV4_UDP_HEADER_LENGTH..].copy_from_slice(payload);
        let written = frame::write_ipv4_udp(
            &mut bytes,
            &frame::UdpWrite {
                ipv4: frame::Ipv4Write {
                    destination_hardware: self.mac,
                    source_hardware: self.peer_mac,
                    source: self.peer,
                    destination: self.address,
                    identification: 1,
                },
                source_port: 40000,
                destination_port: to,
                payload_length: payload.len(),
            },
        )
        .expect("a frame");
        bytes.truncate(written);

        self.deliver(&bytes);
        bytes
    }
}

/// What a transport received, as `(source, payload)` pairs.
fn received(transport: &mut Box<dyn Transport>) -> Vec<(SocketAddr, Vec<u8>)> {
    let mut buffers = vec![vec![0u8; 1408]];
    let mut datagrams = Datagrams::new();
    let count = transport
        .receive(&mut buffers, &mut datagrams)
        .expect("a receive");

    datagrams.as_slice()[..count]
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

fn machine() -> (Wire, Machine) {
    let wire = Wire::new();
    let machine = Machine::new(&wire, OWN, PEER, OUR_MAC, PEER_MAC);
    (wire, machine)
}

fn ip(address: SocketAddr) -> Ipv4Addr {
    match address.ip() {
        IpAddr::V4(address) => address,
        IpAddr::V6(_) => panic!("an IPv4 stack"),
    }
}

/// G1: a channel that asks for port zero is given one, and `local_address`
/// answers where it can be reached.
#[test]
fn g1_a_port_of_zero_is_given_one() {
    let (_wire, machine) = machine();
    let transport = machine
        .bindings
        .open(
            SocketAddr::from((OWN, 0)),
            None,
            None,
            &TransportParams::default(),
            Affinity::Receiver,
        )
        .expect("a transport");

    let local = transport.local_address().expect("a local address");
    assert_eq!(OWN, ip(local));
    assert!((32768..=60999).contains(&local.port()), "{}", local.port());
}

/// G2: a second bind of one address fails, in the chain the endpoints compose
/// their report from.
#[test]
fn g2_a_second_bind_is_eaddr_inuse() {
    let (_wire, machine) = machine();
    let _first = machine.open(40456).expect("a transport");

    let Err(OpenError::Bind(failure)) = machine.open(40456) else {
        panic!("the second bind is a bind failure");
    };

    assert_eq!(Some(libc::EADDRINUSE), failure.source.raw_os_error());
    assert_eq!(SocketAddr::from((OWN, 40456)), failure.address);
    assert_eq!(-1, failure.fd, "no descriptor, and the report says so");
}

/// G3: a wildcard binding hears what is addressed to the port's address, and
/// nothing else.
#[test]
fn g3_a_wildcard_binding_hears_the_ports_own_address() {
    let (_wire, machine) = machine();
    let mut transport = machine.wildcard(40457).expect("a transport");

    machine.deliver_udp(40457, b"to the wildcard");
    machine.stack.pump();
    assert_eq!(1, received(&mut transport).len());
    assert!(
        transport
            .local_address()
            .expect("an address")
            .ip()
            .is_unspecified(),
        "and it reports the wildcard, as a kernel socket does"
    );

    machine.deliver_udp(40458, b"to another port");
    machine.stack.pump();
    // The first frame was read above, so an empty read here is "nothing new":
    // a datagram for a port this transport does not hold is not its.
    assert!(received(&mut transport).is_empty(), "and only that port");
}

/// G4: a connected transport sends where it was connected, whatever address
/// the caller names.
#[test]
fn g4_a_connected_transport_ignores_the_address_it_is_given() {
    let (_wire, machine) = machine();
    let mut transport = machine
        .open_connected(40459, SocketAddr::from((PEER, 40999)))
        .expect("a transport");
    machine.stack.insert_neighbour(PEER, PEER_MAC);

    assert_eq!(
        1,
        transport
            .send(
                Some(SocketAddr::from((Ipv4Addr::new(10, 10, 0, 9), 1234))),
                &[b"payload"]
            )
            .expect("a send")
    );

    let sent = machine.sent();
    assert_eq!(1, sent.len());
    let Received::Udp { ipv4, datagram } =
        frame::read(&sent[0], Checksums::Software).expect("a UDP frame")
    else {
        panic!("a UDP frame");
    };
    assert_eq!(PEER, ipv4.destination, "where it was connected");
    assert_eq!(40999, datagram.destination_port);
}

/// G5: a connected transport hears its peer and nobody else. Turning the
/// filter off is the first falsification the design names, and what it would
/// let through is another session's frames on a shared port.
#[test]
fn g5_a_connected_transport_hears_only_its_peer() {
    let (_wire, machine) = machine();
    let mut transport = machine
        .open_connected(40460, SocketAddr::from((PEER, 40000)))
        .expect("a transport");

    machine.deliver_udp(40460, b"from the peer");
    machine.stack.pump();
    assert_eq!(1, received(&mut transport).len());

    // The same port, the same host, a different source port: somebody else's.
    let mut bytes = vec![0u8; frame::IPV4_UDP_HEADER_LENGTH + 4];
    bytes[frame::IPV4_UDP_HEADER_LENGTH..].copy_from_slice(b"nope");
    let written = frame::write_ipv4_udp(
        &mut bytes,
        &frame::UdpWrite {
            ipv4: frame::Ipv4Write {
                destination_hardware: OUR_MAC,
                source_hardware: PEER_MAC,
                source: PEER,
                destination: OWN,
                identification: 2,
            },
            source_port: 40001,
            destination_port: 40460,
            payload_length: 4,
        },
    )
    .expect("a frame");
    machine.deliver(&bytes[..written]);
    machine.stack.pump();

    assert!(
        received(&mut transport).is_empty(),
        "and only its peer: the first frame was read, and this one is not its"
    );
}

/// G6: every datagram says where it came from.
#[test]
fn g6_every_datagram_carries_where_it_came_from() {
    let (_wire, machine) = machine();
    let mut transport = machine.open(40461).expect("a transport");

    machine.deliver_udp(40461, b"who sent this");
    machine.stack.pump();

    assert_eq!(
        SocketAddr::from((PEER, 40000)),
        received(&mut transport)[0].0
    );
}

/// G7 and G9: a batch is at most sixteen, and the caller is told what left
/// rather than what it offered.
#[test]
fn g7_a_batch_answers_what_left_and_at_most_sixteen() {
    let (_wire, machine) = machine();
    let mut transport = machine.open(40462).expect("a transport");
    machine.stack.insert_neighbour(PEER, PEER_MAC);
    let destination = Some(SocketAddr::from((PEER, 40999)));

    let seventeen: Vec<&[u8]> = (0..17).map(|_| &b"sixteen at most"[..]).collect();
    assert_eq!(
        16,
        transport.send(destination, &seventeen).expect("a send"),
        "seventeen offered, sixteen taken — which is what the caller counts on"
    );
    assert_eq!(16, machine.sent().len());

    let three: Vec<&[u8]> = (0..3).map(|_| &b"all of these"[..]).collect();
    assert_eq!(3, transport.send(destination, &three).expect("a send"));
}

/// G8: an unresolved next hop is back pressure, not an error — and the request
/// that would resolve it goes out in the same pass.
#[test]
fn g8_back_pressure_is_zero_and_not_an_error() {
    let (wire, machine) = machine();
    let mut transport = machine.open(40463).expect("a transport");

    assert_eq!(
        0,
        transport
            .send(Some(SocketAddr::from((PEER, 40999))), &[b"nowhere yet"])
            .expect("back pressure")
    );

    let sent = machine.sent();
    assert_eq!(1, sent.len(), "and one question went out");
    let Received::Arp(question) = frame::read(&sent[0], Checksums::Software).expect("an ARP frame")
    else {
        panic!("an ARP request");
    };
    assert_eq!(PEER, question.target_protocol);
    assert_eq!(0, wire.queued(Side::A), "and nothing else");
}

/// G10: a connected transport follows the peer it is reconnected to.
#[test]
fn g10_a_reconnect_moves_the_peer_it_hears() {
    let (_wire, machine) = machine();
    let mut transport = machine
        .open_connected(40464, SocketAddr::from((PEER, 40000)))
        .expect("a transport");
    let elsewhere = SocketAddr::from((Ipv4Addr::new(10, 10, 0, 8), 40001));

    machine.deliver_udp(40464, b"from the peer");
    machine.stack.pump();
    assert_eq!(1, received(&mut transport).len());

    transport.reconnect(elsewhere).expect("a reconnect");

    machine.deliver_udp(40464, b"from the old peer");
    machine.stack.pump();
    assert!(
        received(&mut transport).is_empty(),
        "the old peer is not heard — and the first frame was read above"
    );

    // And the new one is.
    let mut bytes = vec![0u8; frame::IPV4_UDP_HEADER_LENGTH + 2];
    bytes[frame::IPV4_UDP_HEADER_LENGTH..].copy_from_slice(b"hi");
    let written = frame::write_ipv4_udp(
        &mut bytes,
        &frame::UdpWrite {
            ipv4: frame::Ipv4Write {
                destination_hardware: OUR_MAC,
                source_hardware: [0x02, 0, 0, 0, 0, 0x08],
                source: Ipv4Addr::new(10, 10, 0, 8),
                destination: OWN,
                identification: 3,
            },
            source_port: 40001,
            destination_port: 40464,
            payload_length: 2,
        },
    )
    .expect("a frame");
    machine.deliver(&bytes[..written]);
    machine.stack.pump();

    let received = received(&mut transport);
    assert_eq!(1, received.len(), "the peer it was moved to");
    assert_eq!(elsewhere, received[0].0);
}

/// G11: what the transport says it can hold is what it can hold — its ring, in
/// bytes.
#[test]
fn g11_the_window_is_what_the_port_would_hold() {
    let (_wire, machine) = machine();
    let transport = machine.open(40465).expect("a transport");

    let window = transport.receive_buffer_size().expect("a size");
    assert_eq!(
        machine.stack.params().ring_capacity() * machine.stack.params().mtu,
        window
    );
    assert!(window >= 128 * 1024, "at least the receive window");
}

/// G12: the port is free the moment the transport goes, and a new transport on
/// it does not inherit what the old one had not read.
#[test]
fn g12_the_port_comes_back_the_moment_the_transport_goes() {
    let (_wire, machine) = machine();
    let first = machine.open(40466).expect("a transport");

    machine.deliver_udp(40466, b"for the first one");
    machine.stack.pump();
    assert_eq!(1, machine.stack.bound_ports());

    drop(first);

    assert_eq!(0, machine.stack.bound_ports(), "the port is free");
    let mut second = machine.open(40466).expect("a transport on the same port");
    assert!(
        received(&mut second).is_empty(),
        "and what was waiting went with the first one"
    );
}

/// G13: the frame on the wire is the frame the kernel puts there.
///
/// The fixture is a capture, and what this compares is every field this stack
/// is responsible for — the two MACs, the two addresses, both ports, the
/// version, the TTL, the don't-fragment flag, the payload — plus the checksums,
/// which have to *verify* rather than to match, because the identification is
/// this stack's to choose and was the kernel's to have chosen. The byte-for-
/// byte comparison, identification included, is the frame module's own test,
/// where the identification is an input.
#[test]
fn g13_the_frame_on_the_wire_is_the_kernels() {
    // The capture's own ends: `tests/fixtures/README.md` has the recipe.
    const GOLDEN: &[u8] = include_bytes!("../../../tests/fixtures/frame/ipv4-udp.bin");
    const GOLDEN_PAYLOAD: &[u8] = b"deepmsg golden ipv4/udp frame!";
    const GOLDEN_SOURCE: Ipv4Addr = Ipv4Addr::new(10, 77, 0, 1);
    const GOLDEN_DESTINATION: Ipv4Addr = Ipv4Addr::new(10, 77, 0, 2);

    let wire = Wire::new();
    let machine = Machine::new(&wire, GOLDEN_SOURCE, GOLDEN_DESTINATION, OUR_MAC, PEER_MAC);
    let mut transport = machine.open(42001).expect("a transport");
    machine.stack.insert_neighbour(GOLDEN_DESTINATION, PEER_MAC);

    assert_eq!(
        1,
        transport
            .send(
                Some(SocketAddr::from((GOLDEN_DESTINATION, 40999))),
                &[GOLDEN_PAYLOAD]
            )
            .expect("a send")
    );

    let sent = machine.sent();
    assert_eq!(1, sent.len());
    let built = &sent[0];

    let golden = frame::read(GOLDEN, Checksums::Software).expect("a UDP frame");
    let ours = frame::read(built, Checksums::Software).expect("a UDP frame");

    assert_eq!(
        frame::read_ethernet_header(GOLDEN).expect("a header"),
        frame::read_ethernet_header(built).expect("a header"),
        "the MACs and the ethertype"
    );

    let (
        Received::Udp {
            ipv4: theirs,
            datagram: their_datagram,
        },
        Received::Udp {
            ipv4: ours_header,
            datagram: our_datagram,
        },
    ) = (golden, ours)
    else {
        panic!("two UDP frames");
    };

    // Everything but the two fields that are the sender's to choose: the
    // identification, and the checksum that covers it.
    assert_eq!(
        frame::Ipv4Header {
            identification: 0,
            checksum: 0,
            ..theirs
        },
        frame::Ipv4Header {
            identification: 0,
            checksum: 0,
            ..ours_header
        },
        "every IPv4 field but the identification"
    );
    assert_eq!(their_datagram.source_port, our_datagram.source_port);
    assert_eq!(
        their_datagram.destination_port,
        our_datagram.destination_port
    );
    assert_eq!(their_datagram.payload, our_datagram.payload);
    assert_eq!(GOLDEN_PAYLOAD, our_datagram.payload);
}

/// G14: an Aeron frame crosses with not one byte changed.
#[test]
fn g14_an_aeron_frame_crosses_unchanged() {
    let (_wire, machine) = machine();
    let mut transport = machine.open(40467).expect("a transport");

    // A frame header and a payload that is not text and not a round number.
    let aeron: Vec<u8> = (0..=255u8).chain(0..=255u8).collect();
    machine.deliver_udp(40467, &aeron);
    machine.stack.pump();

    assert_eq!(aeron, received(&mut transport)[0].1);
}
