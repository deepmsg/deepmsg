//! Ethernet II, IPv4, UDP, ICMP echo and ARP, as bytes on the wire.
//!
//! A kernel-bypass port hands over whole Ethernet frames, so everything the
//! kernel used to decide — is this datagram for me, is its header well formed,
//! what should its checksums be — is decided here instead. Two rules run
//! through the module:
//!
//! * **What this stack builds is what Linux builds.** The peer of this driver
//!   is very often a kernel UDP socket, so every field written here has to be a
//!   value that kernel accepts and answers. The values are pinned by
//!   `tests/fixtures/frame/ipv4-udp.bin`, a frame captured off a `veth` pair
//!   with the sending side's checksum offload off, which the encoder has to
//!   reproduce byte for byte (G13).
//! * **A frame it cannot place is counted, never guessed at.** IPv4 options,
//!   fragments, VLAN tags and protocols other than UDP and ICMP land on the
//!   reference's DPDK counters 89/90/93/94
//!   (`aeron-client/src/main/c/aeron_counters.h:167-172`) and are dropped here
//!   with a [`Discard`] that says which. Nothing is silently truncated and
//!   nothing is coerced into a shape it does not have.
//!
//! # The layout
//!
//! ```text
//!   0                   14              34            42
//!   | Ethernet II (14)  | IPv4 (20)     | UDP (8)     | payload …
//! ```
//!
//! [`IPV4_UDP_HEADER_LENGTH`] is the 42 bytes an Aeron frame travels behind,
//! and it is the offset the transport copies from: the Aeron frame itself is
//! never touched (G14).
//!
//! The checksums are RFC 1071's, computed with a 64-bit accumulator, and a
//! trailing odd byte is padded on the **right** — a rule that costs a test of
//! its own, because getting it backwards still produces a number, just not the
//! one the peer is looking for.

use std::net::Ipv4Addr;

/// Bytes of an Ethernet II header with no VLAN tag (a tag is `0x8100` at the
/// ethertype, and v1 counts those and drops them rather than parsing them).
pub const ETHERNET_HEADER_LENGTH: usize = 14;
/// Bytes of an IPv4 header with no options: version 4, IHL 5.
pub const IPV4_HEADER_LENGTH: usize = 20;
/// Bytes of a UDP header.
pub const UDP_HEADER_LENGTH: usize = 8;
/// Bytes of an ICMP echo header: type, code, checksum, identifier, sequence.
pub const ICMP_ECHO_HEADER_LENGTH: usize = 8;
/// Bytes of an ARP packet for IPv4 over Ethernet: the fixed 28, not counting
/// the Ethernet header in front of it.
pub const ARP_PACKET_LENGTH: usize = 28;
/// Ethernet + IPv4 + UDP, the bytes in front of an Aeron frame.
pub const IPV4_UDP_HEADER_LENGTH: usize =
    ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH + UDP_HEADER_LENGTH;

/// `ETH_P_IP`.
pub const ETHERNET_TYPE_IPV4: u16 = 0x0800;
/// `ETH_P_ARP`.
pub const ETHERNET_TYPE_ARP: u16 = 0x0806;
/// `ETH_P_8021Q`, counted as an unsupported ethertype: v1 has no VLAN.
pub const ETHERNET_TYPE_VLAN: u16 = 0x8100;

/// `IPPROTO_ICMP`.
pub const IP_PROTOCOL_ICMP: u8 = 1;
/// `IPPROTO_UDP`.
pub const IP_PROTOCOL_UDP: u8 = 17;

/// The hop limit outgoing unicast frames carry, which is Linux's
/// `ip_default_ttl` — and therefore what the frame in the fixture has.
pub const DEFAULT_TTL: u8 = 64;
/// `FF:FF:FF:FF:FF:FF`.
pub const BROADCAST: [u8; 6] = [0xff; 6];

/// `ARPOP_REQUEST`.
pub const ARP_REQUEST: u16 = 1;
/// `ARPOP_REPLY`.
pub const ARP_REPLY: u16 = 2;

/// `ICMP_ECHO`.
pub const ICMP_ECHO_REQUEST: u8 = 8;
/// `ICMP_ECHOREPLY`.
pub const ICMP_ECHO_REPLY: u8 = 0;

/// Why a frame was dropped, in the terms the counters are named in
/// (`aeron-client/src/main/c/aeron_counters.h:163-173`).
///
/// Each variant names where it is counted, because that mapping is the
/// contract: a frame this stack cannot place is one the reference's DPDK
/// transport cannot place either, and it goes in the same bucket. Two variants
/// are this stack's own call and say so.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discard {
    /// Not a whole header. No counter of its own: a frame this short is a runt
    /// the NIC would have dropped before anyone counted it.
    Short,
    /// An ethertype that is neither IPv4 nor ARP — a VLAN tag included, since
    /// v1 does not unwrap one. Counter 93,
    /// `AERON_COUNTER_DPDK_RX_UNSUPPORTED_ETHERNET_TYPE`.
    UnsupportedEthernetType,
    /// IPv4 with options (IHL other than 5), which this stack does not parse.
    /// Counted with the fragments (90,
    /// `AERON_COUNTER_DPDK_FRAGMENTED_PACKETS`): it is the same "a packet
    /// shape v1 does not serve" answer, and the reference has no counter of
    /// its own for an optioned header.
    Options,
    /// A fragment — MF set, or a non-zero fragment offset. Counter 90.
    Fragmented,
    /// IPv4 carrying something other than UDP or ICMP. Counter 94,
    /// `AERON_COUNTER_DPDK_RX_UNSUPPORTED_PROTOCOL`.
    UnsupportedProtocol,
    /// A checksum the port did not vouch for and the arithmetic did not
    /// confirm. Counter 89, `AERON_COUNTER_DPDK_CHECKSUM_FAILURE`.
    Checksum,
    /// A length field that does not describe a frame that could exist: a total
    /// length past the bytes that arrived, a UDP length that disagrees with
    /// it, an ARP packet whose fixed fields are not IPv4-over-Ethernet's.
    /// Counted as 93, by the same argument as [`Discard::Options`].
    Malformed,
}

/// Where a frame's checksum verdict comes from.
///
/// A NIC that offloads checksums hands the verdict over with the frame and
/// there is nothing to compute; one that does not leaves all of it to this
/// module (§4.5 of the design: 网口有判定就用，没有就软件验).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checksums {
    /// The port already checked them, and passed the frame.
    Verified,
    /// Nobody has: check with RFC 1071.
    Software,
}

/// Who writes a frame's checksums — the writing twin of [`Checksums`], which
/// says who *judged* a frame that arrived.
///
/// A frame's author has to know which of the two it is before it writes a byte:
/// the two modes put **different values in the same two fields**, and a frame
/// that mixes them is one whose checksums are wrong on the wire and nowhere
/// else — nothing on this side reads them back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum L4Checksum {
    /// This stack computes both, and what it writes is what the receiver will
    /// find.
    Software,
    /// The NIC computes both, which changes what goes in the fields: the IPv4
    /// header's is left zero, and the datagram's holds the **pseudo-header**
    /// sum, non-complemented, because that is what the hardware adds the rest
    /// of the datagram to (`/usr/include/dpdk/rte_ip.h:313-328`, "the
    /// non-complemented checksum to set in the L4 header").
    ByTheNic,
}

/// What a frame asks the NIC to compute.
///
/// **Only a frame builder makes one of these**, which is the whole point: the
/// flags a frame travels with and the bytes it carries are two halves of one
/// decision, and a value that could be made up anywhere would be a way to get
/// them apart. The failure that buys is silent and ugly — a frame with the
/// pseudo-header sum in it and no offload behind it is a frame with a wrong
/// checksum, and an ARP frame with a UDP offload on it is a frame nobody
/// resolves.
///
/// A port translates this into what DPDK's own crate wants
/// (`media/userspace/dpdk_port.rs`), which is where the flags and the header
/// lengths become an `ol_flags` and two mbuf fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxOffloads {
    /// Whether the NIC is to compute both checksums of this frame.
    compute: bool,
    /// The Ethernet header's length, read only when `compute` is set.
    l2_len: u16,
    /// The IPv4 header's length, likewise.
    l3_len: u16,
}

impl TxOffloads {
    /// A frame the NIC computes nothing for: every checksum in it was written
    /// here, and is the one the receiver will find.
    pub const NONE: Self = Self {
        compute: false,
        l2_len: 0,
        l3_len: 0,
    };

    /// What a port needs to translate this into its own vocabulary: whether
    /// anything is asked for, and the two header lengths that go with the ask.
    pub(crate) const fn asked(self) -> (bool, u16, u16) {
        (self.compute, self.l2_len, self.l3_len)
    }
}

/// An Ethernet II header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EthernetHeader {
    /// The MAC the frame is addressed to.
    pub destination: [u8; 6],
    /// The MAC it came from.
    pub source: [u8; 6],
    /// [`ETHERNET_TYPE_IPV4`], [`ETHERNET_TYPE_ARP`], or something this stack
    /// does not serve.
    pub ether_type: u16,
}

impl EthernetHeader {
    /// Whether a frame carrying this header is for the port whose MAC is
    /// `mac`.
    ///
    /// A frame addressed anywhere else is not this port's, and the rule is to
    /// drop it **silently** — no counter, because a port that is not in
    /// promiscuous mode is never handed it in the first place (§4.5:
    /// 目的 MAC 既非本机、非广播、非已加入组播 → 静默丢).
    pub fn is_for(&self, mac: [u8; 6]) -> bool {
        self.destination == mac || self.destination == BROADCAST
    }
}

/// An IPv4 header with no options: what it says about the packet behind it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ipv4Header {
    /// The header's own length in bytes, which this stack only ever sees as
    /// [`IPV4_HEADER_LENGTH`] — anything else is a [`Discard::Options`].
    pub header_length: usize,
    /// Bytes of header plus payload.
    pub total_length: usize,
    /// The identification field: filled by the sender, read here only to
    /// reproduce a captured frame.
    pub identification: u16,
    /// The flags and fragment offset word, as it travels.
    pub flags_and_fragment_offset: u16,
    /// The hop limit.
    pub ttl: u8,
    /// [`IP_PROTOCOL_UDP`], [`IP_PROTOCOL_ICMP`], or a [`Discard`].
    pub protocol: u8,
    /// The header checksum, as it arrived.
    pub checksum: u16,
    /// The sender's address.
    pub source: Ipv4Addr,
    /// The address the packet is for.
    pub destination: Ipv4Addr,
}

impl Ipv4Header {
    /// How many bytes follow the header.
    pub fn payload_length(&self) -> usize {
        self.total_length - self.header_length
    }
}

/// A UDP datagram: the two ports, and the bytes behind the 8-byte header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpDatagram<'a> {
    /// The sending port.
    pub source_port: u16,
    /// The port the datagram is for.
    pub destination_port: u16,
    /// The datagram's own length field, header included.
    pub length: usize,
    /// What is behind the header — an Aeron frame, in this stack.
    pub payload: &'a [u8],
}

/// An ARP packet for IPv4 over Ethernet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArpPacket {
    /// [`ARP_REQUEST`] or [`ARP_REPLY`].
    pub operation: u16,
    /// The sender's MAC.
    pub sender_hardware: [u8; 6],
    /// The sender's IPv4 address.
    pub sender_protocol: Ipv4Addr,
    /// The target's MAC — all zeroes in a request, which is the question.
    pub target_hardware: [u8; 6],
    /// The target's IPv4 address.
    pub target_protocol: Ipv4Addr,
}

/// An ICMP echo message: the header's identifier and sequence, and what it
/// carried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IcmpEcho<'a> {
    /// Echo request or echo reply.
    pub echo_type: u8,
    /// The identifier its sender chose.
    pub identifier: u16,
    /// The sequence number.
    pub sequence: u16,
    /// The bytes after the 8-byte header, echoed back unchanged.
    pub payload: &'a [u8],
}

/// What a received frame turned out to be: the two shapes this stack serves,
/// and the one it answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Received<'a> {
    /// An IPv4/UDP datagram, for a port some transport bound.
    Udp {
        /// The header, which carries the sender's address.
        ipv4: Ipv4Header,
        /// The datagram.
        datagram: UdpDatagram<'a>,
    },
    /// An ICMP echo request, which this stack answers: with the port bound to
    /// DPDK there is no kernel to `ping`, so this is the only way to tell a
    /// live link from a dead one (§4.5).
    Echo {
        /// The header, which carries the sender's address.
        ipv4: Ipv4Header,
        /// The request.
        echo: IcmpEcho<'a>,
    },
    /// An ARP packet.
    Arp(ArpPacket),
}

/// Everything an outgoing IPv4 frame needs that is not its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ipv4Write {
    /// The MAC to address the frame to.
    pub destination_hardware: [u8; 6],
    /// This port's MAC.
    pub source_hardware: [u8; 6],
    /// This port's address.
    pub source: Ipv4Addr,
    /// The address the frame is for.
    pub destination: Ipv4Addr,
    /// The identification field, which the caller supplies rather than this
    /// module deriving: Linux fills it from a pool of its own, and the fixture
    /// is a frame Linux built (§4.5).
    pub identification: u16,
}

/// Everything an outgoing IPv4/UDP frame needs that is not its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpWrite {
    /// The IPv4 layer.
    pub ipv4: Ipv4Write,
    /// The sending port.
    pub source_port: u16,
    /// The receiving port.
    pub destination_port: u16,
    /// How many payload bytes are **already written** at
    /// [`IPV4_UDP_HEADER_LENGTH`] in the buffer being written into.
    pub payload_length: usize,
}

/// Read the Ethernet header of `frame`.
///
/// # Errors
///
/// [`Discard::Short`] when there are not fourteen bytes.
pub fn read_ethernet_header(frame: &[u8]) -> Result<EthernetHeader, Discard> {
    let header = frame.get(..ETHERNET_HEADER_LENGTH).ok_or(Discard::Short)?;

    Ok(EthernetHeader {
        destination: read_array(header, 0).ok_or(Discard::Short)?,
        source: read_array(header, 6).ok_or(Discard::Short)?,
        ether_type: read_u16(header, 12).ok_or(Discard::Short)?,
    })
}

/// Write the Ethernet header of `frame`.
///
/// Answers [`ETHERNET_HEADER_LENGTH`], or `None` when the buffer is too small.
pub fn write_ethernet_header(buffer: &mut [u8], header: &EthernetHeader) -> Option<usize> {
    let bytes = buffer.get_mut(..ETHERNET_HEADER_LENGTH)?;
    bytes[0..6].copy_from_slice(&header.destination);
    bytes[6..12].copy_from_slice(&header.source);
    bytes[12..14].copy_from_slice(&header.ether_type.to_be_bytes());

    Some(ETHERNET_HEADER_LENGTH)
}

/// Read the IPv4 header of a packet, without judging its checksum.
///
/// # Errors
///
/// [`Discard::Short`] when there is not a whole header, [`Discard::Options`]
/// for an IHL other than 5, [`Discard::Fragmented`] for a packet with MF set
/// or a fragment offset, [`Discard::Malformed`] when the total length does not
/// describe the bytes that arrived.
pub fn read_ipv4_header(packet: &[u8]) -> Result<Ipv4Header, Discard> {
    let fixed = packet.get(..IPV4_HEADER_LENGTH).ok_or(Discard::Short)?;

    if fixed[0] >> 4 != 4 {
        return Err(Discard::Malformed);
    }

    let header_length = usize::from(fixed[0] & 0x0f) * 4;
    if header_length != IPV4_HEADER_LENGTH {
        return Err(Discard::Options);
    }

    let flags_and_fragment_offset = read_u16(fixed, 6).ok_or(Discard::Short)?;
    // MF (`0x2000`) or a non-zero offset: a piece of a datagram, which v1 does
    // not reassemble.
    if flags_and_fragment_offset & 0x3fff != 0 {
        return Err(Discard::Fragmented);
    }

    let total_length = usize::from(read_u16(fixed, 2).ok_or(Discard::Short)?);
    if total_length < header_length || total_length > packet.len() {
        return Err(Discard::Malformed);
    }

    Ok(Ipv4Header {
        header_length,
        total_length,
        identification: read_u16(fixed, 4).ok_or(Discard::Short)?,
        flags_and_fragment_offset,
        ttl: fixed[8],
        protocol: fixed[9],
        checksum: read_u16(fixed, 10).ok_or(Discard::Short)?,
        source: Ipv4Addr::from(read_array::<4>(fixed, 12).ok_or(Discard::Short)?),
        destination: Ipv4Addr::from(read_array::<4>(fixed, 16).ok_or(Discard::Short)?),
    })
}

/// Read a UDP header and the bytes behind it.
///
/// # Errors
///
/// [`Discard::Short`] when there is not a whole header, [`Discard::Malformed`]
/// when the length field does not describe the datagram that arrived.
pub fn read_udp(datagram: &[u8]) -> Result<UdpDatagram<'_>, Discard> {
    let header = datagram.get(..UDP_HEADER_LENGTH).ok_or(Discard::Short)?;
    let length = usize::from(read_u16(header, 4).ok_or(Discard::Short)?);
    let payload = datagram
        .get(UDP_HEADER_LENGTH..length)
        .ok_or(Discard::Malformed)?;

    Ok(UdpDatagram {
        source_port: read_u16(header, 0).ok_or(Discard::Short)?,
        destination_port: read_u16(header, 2).ok_or(Discard::Short)?,
        length,
        payload,
    })
}

/// Read an ARP packet.
///
/// Answers `None` for an ARP packet this stack does not model — one that is
/// not IPv4 over Ethernet, or whose hardware or protocol length is not 6 and
/// 4. The caller drops that without a counter: it is a well-formed packet the
/// reference has no bucket for either.
pub fn read_arp(packet: &[u8]) -> Option<ArpPacket> {
    let fixed = packet.get(..ARP_PACKET_LENGTH)?;

    if read_u16(fixed, 0)? != 1
        || read_u16(fixed, 2)? != ETHERNET_TYPE_IPV4
        || fixed[4] != 6
        || fixed[5] != 4
    {
        return None;
    }

    Some(ArpPacket {
        operation: read_u16(fixed, 6)?,
        sender_hardware: read_array(fixed, 8)?,
        sender_protocol: Ipv4Addr::from(read_array::<4>(fixed, 14)?),
        target_hardware: read_array(fixed, 18)?,
        target_protocol: Ipv4Addr::from(read_array::<4>(fixed, 24)?),
    })
}

/// Read an ICMP echo message, request or reply.
///
/// # Errors
///
/// [`Discard::Short`] under eight bytes, [`Discard::UnsupportedProtocol`] for
/// an ICMP message that is not an echo — a destination-unreachable or a
/// time-exceeded is dropped, not answered (§4.5: 其余 ICMP 丢).
pub fn read_icmp_echo(message: &[u8]) -> Result<IcmpEcho<'_>, Discard> {
    let header = message
        .get(..ICMP_ECHO_HEADER_LENGTH)
        .ok_or(Discard::Short)?;

    let echo_type = header[0];
    if echo_type != ICMP_ECHO_REQUEST && echo_type != ICMP_ECHO_REPLY {
        return Err(Discard::UnsupportedProtocol);
    }

    Ok(IcmpEcho {
        echo_type,
        identifier: read_u16(header, 4).ok_or(Discard::Short)?,
        sequence: read_u16(header, 6).ok_or(Discard::Short)?,
        payload: &message[ICMP_ECHO_HEADER_LENGTH..],
    })
}

/// Parse a whole received frame as far as this stack takes it.
///
/// This is the one entry point the demultiplexer uses, and it is where the
/// discard counters are decided: everything a frame can be wrong about comes
/// back as a [`Discard`] naming its bucket.
///
/// # Errors
///
/// See [`Discard`].
pub fn read(frame: &[u8], checksums: Checksums) -> Result<Received<'_>, Discard> {
    let ethernet = read_ethernet_header(frame)?;
    let body = frame.get(ETHERNET_HEADER_LENGTH..).ok_or(Discard::Short)?;

    match ethernet.ether_type {
        ETHERNET_TYPE_ARP => return Ok(Received::Arp(read_arp(body).ok_or(Discard::Malformed)?)),
        ETHERNET_TYPE_IPV4 => {}
        _ => return Err(Discard::UnsupportedEthernetType),
    }

    let ipv4 = read_ipv4_header(body)?;

    if checksums == Checksums::Software && !ipv4_checksum_is_valid(&body[..IPV4_HEADER_LENGTH]) {
        return Err(Discard::Checksum);
    }

    let payload = body
        .get(IPV4_HEADER_LENGTH..ipv4.total_length)
        .ok_or(Discard::Malformed)?;

    match ipv4.protocol {
        IP_PROTOCOL_UDP => {
            let datagram = read_udp(payload)?;

            if checksums == Checksums::Software
                && !udp_checksum_is_valid(ipv4.source, ipv4.destination, payload)
            {
                return Err(Discard::Checksum);
            }

            Ok(Received::Udp { ipv4, datagram })
        }
        IP_PROTOCOL_ICMP => {
            let echo = read_icmp_echo(payload)?;

            if checksums == Checksums::Software && !icmp_checksum_is_valid(payload) {
                return Err(Discard::Checksum);
            }

            Ok(Received::Echo { ipv4, echo })
        }
        _ => Err(Discard::UnsupportedProtocol),
    }
}

/// Write a whole IPv4/UDP frame in front of a payload that is already in the
/// buffer.
///
/// The payload is read from — and must already be at — offset
/// [`IPV4_UDP_HEADER_LENGTH`]. That is what makes this the builder for a send
/// path that copies its payload once, into the buffer the port hands the NIC.
///
/// Answers the frame's total length, or `None` when `buffer` cannot hold it.
/// [`write_ipv4_udp`] with this stack writing both checksums, for every caller
/// that is not a port whose NIC computes them.
///
/// **Not a way to choose the mode**: it cannot ask for the offload, so the one
/// direction that is dangerous — a frame whose author wrote the pseudo-header
/// sum and whose port then computes nothing — is still reachable only through a
/// port's own answer ([`L4Checksum`]). What it is for is the case a test and a
/// frame builder both have: a frame whose checksums are all written here, and a
/// length to hand back rather than a length and an ask.
pub fn write_ipv4_udp_here(buffer: &mut [u8], frame: &UdpWrite) -> Option<usize> {
    write_ipv4_udp(buffer, frame, L4Checksum::Software).map(|(written, _)| written)
}

pub fn write_ipv4_udp(
    buffer: &mut [u8],
    frame: &UdpWrite,
    checksums: L4Checksum,
) -> Option<(usize, TxOffloads)> {
    let total = IPV4_UDP_HEADER_LENGTH.checked_add(frame.payload_length)?;
    if buffer.len() < total {
        return None;
    }

    write_ipv4_header(
        buffer,
        &frame.ipv4,
        IP_PROTOCOL_UDP,
        total - ETHERNET_HEADER_LENGTH,
        checksums,
    )?;

    let datagram = buffer.get_mut(ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH..total)?;
    datagram[0..2].copy_from_slice(&frame.source_port.to_be_bytes());
    datagram[2..4].copy_from_slice(&frame.destination_port.to_be_bytes());
    datagram[4..6].copy_from_slice(
        &u16::try_from(UDP_HEADER_LENGTH.checked_add(frame.payload_length)?)
            .ok()?
            .to_be_bytes(),
    );

    // **The two modes write different values into the same field**, and this is
    // the only place that decides which: the whole checksum when this stack
    // computes it, and the pseudo-header sum — non-complemented, so the
    // hardware can add the rest — when the NIC is to.
    let offloads = match checksums {
        L4Checksum::Software => {
            datagram[6..8].copy_from_slice(&[0, 0]);
            let checksum = udp_checksum(frame.ipv4.source, frame.ipv4.destination, datagram);
            datagram[6..8].copy_from_slice(&checksum.to_be_bytes());

            TxOffloads::NONE
        }
        L4Checksum::ByTheNic => {
            let pseudo = pseudo_header_sum(frame.ipv4.source, frame.ipv4.destination, datagram);
            datagram[6..8].copy_from_slice(&folded(pseudo).to_be_bytes());

            TxOffloads {
                compute: true,
                l2_len: u16::try_from(ETHERNET_HEADER_LENGTH).ok()?,
                l3_len: u16::try_from(IPV4_HEADER_LENGTH).ok()?,
            }
        }
    };

    Some((total, offloads))
}

/// Write a whole ARP frame — a request or a reply, whichever `packet` says.
///
/// Answers the frame's length, or `None` when `buffer` cannot hold it.
pub fn write_arp(
    buffer: &mut [u8],
    ethernet: &EthernetHeader,
    packet: &ArpPacket,
) -> Option<usize> {
    let total = ETHERNET_HEADER_LENGTH + ARP_PACKET_LENGTH;
    if buffer.len() < total {
        return None;
    }

    write_ethernet_header(buffer, ethernet)?;

    let arp = buffer.get_mut(ETHERNET_HEADER_LENGTH..total)?;
    arp[0..2].copy_from_slice(&1u16.to_be_bytes());
    arp[2..4].copy_from_slice(&ETHERNET_TYPE_IPV4.to_be_bytes());
    arp[4] = 6;
    arp[5] = 4;
    arp[6..8].copy_from_slice(&packet.operation.to_be_bytes());
    arp[8..14].copy_from_slice(&packet.sender_hardware);
    arp[14..18].copy_from_slice(&packet.sender_protocol.octets());
    arp[18..24].copy_from_slice(&packet.target_hardware);
    arp[24..28].copy_from_slice(&packet.target_protocol.octets());

    Some(total)
}

/// Write a whole ICMP echo **reply** to `request`, addressed back to whoever
/// sent it.
///
/// The identifier, the sequence number and the payload are echoed unchanged:
/// that is the contract of `ping`, and a reply that changes any of them is one
/// a `ping` reports as an error rather than as a round trip.
///
/// Answers the frame's length, or `None` when `buffer` cannot hold it.
pub fn write_icmp_echo_reply(
    buffer: &mut [u8],
    frame: &Ipv4Write,
    request: &IcmpEcho<'_>,
) -> Option<usize> {
    write_icmp_echo(buffer, frame, request, ICMP_ECHO_REPLY)
}

/// The Internet checksum of `bytes` (RFC 1071), with a 64-bit accumulator.
///
/// The sixteen-bit words are summed in network order and the carries folded
/// back in at the end. A trailing odd byte is padded with a zero on the
/// **right**, so it is the high half of the last word.
pub fn internet_checksum(bytes: &[u8]) -> u16 {
    fold(partial_sum(bytes))
}

/// The checksum of UDP's pseudo-header plus `datagram`, which is what a UDP
/// checksum is over (RFC 768): the two addresses, a zero, the protocol, the
/// datagram's length, then the datagram itself with its checksum field zero.
///
/// A result of zero is returned as `0xffff`, because a zero checksum on the
/// wire means "not computed" — the one value that has to be mapped.
pub fn udp_checksum(source: Ipv4Addr, destination: Ipv4Addr, datagram: &[u8]) -> u16 {
    match fold(pseudo_header_sum(source, destination, datagram) + partial_sum(datagram)) {
        0 => u16::MAX,
        checksum => checksum,
    }
}

/// Whether an IPv4 header's checksum is the one it should be: summing a header
/// that already carries it comes to zero.
pub fn ipv4_checksum_is_valid(header: &[u8]) -> bool {
    internet_checksum(header) == 0
}

/// Whether a UDP datagram — header and payload, checksum field as it arrived —
/// carries the checksum it should.
///
/// Deliberately not a check for a zero field: RFC 768 lets a sender skip the
/// checksum on IPv4, and the kernel does not.
pub fn udp_checksum_is_valid(source: Ipv4Addr, destination: Ipv4Addr, datagram: &[u8]) -> bool {
    if datagram.len() < UDP_HEADER_LENGTH {
        return false;
    }

    // Summed with the checksum field still in it, so the whole thing comes to
    // all ones — the same property the IPv4 header's own check is.
    fold(pseudo_header_sum(source, destination, datagram) + partial_sum(datagram)) == 0
}

/// Whether an ICMP message carries the checksum it should.
pub fn icmp_checksum_is_valid(message: &[u8]) -> bool {
    internet_checksum(message) == 0
}

/// The sums a UDP checksum starts from: the two addresses, a zero, the
/// protocol, and the datagram's length.
fn pseudo_header_sum(source: Ipv4Addr, destination: Ipv4Addr, datagram: &[u8]) -> u64 {
    partial_sum(&source.octets())
        + partial_sum(&destination.octets())
        + u64::from(IP_PROTOCOL_UDP)
        + u64::from(u16::try_from(datagram.len()).unwrap_or(u16::MAX))
}

/// Write the Ethernet and IPv4 headers of a frame whose payload begins at
/// [`ETHERNET_HEADER_LENGTH`] + [`IPV4_HEADER_LENGTH`], checksum included.
fn write_ipv4_header(
    buffer: &mut [u8],
    frame: &Ipv4Write,
    protocol: u8,
    total_length: usize,
    checksums: L4Checksum,
) -> Option<()> {
    write_ethernet_header(
        buffer,
        &EthernetHeader {
            destination: frame.destination_hardware,
            source: frame.source_hardware,
            ether_type: ETHERNET_TYPE_IPV4,
        },
    )?;

    let ipv4 =
        buffer.get_mut(ETHERNET_HEADER_LENGTH..ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH)?;
    ipv4[0] = 0x45;
    ipv4[1] = 0;
    ipv4[2..4].copy_from_slice(&u16::try_from(total_length).ok()?.to_be_bytes());
    ipv4[4..6].copy_from_slice(&frame.identification.to_be_bytes());
    // Don't-fragment: this stack never fragments, and the reference drops what
    // arrives fragmented, so a frame it sends must not come back as one.
    ipv4[6..8].copy_from_slice(&0x4000u16.to_be_bytes());
    ipv4[8] = DEFAULT_TTL;
    ipv4[9] = protocol;
    ipv4[10..12].copy_from_slice(&[0, 0]);
    ipv4[12..16].copy_from_slice(&frame.source.octets());
    ipv4[16..20].copy_from_slice(&frame.destination.octets());
    // The header's own checksum is the second field the mode decides: left zero
    // when the NIC computes it, which is what `RTE_MBUF_F_TX_IP_CKSUM` promises
    // it will do.
    let checksum = match checksums {
        L4Checksum::Software => internet_checksum(ipv4),
        L4Checksum::ByTheNic => 0,
    };
    ipv4[10..12].copy_from_slice(&checksum.to_be_bytes());

    Some(())
}

/// Write an ICMP echo frame of `echo_type`.
///
/// A reply is one byte of this and a request is another, so the two are the
/// same encoder — and a test that needs a *request* on the wire (an echo
/// request is what a ping sends, and what this stack answers) needs it too.
pub(crate) fn write_icmp_echo(
    buffer: &mut [u8],
    frame: &Ipv4Write,
    echo: &IcmpEcho<'_>,
    echo_type: u8,
) -> Option<usize> {
    let total_length = IPV4_HEADER_LENGTH
        .checked_add(ICMP_ECHO_HEADER_LENGTH)?
        .checked_add(echo.payload.len())?;
    let total = ETHERNET_HEADER_LENGTH.checked_add(total_length)?;
    if buffer.len() < total {
        return None;
    }

    // ICMP is not one of the two checksums a NIC is asked to compute here: the
    // offload this build asks about is IPv4's header and UDP's datagram, so an
    // ICMP reply's checksum is always this stack's (`L4Checksum::Software`
    // names that, and not a mode this builder has).
    write_ipv4_header(
        buffer,
        frame,
        IP_PROTOCOL_ICMP,
        total_length,
        L4Checksum::Software,
    )?;

    let message = buffer.get_mut(ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH..total)?;
    message[0] = echo_type;
    message[1] = 0;
    message[2..4].copy_from_slice(&[0, 0]);
    message[4..6].copy_from_slice(&echo.identifier.to_be_bytes());
    message[6..8].copy_from_slice(&echo.sequence.to_be_bytes());
    message[ICMP_ECHO_HEADER_LENGTH..].copy_from_slice(echo.payload);
    let checksum = internet_checksum(message);
    message[2..4].copy_from_slice(&checksum.to_be_bytes());

    Some(total)
}

/// Sum of every sixteen-bit word in `bytes`, as a sixty-four-bit total.
fn partial_sum(bytes: &[u8]) -> u64 {
    let mut sum = 0u64;

    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        // Read as big-endian so the four sixteen-bit words inside land in
        // their network-order positions, then take them apart into the
        // accumulator: one add per eight bytes, and the carries between the
        // words are captured here rather than lost at the top of the register.
        let words = u64::from_be_bytes(chunk.try_into().unwrap_or([0; 8]));
        sum +=
            (words & 0xffff) + ((words >> 16) & 0xffff) + ((words >> 32) & 0xffff) + (words >> 48);
    }

    let mut tail = chunks.remainder();
    if let Some((head, rest)) = tail.split_at_checked(4) {
        let words = u32::from_be_bytes(head.try_into().unwrap_or([0; 4]));
        sum += u64::from((words >> 16) + (words & 0xffff));
        tail = rest;
    }
    if let Some((head, rest)) = tail.split_at_checked(2) {
        sum += u64::from(u16::from_be_bytes(head.try_into().unwrap_or([0; 2])));
        tail = rest;
    }
    if let [last] = tail {
        // Padded on the right: the odd byte is the high half of its word.
        sum += u64::from(*last) << 8;
    }

    sum
}

/// Fold a sixty-four-bit sum down to sixteen bits and complement it.
/// The carries of a one's-complement sum folded down, and left **as they are**.
///
/// This is the half that a NIC wants to find: a hardware checksum engine adds
/// the datagram to the value in the field, so the field holds the folded sum
/// and not its complement (`/usr/include/dpdk/rte_ip.h:328`, "the
/// non-complemented checksum to set in the L4 header").
fn folded(mut sum: u64) -> u16 {
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }

    sum as u16
}

fn fold(sum: u64) -> u16 {
    !folded(sum)
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Option<[u8; N]> {
    bytes.get(offset..offset + N)?.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frame Linux built for `deepmsg golden ipv4/udp frame!` between two
    /// `veth` ends, captured with the sending side's checksum offload off
    /// (`tests/fixtures/README.md` has the recipe and every field).
    const GOLDEN_IPV4_UDP: &[u8] =
        include_bytes!("../../../../../tests/fixtures/frame/ipv4-udp.bin");
    /// The ARP request Linux built, asking for `10.77.0.3`.
    const GOLDEN_ARP_REQUEST: &[u8] =
        include_bytes!("../../../../../tests/fixtures/frame/arp-request.bin");

    const GOLDEN_PAYLOAD: &[u8] = b"deepmsg golden ipv4/udp frame!";

    fn golden_udp_write(payload_length: usize, identification: u16) -> UdpWrite {
        UdpWrite {
            ipv4: Ipv4Write {
                destination_hardware: [0x02, 0, 0, 0, 0, 0x02],
                source_hardware: [0x02, 0, 0, 0, 0, 0x01],
                source: Ipv4Addr::new(10, 77, 0, 1),
                destination: Ipv4Addr::new(10, 77, 0, 2),
                identification,
            },
            source_port: 12001,
            destination_port: 10999,
            payload_length,
        }
    }

    /// **The two modes write different values into the same two fields**, and
    /// the difference is the whole reason a frame's author has to know which one
    /// it is writing: nothing on this side reads a sent frame back, so a frame
    /// that mixed them would be a wrong checksum on the wire and nowhere else.
    ///
    /// The second half of this is the convention itself, and it is checked by
    /// **playing the NIC**: hardware computes `~folded(field + the rest of the
    /// datagram)` (`/usr/include/dpdk/rte_ip.h:313-328`, "the
    /// non-complemented checksum to set in the L4 header"), so a frame written
    /// for it becomes a frame that verifies *if and only if* the field it was
    /// given holds that non-complemented pseudo-header sum. Getting the
    /// complement backwards — the easy mistake — makes this the assertion that
    /// fails.
    #[test]
    fn a_frame_written_for_the_nic_becomes_a_frame_that_verifies() {
        let payload = b"the nic computes what is in this field";
        let write = golden_udp_write(payload.len(), 7);
        let of = |frame: &[u8], written: usize| {
            frame[ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH..written].to_vec()
        };

        // What this stack writes: a checksum, and nothing asked of anybody.
        let mut here = vec![0u8; IPV4_UDP_HEADER_LENGTH + payload.len()];
        let (written, offloads) =
            write_ipv4_udp(&mut here, &write, L4Checksum::Software).expect("a frame");
        assert_eq!(
            TxOffloads::NONE.asked(),
            offloads.asked(),
            "a frame this stack wrote asks the NIC for nothing"
        );
        assert!(
            udp_checksum_is_valid(
                write.ipv4.source,
                write.ipv4.destination,
                &of(&here, written)
            ),
            "and it verifies as it stands"
        );
        assert!(
            ipv4_checksum_is_valid(
                &here[ETHERNET_HEADER_LENGTH..ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH]
            ),
            "its header's checksum too"
        );

        // What it writes for a NIC: the pseudo-header sum, and the ask.
        let mut by_the_nic = vec![0u8; IPV4_UDP_HEADER_LENGTH + payload.len()];
        let (written, offloads) =
            write_ipv4_udp(&mut by_the_nic, &write, L4Checksum::ByTheNic).expect("a frame");
        let (asks, l2_len, l3_len) = offloads.asked();
        assert!(
            asks,
            "a frame written for the NIC asks it for the checksums"
        );
        assert_eq!(
            (ETHERNET_HEADER_LENGTH as u16, IPV4_HEADER_LENGTH as u16),
            (l2_len, l3_len),
            "and carries the header lengths the offload's contract wants"
        );

        let for_the_nic = of(&by_the_nic, written);
        assert!(
            !udp_checksum_is_valid(write.ipv4.source, write.ipv4.destination, &for_the_nic),
            "the field is not a finished checksum, so it does not verify as one"
        );
        assert_ne!(
            of(&here, here.len()),
            for_the_nic,
            "and the two modes really do write different bytes"
        );
        assert_eq!(
            [0, 0],
            by_the_nic[ETHERNET_HEADER_LENGTH + 10..ETHERNET_HEADER_LENGTH + 12],
            "the IPv4 header's checksum is the NIC's to write, and is left zero"
        );

        // Now the NIC does what DPDK says it does, and the frame verifies.
        let field = u16::from_be_bytes([for_the_nic[6], for_the_nic[7]]);
        let mut rest = for_the_nic.clone();
        rest[6..8].fill(0);
        let computed = fold(u64::from(field) + partial_sum(&rest));

        let mut done = for_the_nic.clone();
        done[6..8].copy_from_slice(&computed.to_be_bytes());
        assert!(
            udp_checksum_is_valid(write.ipv4.source, write.ipv4.destination, &done),
            "what the NIC computes from the pseudo-header sum is the checksum the \
             receiver wants: the convention is the non-complemented sum"
        );
    }

    #[test]
    fn the_goldens_are_whole_frames() {
        assert_eq!(
            IPV4_UDP_HEADER_LENGTH + GOLDEN_PAYLOAD.len(),
            GOLDEN_IPV4_UDP.len()
        );
        assert_eq!(
            ETHERNET_HEADER_LENGTH + ARP_PACKET_LENGTH,
            GOLDEN_ARP_REQUEST.len()
        );
    }

    /// G13: what this module builds is byte for byte what the kernel built.
    ///
    /// The captured frame is parsed for the fields the encoder is *given* —
    /// the identification included — rebuilt from them, and compared with the
    /// capture. Nothing of the header is passed through: the checksums in the
    /// rebuilt frame are the ones computed here.
    #[test]
    fn the_golden_frame_is_reproduced_byte_for_byte() {
        let Received::Udp { ipv4, datagram } =
            read(GOLDEN_IPV4_UDP, Checksums::Software).expect("a UDP frame")
        else {
            panic!("the golden is an IPv4/UDP frame");
        };
        assert_eq!(GOLDEN_PAYLOAD, datagram.payload, "its payload is intact");

        let ethernet = read_ethernet_header(GOLDEN_IPV4_UDP).expect("a header");
        let mut rebuilt = vec![0u8; GOLDEN_IPV4_UDP.len()];
        rebuilt[IPV4_UDP_HEADER_LENGTH..].copy_from_slice(datagram.payload);

        let written = write_ipv4_udp_here(
            &mut rebuilt,
            &UdpWrite {
                ipv4: Ipv4Write {
                    destination_hardware: ethernet.destination,
                    source_hardware: ethernet.source,
                    source: ipv4.source,
                    destination: ipv4.destination,
                    identification: ipv4.identification,
                },
                source_port: datagram.source_port,
                destination_port: datagram.destination_port,
                payload_length: datagram.payload.len(),
            },
        )
        .expect("a frame");

        assert_eq!(GOLDEN_IPV4_UDP.len(), written);
        assert_eq!(GOLDEN_IPV4_UDP, rebuilt.as_slice());
    }

    /// Every field of the golden, spelled out — so that a rebuild which drifts
    /// says which field drifted rather than only that the bytes differ.
    #[test]
    fn the_golden_carries_the_fields_the_recipe_says_it_does() {
        let ethernet = read_ethernet_header(GOLDEN_IPV4_UDP).expect("a header");
        assert_eq!([0x02, 0, 0, 0, 0, 0x02], ethernet.destination);
        assert_eq!([0x02, 0, 0, 0, 0, 0x01], ethernet.source);
        assert_eq!(ETHERNET_TYPE_IPV4, ethernet.ether_type);

        let ipv4 = read_ipv4_header(&GOLDEN_IPV4_UDP[ETHERNET_HEADER_LENGTH..]).expect("IPv4");
        assert_eq!(0, GOLDEN_IPV4_UDP[ETHERNET_HEADER_LENGTH + 1], "TOS 0");
        assert_eq!(58, ipv4.total_length);
        assert_eq!(0x4000, ipv4.flags_and_fragment_offset, "don't fragment");
        assert_eq!(DEFAULT_TTL, ipv4.ttl);
        assert_eq!(IP_PROTOCOL_UDP, ipv4.protocol);
        assert_eq!(Ipv4Addr::new(10, 77, 0, 1), ipv4.source);
        assert_eq!(Ipv4Addr::new(10, 77, 0, 2), ipv4.destination);

        let Received::Udp { datagram, .. } =
            read(GOLDEN_IPV4_UDP, Checksums::Software).expect("a UDP frame")
        else {
            panic!("a UDP frame");
        };
        assert_eq!(42001, datagram.source_port);
        assert_eq!(40999, datagram.destination_port);
        assert_eq!(38, datagram.length);
    }

    /// The checksums in the golden are real ones — which is the whole reason it
    /// was captured off a `veth` rather than off `lo`, where the kernel leaves
    /// the pseudo-header sum in the field and cannot be talked out of it
    /// (`ethtool -k lo`: `tx-checksum-ip-generic: on [fixed]`).
    #[test]
    fn the_goldens_checksums_verify() {
        let header =
            &GOLDEN_IPV4_UDP[ETHERNET_HEADER_LENGTH..ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH];
        assert!(ipv4_checksum_is_valid(header));

        let ipv4 = read_ipv4_header(&GOLDEN_IPV4_UDP[ETHERNET_HEADER_LENGTH..]).expect("IPv4");
        let datagram = &GOLDEN_IPV4_UDP[ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH
            ..ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH + ipv4.payload_length()];
        assert!(udp_checksum_is_valid(
            ipv4.source,
            ipv4.destination,
            datagram
        ));
    }

    #[test]
    fn the_golden_arp_request_is_reproduced_byte_for_byte() {
        let Received::Arp(packet) =
            read(GOLDEN_ARP_REQUEST, Checksums::Software).expect("an ARP frame")
        else {
            panic!("the golden is an ARP frame");
        };
        let ethernet = read_ethernet_header(GOLDEN_ARP_REQUEST).expect("a header");

        assert_eq!(ARP_REQUEST, packet.operation);
        assert_eq!(BROADCAST, ethernet.destination);
        assert_eq!(Ipv4Addr::new(10, 77, 0, 1), packet.sender_protocol);
        assert_eq!(Ipv4Addr::new(10, 77, 0, 3), packet.target_protocol);
        assert_eq!([0; 6], packet.target_hardware, "a request asks for it");

        let mut rebuilt = vec![0u8; GOLDEN_ARP_REQUEST.len()];
        assert_eq!(
            GOLDEN_ARP_REQUEST.len(),
            write_arp(&mut rebuilt, &ethernet, &packet).expect("a frame")
        );
        assert_eq!(GOLDEN_ARP_REQUEST, rebuilt.as_slice());
    }

    /// The reply's encoder is the request's with the two ends swapped, and the
    /// golden cannot pin it: this host's kernel does not answer an ARP request
    /// for an address it holds on the receiving interface (measured — see the
    /// fixture README).
    #[test]
    fn an_arp_reply_swaps_the_two_ends() {
        let Received::Arp(request) =
            read(GOLDEN_ARP_REQUEST, Checksums::Software).expect("an ARP frame")
        else {
            panic!("an ARP frame");
        };

        let reply = ArpPacket {
            operation: ARP_REPLY,
            sender_hardware: [0x02, 0, 0, 0, 0, 0x02],
            sender_protocol: request.target_protocol,
            target_hardware: request.sender_hardware,
            target_protocol: request.sender_protocol,
        };
        let header = EthernetHeader {
            destination: request.sender_hardware,
            source: reply.sender_hardware,
            ether_type: ETHERNET_TYPE_ARP,
        };

        let mut frame = vec![0u8; ETHERNET_HEADER_LENGTH + ARP_PACKET_LENGTH];
        assert_eq!(
            ETHERNET_HEADER_LENGTH + ARP_PACKET_LENGTH,
            write_arp(&mut frame, &header, &reply).expect("a frame")
        );

        let Received::Arp(read_back) = read(&frame, Checksums::Software).expect("an ARP frame")
        else {
            panic!("an ARP frame");
        };
        assert_eq!(reply, read_back);
        assert_ne!(request.operation, read_back.operation);
    }

    /// RFC 1071's own worked example: `00 01 f2 03 f4 f5 f6 f7` sums to
    /// `0xddf2`, so the checksum is `0x220d`.
    #[test]
    fn the_internet_checksum_matches_rfc_1071s_example() {
        assert_eq!(
            0x220d,
            internet_checksum(&[0x00, 0x01, 0xf2, 0x03, 0xf4, 0xf5, 0xf6, 0xf7])
        );
    }

    /// The odd trailing byte is padded on the right. Getting this backwards
    /// still produces a number, which is exactly why it is worth a test.
    #[test]
    fn an_odd_trailing_byte_is_the_high_half_of_its_word() {
        let odd = [0x11, 0x22, 0x33];
        let padded = [0x11, 0x22, 0x33, 0x00];

        assert_eq!(internet_checksum(&padded), internet_checksum(&odd));
        assert_ne!(
            internet_checksum(&[0x11, 0x22, 0x00, 0x33]),
            internet_checksum(&odd),
            "the zero goes after the byte, not before it"
        );
    }

    /// A header that already carries its checksum sums to zero, which is the
    /// property every receiver relies on.
    #[test]
    fn a_checksum_over_its_own_field_verifies() {
        let mut header = [0u8; IPV4_HEADER_LENGTH];
        header[0] = 0x45;
        header[8] = DEFAULT_TTL;
        header[9] = IP_PROTOCOL_UDP;
        header[12..16].copy_from_slice(&[10, 77, 0, 1]);
        header[16..20].copy_from_slice(&[10, 77, 0, 2]);

        let checksum = internet_checksum(&header);
        header[10..12].copy_from_slice(&checksum.to_be_bytes());
        assert!(ipv4_checksum_is_valid(&header));
    }

    /// A UDP checksum that computes to zero travels as `0xffff`, because a
    /// zero in the field means "not computed". Found by search rather than
    /// written down, so the test is about the mapping and not about an input.
    #[test]
    fn a_udp_checksum_that_folds_to_zero_travels_as_all_ones() {
        let source = Ipv4Addr::new(10, 77, 0, 1);
        let destination = Ipv4Addr::new(10, 77, 0, 2);

        let zero_sum = (0..=u16::MAX).find_map(|payload| {
            let mut datagram = [0u8; UDP_HEADER_LENGTH + 2];
            datagram[0..2].copy_from_slice(&12001u16.to_be_bytes());
            datagram[2..4].copy_from_slice(&10999u16.to_be_bytes());
            datagram[4..6].copy_from_slice(&10u16.to_be_bytes());
            datagram[8..10].copy_from_slice(&payload.to_be_bytes());

            (udp_checksum(source, destination, &datagram) == u16::MAX).then_some(datagram)
        });

        let datagram = zero_sum.expect("some datagram folds to zero");
        assert!(udp_checksum_is_valid(source, destination, &datagram));
    }

    /// G14's other half: what arrives behind the 42 bytes is what the transport
    /// hands up, unchanged.
    #[test]
    fn the_payload_behind_the_headers_is_untouched() {
        let aeron = [0xab; 64];
        let mut frame = vec![0u8; IPV4_UDP_HEADER_LENGTH + aeron.len()];
        frame[IPV4_UDP_HEADER_LENGTH..].copy_from_slice(&aeron);

        let written =
            write_ipv4_udp_here(&mut frame, &golden_udp_write(aeron.len(), 1)).expect("a frame");

        let Received::Udp { datagram, .. } =
            read(&frame[..written], Checksums::Software).expect("a UDP frame")
        else {
            panic!("a UDP frame");
        };
        assert_eq!(&aeron[..], datagram.payload);
    }

    /// An echo request is answered with a reply that carries the same
    /// identifier, sequence and payload, addressed back to the sender.
    #[test]
    fn an_echo_request_is_answered_with_its_own_identifier() {
        let request = IcmpEcho {
            echo_type: ICMP_ECHO_REQUEST,
            identifier: 0x1234,
            sequence: 7,
            payload: b"deepmsg!",
        };
        let here = Ipv4Write {
            destination_hardware: [0x02, 0, 0, 0, 0, 0x02],
            source_hardware: [0x02, 0, 0, 0, 0, 0x01],
            source: Ipv4Addr::new(10, 77, 0, 1),
            destination: Ipv4Addr::new(10, 77, 0, 2),
            identification: 3,
        };

        let mut request_frame = vec![0u8; ETHERNET_HEADER_LENGTH + 20 + 8 + request.payload.len()];
        let written = write_icmp_echo(&mut request_frame, &here, &request, ICMP_ECHO_REQUEST)
            .expect("a frame");
        request_frame.truncate(written);

        let Received::Echo { ipv4, echo } =
            read(&request_frame, Checksums::Software).expect("an echo")
        else {
            panic!("an echo");
        };
        assert_eq!(ICMP_ECHO_REQUEST, echo.echo_type);
        assert_eq!(0x1234, echo.identifier);

        let answer = Ipv4Write {
            destination_hardware: [0x02, 0, 0, 0, 0, 0x01],
            source_hardware: [0x02, 0, 0, 0, 0, 0x02],
            source: Ipv4Addr::new(10, 77, 0, 2),
            destination: ipv4.source,
            identification: 4,
        };
        let mut reply_frame = vec![0u8; request_frame.len()];
        let written = write_icmp_echo_reply(&mut reply_frame, &answer, &echo).expect("a frame");
        reply_frame.truncate(written);

        let Received::Echo { ipv4, echo } =
            read(&reply_frame, Checksums::Software).expect("an echo")
        else {
            panic!("an echo");
        };
        assert_eq!(ICMP_ECHO_REPLY, echo.echo_type, "a reply, not a request");
        assert_eq!(0x1234, echo.identifier);
        assert_eq!(7, echo.sequence);
        assert_eq!(b"deepmsg!", echo.payload);
        assert_eq!(
            Ipv4Addr::new(10, 77, 0, 2),
            ipv4.source,
            "answered from here"
        );
        assert_eq!(
            Ipv4Addr::new(10, 77, 0, 1),
            ipv4.destination,
            "back to the pinger"
        );
    }

    /// A frame this stack does not serve is named, not guessed at — the
    /// counters depend on the name.
    ///
    /// The shape cases ask a port that vouches for its checksums, because
    /// changing a header byte changes its checksum too and the checksum test
    /// would answer first — which is what the last case is about, and why it
    /// is the only one that asks the arithmetic.
    #[test]
    fn what_the_stack_does_not_serve_is_named() {
        let golden = GOLDEN_IPV4_UDP;

        let mut vlan = golden.to_vec();
        vlan[12..14].copy_from_slice(&ETHERNET_TYPE_VLAN.to_be_bytes());
        assert_eq!(
            Err(Discard::UnsupportedEthernetType),
            read(&vlan, Checksums::Verified)
        );

        let mut tcp = golden.to_vec();
        tcp[ETHERNET_HEADER_LENGTH + 9] = 6;
        assert_eq!(
            Err(Discard::UnsupportedProtocol),
            read(&tcp, Checksums::Verified)
        );

        let mut fragmented = golden.to_vec();
        fragmented[ETHERNET_HEADER_LENGTH + 6..ETHERNET_HEADER_LENGTH + 8]
            .copy_from_slice(&0x2000u16.to_be_bytes());
        assert_eq!(
            Err(Discard::Fragmented),
            read(&fragmented, Checksums::Verified)
        );

        let mut options = golden.to_vec();
        options[ETHERNET_HEADER_LENGTH] = 0x46;
        assert_eq!(Err(Discard::Options), read(&options, Checksums::Verified));

        let mut corrupt = golden.to_vec();
        corrupt[ETHERNET_HEADER_LENGTH + 10] ^= 0xff;
        assert_eq!(Err(Discard::Checksum), read(&corrupt, Checksums::Software));
        assert_eq!(
            Err(Discard::Checksum),
            read(&tcp, Checksums::Software),
            "a header byte changed under a checksum test is caught by the \
             checksum first: the protocol field is only believed once the \
             header it lives in is"
        );

        assert_eq!(Err(Discard::Short), read(&golden[..8], Checksums::Software));
    }

    /// A port that vouches for a frame's checksums does not pay for the
    /// arithmetic again — and the frame is read even where its checksum bytes
    /// are wrong, because the port said so.
    #[test]
    fn a_verified_frame_is_not_checksummed_again() {
        let mut corrupt = GOLDEN_IPV4_UDP.to_vec();
        corrupt[ETHERNET_HEADER_LENGTH + 10] ^= 0xff;

        assert!(read(&corrupt, Checksums::Verified).is_ok());
    }

    /// A frame addressed to another port is not this port's — and the rule is
    /// to drop it silently rather than count it (§4.5).
    #[test]
    fn a_frame_for_another_mac_is_not_mine() {
        let ethernet = read_ethernet_header(GOLDEN_IPV4_UDP).expect("a header");

        assert!(ethernet.is_for([0x02, 0, 0, 0, 0, 0x02]), "addressed to me");
        assert!(
            !ethernet.is_for([0x02, 0, 0, 0, 0, 0x09]),
            "addressed to another port"
        );

        let broadcast = EthernetHeader {
            destination: BROADCAST,
            ..ethernet
        };
        assert!(
            broadcast.is_for([0x02, 0, 0, 0, 0, 0x09]),
            "a broadcast is for every port"
        );
    }
}
