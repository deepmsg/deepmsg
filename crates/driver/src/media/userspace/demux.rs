//! One frame → one transport's ring, or one counter.
//!
//! Everything a received frame can be is decided here, and every decision that
//! is not "this one is for that transport" lands on a counter the reference
//! names (`aeron-client/src/main/c/aeron_counters.h:155-173`). That is the
//! point of the module: a frame the stack cannot place is one the reference's
//! DPDK transport cannot place either, and a driver whose counters say nothing
//! arrived while its transports say otherwise is a driver nobody can debug.
//!
//! ```text
//!   frame → is it for this port's MAC?                    no → drop, silently
//!         → parse it                                     no → 89/90/93/94
//!         → ARP?                                          yes → learn, answer
//!         → ICMP echo?                                    yes → answer
//!         → UDP: whose port is it?                        no  → 86 or 95
//!                                                         yes → that ring
//! ```
//!
//! # Why the two discards are split in two
//!
//! A datagram for a port nobody bound is counted **86** (`RX_SENDER_DISCARD`)
//! or **95** (`RX_RECEIVER_DISCARD`) depending on what it is: the reference's
//! DPDK transport keeps one queue per agent thread, so a control frame that
//! arrives with no transport to take it was going to the sender's thread and a
//! data frame was going to the receiver's, and its two counters count exactly
//! that. This stack has one receive queue for the machine (the design's §4.7),
//! so the same question is answered from the frame itself — the Aeron header's
//! type says which side it was for — and the two counters keep their meaning.
//!
//! # What it is not
//!
//! Not a filter on anything but the MAC and the port table: a frame for the
//! right port is filed whatever it carries, and what the Aeron layer makes of
//! it is the Aeron layer's business (G14).

use std::net::SocketAddr;

use crate::protocol::frame_type;

use super::arp;
use super::frame::{self, Discard, Received};
use super::packet_io::{Disposition, RxFrame};
use super::stack::{Deferred, Stack};

/// Where the Aeron frame type lives in a datagram's payload: the frame header
/// is length, version, flags, type, and the type is the two bytes at six
/// (`aeron_udp_protocol.h:27-34`).
const FRAME_TYPE_OFFSET: usize = 6;

/// Decide what becomes of one frame.
///
/// A frame for a transport comes back as `Keep(binding)`; a frame that is
/// nobody's is counted and `Consumed`; an answer that has to be sent is
/// described into `deferred`, because nothing is sent from inside a receive
/// callback (the design's §3.8 lock order).
pub(crate) fn route(
    stack: &Stack,
    frame: RxFrame<'_>,
    deferred: &mut Vec<Deferred>,
) -> Disposition {
    let ethernet = match frame::read_ethernet_header(frame.bytes) {
        Ok(ethernet) if ethernet.is_for(stack.params().mac) => ethernet,
        // Not this port's, and dropped **silently**: a port that is not in
        // promiscuous mode is never handed one, so there is nothing to count
        // (design §4.5).
        _ => return Disposition::Consumed,
    };

    let received = match frame::read(frame.bytes, frame.checksums) {
        Ok(received) => received,
        Err(discard) => {
            count_a_discard(stack, discard);
            return Disposition::Consumed;
        }
    };

    match received {
        Received::Arp(packet) => {
            // Gleaning, and not only for ARP: any frame from an address on
            // this link carries that address's MAC, and learning it is what
            // lets an answer go straight back (design §4.5).
            stack.glean(packet.sender_protocol, packet.sender_hardware);

            if arp::asks_about(&packet, stack.params().address) {
                let (ethernet, reply) = arp::reply_to(stack.params(), &packet);
                deferred.push(Deferred::ArpReply {
                    ethernet,
                    packet: reply,
                });
            }

            Disposition::Consumed
        }
        Received::Echo { ipv4, echo } => {
            answer_echo(stack, ipv4, echo, ethernet, deferred);
            Disposition::Consumed
        }
        Received::Udp { ipv4, datagram } => {
            stack.glean(ipv4.source, ethernet.source);

            let source = SocketAddr::from((ipv4.source, datagram.source_port));
            match stack.lookup(ipv4.destination, datagram.destination_port, source) {
                Some(binding) => Disposition::Keep(binding),
                None => {
                    count_unclaimed(stack, datagram.payload);
                    Disposition::Consumed
                }
            }
        }
    }
}

/// Answer an echo request, which is the only way to tell a live link from a
/// dead one once the port is bound to a userspace stack: there is no kernel to
/// `ping` it (design §4.5).
fn answer_echo(
    stack: &Stack,
    ipv4: frame::Ipv4Header,
    echo: frame::IcmpEcho<'_>,
    ethernet: frame::EthernetHeader,
    deferred: &mut Vec<Deferred>,
) {
    // Only requests are answered. A reply that arrived is somebody else's
    // answer to somebody else's ping — ours would be a reply to a reply, which
    // is how two stacks ping-pong forever.
    if echo.echo_type != frame::ICMP_ECHO_REQUEST {
        return;
    }

    let params = stack.params();
    let mut reply = vec![
        0u8;
        frame::ETHERNET_HEADER_LENGTH
            + frame::IPV4_HEADER_LENGTH
            + frame::ICMP_ECHO_HEADER_LENGTH
            + echo.payload.len()
    ];
    let Some(written) = frame::write_icmp_echo_reply(
        &mut reply,
        &frame::Ipv4Write {
            destination_hardware: ethernet.source,
            source_hardware: params.mac,
            source: params.address,
            destination: ipv4.source,
            identification: stack.next_identification(),
        },
        &echo,
    ) else {
        return;
    };
    reply.truncate(written);

    deferred.push(Deferred::IcmpReply(reply));
}

/// Count a frame the parser would not take.
///
/// **The one place that says which `Discard` is which counter**, so that a
/// frame rejected somewhere other than here lands on the same counter it would
/// have landed on here: [`super::transport`]'s re-read of a frame it will not
/// hand up asks this, and that is the whole reason it is not private — one
/// frame, one decision, one counter, wherever the decision is made.
pub(crate) fn count_a_discard(stack: &Stack, discard: Discard) {
    let counters = stack.counters();

    match discard {
        Discard::Checksum => &counters.checksum_failure,
        Discard::Fragmented | Discard::Options => &counters.fragmented,
        Discard::UnsupportedEthernetType | Discard::Malformed => {
            &counters.unsupported_ethernet_type
        }
        Discard::UnsupportedProtocol => &counters.unsupported_protocol,
        // A runt is something the NIC would have dropped before anybody could
        // count it, and so does this.
        Discard::Short => return,
    }
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Count a datagram for a port nobody bound, on the counter its frame type
/// belongs to.
pub(crate) fn count_unclaimed(stack: &Stack, payload: &[u8]) {
    let counter = if is_data_plane(payload) {
        &stack.counters().rx_receiver_discard
    } else {
        &stack.counters().rx_sender_discard
    };

    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Count a whole frame that was routed to a binding that is no longer there.
///
/// **The second half of the rule this module's note states**: every decision
/// about a received frame lands on a counter, and `Stack::deliver` has two ways
/// to drop one — a slot that is gone, and a ring whose binding has been rebound
/// under it. The fact is the unclaimed fact ("no transport has it now"), so it
/// is counted the same way, from the frame's own bytes. A frame whose bytes do
/// not read back as UDP takes [`is_data_plane`]'s own way out — the side is not
/// there to ask, so it is the data plane's. (The third way, `Pushed::Stale`,
/// cannot ask this: the ring has consumed the frame by then. It counts on the
/// receiver's counter for the same reason, stated where it happens.)
pub(crate) fn count_a_frame_nobody_has(stack: &Stack, frame: &[u8]) {
    let payload = match frame::read(frame, frame::Checksums::Software) {
        Ok(Received::Udp { datagram, .. }) => datagram.payload,
        Ok(Received::Arp(_) | Received::Echo { .. }) | Err(_) => &[],
    };

    count_unclaimed(stack, payload);
}

/// Whether an Aeron frame carries data — which is what says its datagram was
/// going to the receiver's side rather than the sender's.
///
/// Three types are data: `DATA` and `PAD` are the data plane's own, and
/// `SETUP` is how a session starts, sent from a publisher to a subscriber
/// (`aeron-receive_channel_endpoint.c`'s dispatch is the reference's own
/// reading of the same three). Everything else — `NAK`, `SM`, `ERR`, `RTTM`,
/// the response to a setup — travels towards a publisher, which is the side
/// the reference's 86 counts.
///
/// A payload too short to hold a frame header is counted as data, because this
/// is only ever asked about a datagram nobody bound, and being wrong between
/// two discard counters is not worth a third.
fn is_data_plane(payload: &[u8]) -> bool {
    let Some(bytes) = payload.get(FRAME_TYPE_OFFSET..FRAME_TYPE_OFFSET + 2) else {
        return true;
    };
    let Ok(bytes) = <[u8; 2]>::try_from(bytes) else {
        return true;
    };
    let frame_type = i16::from_le_bytes(bytes);

    matches!(
        frame_type,
        frame_type::DATA | frame_type::PAD | frame_type::SETUP
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::userspace::fake_port::{FakePort, Side, Wire};
    use crate::media::userspace::frame::TxOffloads;
    use crate::media::userspace::frame::{
        ARP_REPLY, ARP_REQUEST, ArpPacket, BROADCAST, Checksums, ETHERNET_TYPE_ARP,
        ETHERNET_TYPE_VLAN, EthernetHeader, ICMP_ECHO_REQUEST, IPV4_UDP_HEADER_LENGTH, IcmpEcho,
        Ipv4Write, UdpWrite,
    };
    use crate::media::userspace::packet_io::{RxPort, TxPort};
    use crate::media::userspace::stack::{Clock, StackParams};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use std::net::Ipv4Addr;

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);
    const OUR_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x01];
    const PEER_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x02];

    fn stack(wire: &Wire) -> Arc<Stack> {
        Stack::over_a_fake_wire(
            wire,
            Side::A,
            StackParams::new(OWN, 24, OUR_MAC),
            Clock::Monotonic,
        )
    }

    fn far_end(wire: &Wire) -> FakePort {
        wire.port(Side::B, PEER_MAC, 1500)
    }

    fn send(port: &mut FakePort, bytes: &[u8]) {
        let written = bytes.len();
        assert_eq!(
            1,
            port.tx_burst(1, &mut |buffer| {
                buffer[..written].copy_from_slice(bytes);

                (written, TxOffloads::NONE)
            })
        );
    }

    /// A UDP frame carrying an Aeron frame of `frame_type`, to `to`.
    fn udp_frame(frame_type: i16, to: u16, payload: &[u8]) -> Vec<u8> {
        let mut aeron = vec![0u8; 8 + payload.len()];
        aeron[6..8].copy_from_slice(&frame_type.to_le_bytes());
        aeron[8..].copy_from_slice(payload);

        let mut bytes = vec![0u8; IPV4_UDP_HEADER_LENGTH + aeron.len()];
        bytes[IPV4_UDP_HEADER_LENGTH..].copy_from_slice(&aeron);
        let written = frame::write_ipv4_udp_here(
            &mut bytes,
            &UdpWrite {
                ipv4: Ipv4Write {
                    destination_hardware: OUR_MAC,
                    source_hardware: PEER_MAC,
                    source: PEER,
                    destination: OWN,
                    identification: 1,
                },
                source_port: 10000,
                destination_port: to,
                payload_length: aeron.len(),
            },
        )
        .expect("a frame");
        bytes.truncate(written);

        bytes
    }

    /// Recompute an IPv4 header's checksum, for a test that changed a byte in
    /// it: the checksum is checked before the field it covers is believed, so
    /// a test about the field has to leave the header consistent.
    fn fix_ipv4_checksum(bytes: &mut [u8]) {
        let header = &mut bytes[frame::ETHERNET_HEADER_LENGTH
            ..frame::ETHERNET_HEADER_LENGTH + frame::IPV4_HEADER_LENGTH];
        header[10..12].copy_from_slice(&[0, 0]);
        let checksum = frame::internet_checksum(header);
        header[10..12].copy_from_slice(&checksum.to_be_bytes());
    }

    fn bind(stack: &Stack, port: u16) {
        stack
            .bind(Some(OWN), port, None, SocketAddr::from((OWN, port)))
            .expect("a bind");
    }

    /// Every discard has a counter, and this is the whole mapping.
    #[test]
    fn every_discard_lands_on_its_counter() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let far = &mut far_end(&wire);

        let mut vlan = udp_frame(frame_type::DATA, 10500, b"x");
        vlan[12..14].copy_from_slice(&ETHERNET_TYPE_VLAN.to_be_bytes());
        send(far, &vlan);

        let mut tcp = udp_frame(frame_type::DATA, 10500, b"x");
        tcp[frame::ETHERNET_HEADER_LENGTH + 9] = 6;
        fix_ipv4_checksum(&mut tcp);
        send(far, &tcp);

        let mut fragmented = udp_frame(frame_type::DATA, 10500, b"x");
        fragmented[frame::ETHERNET_HEADER_LENGTH + 6..frame::ETHERNET_HEADER_LENGTH + 8]
            .copy_from_slice(&0x2000u16.to_be_bytes());
        send(far, &fragmented);

        let mut options = udp_frame(frame_type::DATA, 10500, b"x");
        options[frame::ETHERNET_HEADER_LENGTH] = 0x46;
        send(far, &options);

        let mut corrupt = udp_frame(frame_type::DATA, 10500, b"x");
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        send(far, &corrupt);

        send(far, &udp_frame(frame_type::DATA, 10500, b"x"));

        assert_eq!(6, stack.pump(), "every frame was read, and one was claimed");

        let counters = stack.counters();
        assert_eq!(
            1,
            counters.unsupported_ethernet_type.load(Ordering::Relaxed)
        );
        assert_eq!(1, counters.unsupported_protocol.load(Ordering::Relaxed));
        assert_eq!(
            2,
            counters.fragmented.load(Ordering::Relaxed),
            "fragment and options"
        );
        assert_eq!(1, counters.checksum_failure.load(Ordering::Relaxed));
        assert_eq!(
            1,
            counters.rx_receiver_discard.load(Ordering::Relaxed),
            "and the good one was for a port nobody bound"
        );
    }

    /// A datagram for a port nobody bound is counted by what it was, which is
    /// the one thing the reference's two discard counters say.
    #[test]
    fn an_unclaimed_datagram_is_counted_by_its_kind() {
        let wire = Wire::new();
        let stack = stack(&wire);
        let far = &mut far_end(&wire);

        send(far, &udp_frame(frame_type::DATA, 10501, b"data"));
        send(far, &udp_frame(frame_type::PAD, 10501, b""));
        send(far, &udp_frame(frame_type::SETUP, 10501, b"setup"));
        send(far, &udp_frame(frame_type::SM, 10501, b"status"));
        send(far, &udp_frame(frame_type::NAK, 10501, b"nak"));
        send(far, &udp_frame(frame_type::ERR, 10501, b"error"));
        send(far, &udp_frame(frame_type::RTTM, 10501, b"rtt"));
        send(far, &udp_frame(0x7fff, 10501, b"something else"));

        stack.pump();

        assert_eq!(
            3,
            stack.counters().rx_receiver_discard.load(Ordering::Relaxed)
        );
        assert_eq!(
            5,
            stack.counters().rx_sender_discard.load(Ordering::Relaxed),
            "the four control frames, and the one nobody can name"
        );
    }

    /// A datagram for a port somebody did bind is not counted at all: it is
    /// filed, and the counters stay where they were.
    #[test]
    fn a_claimed_datagram_is_not_counted() {
        let wire = Wire::new();
        let stack = stack(&wire);
        bind(&stack, 10502);

        send(
            &mut far_end(&wire),
            &udp_frame(frame_type::DATA, 10502, b"mine"),
        );
        assert_eq!(1, stack.pump());

        let counters = stack.counters();
        assert_eq!(0, counters.rx_receiver_discard.load(Ordering::Relaxed));
        assert_eq!(0, counters.rx_sender_discard.load(Ordering::Relaxed));
        assert_eq!(0, counters.queue_drop.load(Ordering::Relaxed));
    }

    /// A frame for another port's MAC is not read at all — and not counted,
    /// because a port that is not in promiscuous mode never sees it.
    #[test]
    fn a_frame_for_another_mac_is_not_counted() {
        let wire = Wire::new();
        let stack = stack(&wire);

        let mut bytes = udp_frame(frame_type::DATA, 10503, b"elsewhere");
        bytes[0..6].copy_from_slice(&[0x02, 0, 0, 0, 0, 0x09]);
        send(&mut far_end(&wire), &bytes);

        stack.pump();

        let counters = stack.counters();
        assert_eq!(0, counters.rx_receiver_discard.load(Ordering::Relaxed));
        assert_eq!(0, counters.rx_sender_discard.load(Ordering::Relaxed));
        assert_eq!(
            0,
            counters.unsupported_ethernet_type.load(Ordering::Relaxed)
        );
    }

    /// An ARP request for this port is answered after the receive lock is let
    /// go, which is what keeps the two ports from deadlocking against each
    /// other.
    #[test]
    fn an_arp_request_is_answered_after_the_burst() {
        let wire = Wire::new();
        let stack = stack(&wire);

        let mut bytes = vec![0u8; 64];
        let written = frame::write_arp(
            &mut bytes,
            &EthernetHeader {
                destination: BROADCAST,
                source: PEER_MAC,
                ether_type: ETHERNET_TYPE_ARP,
            },
            &ArpPacket {
                operation: ARP_REQUEST,
                sender_hardware: PEER_MAC,
                sender_protocol: PEER,
                target_hardware: [0; 6],
                target_protocol: OWN,
            },
        )
        .expect("a frame");
        send(&mut far_end(&wire), &bytes[..written]);

        assert_eq!(1, stack.pump());
        assert_eq!(1, wire.queued(Side::A), "and the answer left");

        let mut answered = false;
        far_end(&wire).rx_burst(
            1,
            &mut |frame| {
                let Ok(Received::Arp(reply)) = frame::read(frame.bytes, Checksums::Software) else {
                    panic!("the frame that left is an ARP reply");
                };
                assert_eq!(ARP_REPLY, reply.operation);
                assert_eq!(PEER, reply.target_protocol);
                assert_eq!(OUR_MAC, reply.sender_hardware);
                answered = true;

                Disposition::Consumed
            },
            &mut Vec::new(),
        );

        assert!(answered, "and the answer left");
        assert_eq!(
            crate::media::userspace::neighbour::NextHop::Known(PEER_MAC),
            stack.neighbour(PEER),
            "and the asker was learned on the way past"
        );
    }

    /// An echo request is answered with an echo reply carrying the same
    /// identifier, sequence and payload.
    #[test]
    fn an_echo_request_is_answered() {
        let wire = Wire::new();
        let stack = stack(&wire);

        let payload = b"deepmsg ping";
        let mut bytes = vec![0u8; frame::ETHERNET_HEADER_LENGTH + 20 + 8 + payload.len()];
        let written = frame::write_icmp_echo(
            &mut bytes,
            &Ipv4Write {
                destination_hardware: OUR_MAC,
                source_hardware: PEER_MAC,
                source: PEER,
                destination: OWN,
                identification: 9,
            },
            &IcmpEcho {
                echo_type: ICMP_ECHO_REQUEST,
                identifier: 0x4321,
                sequence: 12,
                payload,
            },
            ICMP_ECHO_REQUEST,
        )
        .expect("a frame");
        bytes.truncate(written);

        send(&mut far_end(&wire), &bytes);
        assert_eq!(1, stack.pump());
        assert_eq!(1, wire.queued(Side::A), "the answer left");

        let mut answered = false;
        far_end(&wire).rx_burst(
            1,
            &mut |frame| {
                let Ok(Received::Echo { ipv4, echo }) =
                    frame::read(frame.bytes, Checksums::Software)
                else {
                    panic!("the frame that left is an echo reply");
                };
                assert_eq!(frame::ICMP_ECHO_REPLY, echo.echo_type);
                assert_eq!(0x4321, echo.identifier);
                assert_eq!(12, echo.sequence);
                assert_eq!(payload, echo.payload);
                assert_eq!(PEER, ipv4.destination, "back to the pinger");
                assert_eq!(OWN, ipv4.source);
                answered = true;

                Disposition::Consumed
            },
            &mut Vec::new(),
        );

        assert!(answered, "and the answer left");
    }

    /// A frame with no ethertype the stack serves is counted as an
    /// unsupported type even when it is an IPv4 frame underneath, which is
    /// what makes the counter a statement about the wire rather than about the
    /// parser.
    #[test]
    fn a_frame_that_is_not_ipv4_is_counted_once() {
        let wire = Wire::new();
        let stack = stack(&wire);

        let mut bytes = udp_frame(frame_type::DATA, 10504, b"x");
        bytes[12..14].copy_from_slice(&0x86ddu16.to_be_bytes());
        send(&mut far_end(&wire), &bytes);

        stack.pump();

        assert_eq!(
            1,
            stack
                .counters()
                .unsupported_ethernet_type
                .load(Ordering::Relaxed)
        );
        assert_eq!(
            0,
            stack.counters().rx_receiver_discard.load(Ordering::Relaxed)
        );
    }
}
