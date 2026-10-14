//! ARP: the frames, and the two decisions that go with them.
//!
//! Asking, answering and learning. A [super::neighbour::NeighbourTable] holds
//! what is known and when it was learned; this module is the wire side of it —
//! what a request looks like, what a reply to one looks like, and which
//! arriving packets are worth learning from.
//!
//! **Answering is not optional.** With the port bound to a userspace stack
//! there is no kernel to answer on its behalf, so a peer that is running the
//! reference (a kernel UDP socket, or `aeronmd`) can never resolve this stack's
//! address unless every request addressed to it is answered here. The
//! falsification the design asks for is exactly that: stop answering, and the
//! peer's sends stop arriving.
//!
//! **Learning is gleaning, and it is bounded by the link.** Any IPv4 or ARP
//! frame that arrives from an address on this link carries a source MAC worth
//! remembering — that is what makes two drivers that have just met each other
//! able to answer without asking. A source *off* this link is not learned: its
//! MAC is the router's, and remembering it as the source address's would send
//! that address's traffic to the router for the wrong reason.

use std::net::Ipv4Addr;

use super::frame::{
    self, ARP_REPLY, ARP_REQUEST, ArpPacket, BROADCAST, ETHERNET_TYPE_ARP, EthernetHeader,
};
use super::stack::StackParams;

/// Write the request that asks who holds `address`, addressed to the
/// broadcast MAC.
///
/// Answers the frame's length, or `None` when the buffer cannot hold it.
pub fn write_request(buffer: &mut [u8], params: &StackParams, address: Ipv4Addr) -> Option<usize> {
    frame::write_arp(
        buffer,
        &EthernetHeader {
            destination: BROADCAST,
            source: params.mac,
            ether_type: ETHERNET_TYPE_ARP,
        },
        &ArpPacket {
            operation: ARP_REQUEST,
            sender_hardware: params.mac,
            sender_protocol: params.address,
            // What is being asked for, and the field the answer fills in.
            target_hardware: [0; 6],
            target_protocol: address,
        },
    )
}

/// The reply to a request this port is the target of.
///
/// The two ends swap: the MAC and address the request came from become the
/// destination, and this port's become the source.
pub fn reply_to(params: &StackParams, request: &ArpPacket) -> (EthernetHeader, ArpPacket) {
    (
        EthernetHeader {
            destination: request.sender_hardware,
            source: params.mac,
            ether_type: ETHERNET_TYPE_ARP,
        },
        ArpPacket {
            operation: ARP_REPLY,
            sender_hardware: params.mac,
            sender_protocol: params.address,
            target_hardware: request.sender_hardware,
            target_protocol: request.sender_protocol,
        },
    )
}

/// Whether a packet is a request for `address`, which is what this stack has
/// to answer.
///
/// A reply is not a question, and a request for somebody else's address is not
/// this port's to answer — the design's "目标 IP 是本口 IP 的请求必答", and
/// nothing more than that.
pub fn asks_about(packet: &ArpPacket, address: Ipv4Addr) -> bool {
    packet.operation == ARP_REQUEST && packet.target_protocol == address
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::userspace::frame::Checksums;

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);
    const MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x01];
    const PEER_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x02];

    fn params() -> StackParams {
        StackParams::new(OWN, 24, MAC)
    }

    /// A request is a broadcast, says who is asking, and leaves the answer
    /// blank — which is what the peer fills in.
    #[test]
    fn a_request_broadcasts_and_asks() {
        let mut frame = vec![0u8; 64];
        let written = write_request(&mut frame, &params(), PEER).expect("a frame");
        frame.truncate(written);

        let ethernet = frame::read_ethernet_header(&frame).expect("a header");
        assert_eq!(BROADCAST, ethernet.destination);
        assert_eq!(MAC, ethernet.source);

        let frame::Received::Arp(packet) =
            frame::read(&frame, Checksums::Software).expect("an ARP frame")
        else {
            panic!("an ARP frame");
        };
        assert_eq!(ARP_REQUEST, packet.operation);
        assert_eq!(MAC, packet.sender_hardware);
        assert_eq!(OWN, packet.sender_protocol);
        assert_eq!([0; 6], packet.target_hardware);
        assert_eq!(PEER, packet.target_protocol);
    }

    #[test]
    fn a_reply_swaps_the_two_ends() {
        let request = ArpPacket {
            operation: ARP_REQUEST,
            sender_hardware: PEER_MAC,
            sender_protocol: PEER,
            target_hardware: [0; 6],
            target_protocol: OWN,
        };

        assert!(asks_about(&request, OWN));
        assert!(!asks_about(&request, PEER), "not somebody else's question");

        let (ethernet, reply) = reply_to(&params(), &request);
        assert_eq!(PEER_MAC, ethernet.destination, "straight back to the asker");
        assert_eq!(MAC, ethernet.source);
        assert_eq!(ARP_REPLY, reply.operation);
        assert_eq!(MAC, reply.sender_hardware);
        assert_eq!(OWN, reply.sender_protocol);
        assert_eq!(PEER_MAC, reply.target_hardware);
        assert_eq!(PEER, reply.target_protocol);

        let mut frame = vec![0u8; 64];
        let written = frame::write_arp(&mut frame, &ethernet, &reply).expect("a frame");
        let frame::Received::Arp(read_back) =
            frame::read(&frame[..written], Checksums::Software).expect("an ARP frame")
        else {
            panic!("an ARP frame");
        };
        assert_eq!(reply, read_back);
        assert!(
            !asks_about(&read_back, OWN),
            "a reply is not a question, and answering one would be a loop"
        );
    }

    /// A request for an address this port does not hold is read and not
    /// answered: the peer that owns it answers it.
    #[test]
    fn a_request_for_another_address_is_not_ours_to_answer() {
        let elsewhere = Ipv4Addr::new(10, 10, 0, 9);
        let request = ArpPacket {
            operation: ARP_REQUEST,
            sender_hardware: PEER_MAC,
            sender_protocol: PEER,
            target_hardware: [0; 6],
            target_protocol: elsewhere,
        };

        assert!(!asks_about(&request, OWN));
    }
}
