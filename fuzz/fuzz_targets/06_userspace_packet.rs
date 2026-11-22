#![no_main]

//! Target 06: the userspace Ethernet / IPv4 / UDP / ARP parsers.
//!
//! On a DPDK port these bytes come off the wire with no kernel in front of
//! them. Every function is pure over a slice and answers `Result`/`Option`;
//! where a datagram parses, its declared length must fit inside the frame that
//! carried it.

use std::net::Ipv4Addr;

use deepmsg_driver::media::userspace::arp;
use deepmsg_driver::media::userspace::frame::{
    Checksums, Received, icmp_checksum_is_valid, internet_checksum, ipv4_checksum_is_valid, read,
    read_arp, read_ethernet_header, read_icmp_echo, read_ipv4_header, read_udp, udp_checksum,
    udp_checksum_is_valid,
};
use deepmsg_driver::media::userspace::stack::StackParams;
use libfuzzer_sys::fuzz_target;

fn walk(data: &[u8]) {
    let _ = read_ethernet_header(data);
    let _ = read_ipv4_header(data);
    let _ = read_udp(data);
    let _ = read_icmp_echo(data);
    let _ = internet_checksum(data);
    let _ = ipv4_checksum_is_valid(data);
    let _ = icmp_checksum_is_valid(data);

    // The checksum helpers take addresses the caller already parsed; the
    // fuzzer's bytes decide the bytes, not the addresses.
    let source = Ipv4Addr::new(10, 0, 0, 1);
    let destination = Ipv4Addr::new(10, 0, 0, 2);
    let _ = udp_checksum(source, destination, data);
    let _ = udp_checksum_is_valid(source, destination, data);

    for checksums in [Checksums::Software, Checksums::Verified] {
        match read(data, checksums) {
            Ok(Received::Udp { datagram, .. }) => {
                assert_eq!(
                    datagram.payload.len() + 8,
                    datagram.length,
                    "a datagram's payload and its length field disagree"
                );
                assert!(datagram.length <= data.len());
            }
            Ok(Received::Echo { .. } | Received::Arp(_)) | Err(_) => {}
        }
    }

    if let Some(packet) = read_arp(data) {
        let params = StackParams::new(Ipv4Addr::new(10, 0, 0, 1), 24, [2, 0, 0, 0, 0, 1]);
        let _ = arp::asks_about(&packet, params.address);
        let (header, reply) = arp::reply_to(&params, &packet);
        // The reply is this stack's own construction; its addresses must be
        // the ones the request asked about.
        assert_eq!(reply.target_protocol, packet.sender_protocol);
        assert_eq!(header.destination, packet.sender_hardware);
    }
}

fuzz_target!(|data: &[u8]| {
    walk(data);
});
