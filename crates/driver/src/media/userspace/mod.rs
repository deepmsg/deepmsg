//! The userspace UDP stack (M12): a [`crate::media::Transport`] over Ethernet
//! frames the driver builds and parses itself, with no socket underneath.
//!
//! This is the layer the `dpdk` transport bindings will run on — the frames
//! are built and read here, and the port underneath is whatever implements the
//! `packet_io::RxPort`/`packet_io::TxPort` pair: a pair of in-memory wires in
//! the tests, a DPDK queue pair in the build that has it. Nothing in this
//! module knows which, and nothing in it is `unsafe`.
//!
//! # Why it exists
//!
//! A kernel-bypass transport has no `recvfrom` to lean on: it gets whole
//! Ethernet frames and has to answer every question the kernel used to answer
//! for us — which of these frames is mine, what its checksums should be, who
//! owns the destination port, how a next hop's MAC address is found. The
//! behaviours it has to reproduce are listed as G1–G14 in the design and each
//! one is a test here; the ones a wrong answer would break silently are the
//! interesting ones, and they are the reason this is a module with tests
//! rather than a shim inside the DPDK crate.
//!
//! # Shape
//!
//! ```text
//!   frame.rs      Ethernet II / IPv4 / UDP / ICMP echo / ARP, as bytes
//!   packet_io.rs  what a port is: rx_burst, tx_burst, and what a kept frame is
//!   fake_port.rs  a port over a wire of VecDeques, and the wire's own knobs
//!   ports.rs      (ip, port) → the transport that bound it
//!   neighbour.rs  next hop → MAC
//!   arp.rs        requests, replies, gleaning, ageing
//!   demux.rs      one frame → one transport's ready list
//!   stack.rs      the state all of the above share
//!   transport.rs  the Transport the driver sees
//! ```
//!
//! # What it is not
//!
//! No VLAN, no IPv6, no IP options, no reassembly, no multicast: a frame that
//! needs any of those is counted and dropped, not handled (the design's §4.11
//! list).

pub mod fake_port;
pub mod frame;
pub mod packet_io;
