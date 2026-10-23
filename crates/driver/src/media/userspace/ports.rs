//! Which transports hold which `(address, port)`, and what a frame's
//! destination has to match to reach one.
//!
//! The kernel answers these questions with a socket: a bind fails with
//! `EADDRINUSE`, a connected socket ignores datagrams from anyone else, a
//! wildcard socket hears every address the machine has. Every one of those
//! answers is something the driver above already depends on — the port
//! manager's allocation, the error text in a failed bind, the sender's belief
//! that a connected transport hears only its peer — so they are reproduced
//! here rather than reinvented (G1–G5, G12).
//!
//! # Two tables, and only one of them has a shape that can be read cheaply
//!
//! The first is indexed by destination port and read on every received frame:
//! one load from a 128 KiB array, no hash, no allocation, no search. It holds
//! `slot + 1`, because zero is the value a zeroed page has and "nobody bound
//! this port" is the common case.
//!
//! The second is the slots themselves — a fixed array sized when the stack is
//! built, holding each binding in **atomics** rather than behind a lock: the
//! receive path reads one on every frame it delivers, and a kernel socket
//! costs no lock to ask the same question. Binding and releasing happen on the
//! conductor, one lock for the two of them.
//!
//! # The generation, and what it is for
//!
//! A transport that closes and a transport that binds the same port are two
//! different transports, and the kernel never delivers the first one's queued
//! datagrams to the second. Slots are reused here, so each bind takes a
//! generation — monotonic per slot — and the ring that holds a transport's
//! frames refuses anything pushed under a generation that is not its own
//! (§3.2 of the plan). A frame that races a release is refused rather than
//! misdelivered; a frame that races a *bind* cannot be delivered at all,
//! because the entry it would have to match is written after the slot is
//! filled.

use std::net::{Ipv4Addr, SocketAddr};
use std::ops::RangeInclusive;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

use crate::sys::socket::BindFailure;

use super::packet_io::SlotId;

/// How many `(address, port)` pairs a stack can have bound at once.
///
/// The port table can name [`u16::MAX`] of them, and this is far below that
/// on purpose: a driver with four thousand channels at once is a driver that
/// has run out of something else first, and the slots are preallocated so that
/// a bind never allocates.
pub const SLOT_CAPACITY: usize = 4096;

/// One past the highest port.
const PORTS: usize = 1 << 16;

/// The address field's value for a wildcard bind. No real address can collide
/// with it, and `0.0.0.0` is the wildcard anyway — the caller passes `None`.
const WILDCARD: u32 = u32::MAX;

/// The default ephemeral range, which is Linux's `ip_local_port_range`
/// (`/proc/sys/net/ipv4/ip_local_port_range`), because a peer that sees one of
/// these has no way to tell which stack chose it.
pub const EPHEMERAL_PORTS: RangeInclusive<u16> = 32768..=60999;

/// Which slot a port belongs to, and which binding of that slot it is.
///
/// `generation` is the slot's at the moment the binding was made: a frame
/// pushed under it reaches the transport that bound the port, and a frame
/// pushed under any other is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Binding {
    /// The slot the binding lives in.
    pub slot: SlotId,
    /// The port bound — the requested one, or the one that was allocated.
    pub port: u16,
    /// Which binding of the slot this is.
    pub generation: u32,
}

/// One slot's binding, in atomics: written under the bind lock and read on the
/// receive path.
#[derive(Debug, Default)]
struct Slot {
    /// The bound address, or [`WILDCARD`].
    address: AtomicU32,
    /// The connected peer, packed as `(address << 16) | port`, or zero for a
    /// transport that is not connected. Zero is free because a peer's port is
    /// never zero.
    peer: AtomicU64,
    /// Incremented by every bind and every release; see the module note.
    generation: AtomicU32,
}

impl Slot {
    /// The peer a connected transport hears from, or `None` when it is not
    /// connected.
    fn connected_peer(&self) -> Option<SocketAddr> {
        let packed = self.peer.load(Ordering::Acquire);
        if packed == 0 {
            return None;
        }

        Some(SocketAddr::from((
            Ipv4Addr::from((packed >> 16) as u32),
            packed as u16,
        )))
    }
}

/// What a bind and a release change, under one lock.
#[derive(Debug)]
struct Bindings {
    /// The slots nobody holds, newest first.
    free: Vec<SlotId>,
    /// Where the ephemeral search resumes, so that two binds in a row do not
    /// both scan from the bottom.
    next_ephemeral: u16,
}

/// The `(address, port)` pairs in use, and how a destination finds them.
#[derive(Debug)]
pub struct PortTable {
    entries: Box<[AtomicU16]>,
    slots: Box<[Slot]>,
    bindings: Mutex<Bindings>,
    ephemeral: RangeInclusive<u16>,
}

impl PortTable {
    /// A table with room for `capacity` bindings, allocating ephemeral ports
    /// from `ephemeral`.
    ///
    /// # Panics
    ///
    /// When `capacity` is zero or larger than the port table can name.
    pub fn new(capacity: usize, ephemeral: RangeInclusive<u16>) -> Self {
        assert!(capacity > 0 && capacity < usize::from(u16::MAX));

        let entries = (0..PORTS)
            .map(|_| AtomicU16::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let slots = (0..capacity)
            .map(|_| Slot::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let next_ephemeral = *ephemeral.start();

        Self {
            entries,
            slots,
            bindings: Mutex::new(Bindings {
                free: (0..capacity as SlotId).rev().collect(),
                next_ephemeral,
            }),
            ephemeral,
        }
    }

    /// How many bindings this table can hold at once.
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// How many it holds now.
    pub fn in_use(&self) -> usize {
        self.slots.len() - self.bindings.lock().expect("the bindings").free.len()
    }

    /// Bind `(address, port)`, allocating a port from the ephemeral range when
    /// `port` is zero.
    ///
    /// `address` is `None` for a wildcard bind, which hears every address this
    /// port has. `peer` is the far end a connected transport hears from and
    /// sends to.
    ///
    /// `failure_at` is the address the failure is reported against —
    /// `(address, port)` with the caller's address in it, which for a wildcard
    /// bind is what the channel asked for rather than what was stored.
    ///
    /// # Errors
    ///
    /// [`BindFailure`] with `EADDRINUSE` when the port is taken, and with
    /// `EADDRNOTAVAIL` when every ephemeral port is.
    pub fn bind(
        &self,
        address: Option<Ipv4Addr>,
        port: u16,
        peer: Option<SocketAddr>,
        failure_at: SocketAddr,
    ) -> Result<Binding, BindFailure> {
        let mut bindings = self.bindings.lock().expect("the bindings");

        let port = if port == 0 {
            self.allocate_ephemeral(&mut bindings)
                .ok_or_else(|| failure(libc::EADDRNOTAVAIL, failure_at))?
        } else {
            if self.entries[usize::from(port)].load(Ordering::Acquire) != 0 {
                return Err(failure(libc::EADDRINUSE, failure_at));
            }
            port
        };

        let Some(slot) = bindings.free.pop() else {
            return Err(failure(libc::ENOBUFS, failure_at));
        };

        // Filled before the entry names it, so that a receive that finds the
        // slot finds the binding that goes with it.
        let entry = &self.slots[usize::from(slot)];
        entry
            .address
            .store(address.map_or(WILDCARD, u32::from), Ordering::Relaxed);
        entry.peer.store(pack_peer(peer), Ordering::Relaxed);
        let generation = entry.generation.fetch_add(1, Ordering::AcqRel) + 1;

        self.entries[usize::from(port)].store(slot + 1, Ordering::Release);

        Ok(Binding {
            slot,
            port,
            generation,
        })
    }

    /// Give a port back. After this the port can be bound again, and the
    /// binding that was handed out is dead: nothing pushed under its
    /// generation reaches a transport bound afterwards.
    pub fn release(&self, binding: Binding) {
        let mut bindings = self.bindings.lock().expect("the bindings");
        let slot = &self.slots[usize::from(binding.slot)];

        // Only the binding that holds the slot may give it up. This is the
        // ABA the generation exists for: a transport that closed and a
        // transport that bound the same port afterwards get the same slot
        // back, and the first one's release arriving late must not free the
        // port out from under the second. Without this test the check below
        // sees the same `slot + 1` it wrote and cannot tell the two apart.
        if slot.generation.load(Ordering::Acquire) != binding.generation {
            return;
        }

        let index = usize::from(binding.port);
        if self.entries[index].load(Ordering::Acquire) != binding.slot + 1 {
            return;
        }

        self.entries[index].store(0, Ordering::Release);
        slot.address.store(WILDCARD, Ordering::Relaxed);
        slot.peer.store(0, Ordering::Relaxed);
        slot.generation.fetch_add(1, Ordering::AcqRel);

        bindings.free.push(binding.slot);
    }

    /// The binding a datagram delivered to `destination` from `source` reaches,
    /// or `None` when nothing bound that destination port, the binding is for
    /// another address, or a connected transport is hearing from somebody else.
    ///
    /// `own` is this port's own address: a wildcard binding hears datagrams
    /// addressed to it.
    pub fn lookup(
        &self,
        own: Ipv4Addr,
        destination: Ipv4Addr,
        port: u16,
        source: SocketAddr,
    ) -> Option<Binding> {
        let entry = self.entries[usize::from(port)].load(Ordering::Acquire);
        if entry == 0 {
            return None;
        }

        let slot = entry - 1;
        let record = self.slots.get(usize::from(slot))?;

        let bound = record.address.load(Ordering::Acquire);
        let expected = if bound == WILDCARD { own } else { bound.into() };
        if destination != expected {
            return None;
        }

        if let Some(peer) = record.connected_peer() {
            if peer != source {
                return None;
            }
        }

        Some(Binding {
            slot,
            port,
            generation: record.generation.load(Ordering::Acquire),
        })
    }

    /// Point a connected binding at a different peer, which is what a name
    /// that resolved somewhere else needs (`aeron_udp_channel_transport_reconnect`,
    /// `aeron_udp_channel_transport.c:376-392`).
    ///
    /// A binding that has already been replaced is left alone: the generation
    /// says which binding of the slot this is, and only the current one may
    /// change who it hears from.
    pub fn reconnect(&self, binding: Binding, peer: SocketAddr) {
        let slot = &self.slots[usize::from(binding.slot)];
        if slot.generation.load(Ordering::Acquire) != binding.generation {
            return;
        }

        slot.peer.store(pack_peer(Some(peer)), Ordering::Release);
    }

    /// The next free ephemeral port, in range order from wherever the last
    /// allocation left off.
    fn allocate_ephemeral(&self, bindings: &mut Bindings) -> Option<u16> {
        let first = bindings.next_ephemeral;
        let mut candidate = first;

        loop {
            if self.entries[usize::from(candidate)].load(Ordering::Acquire) == 0 {
                bindings.next_ephemeral = if candidate == *self.ephemeral.end() {
                    *self.ephemeral.start()
                } else {
                    candidate + 1
                };
                return Some(candidate);
            }

            candidate = if candidate == *self.ephemeral.end() {
                *self.ephemeral.start()
            } else {
                candidate + 1
            };
            if candidate == first {
                return None;
            }
        }
    }
}

/// A bind failure in the shape the endpoints compose their report from.
///
/// The descriptor is `-1` because there is none: the report's first line reads
/// `failed to bind(-1, 10.10.0.1:10456)`, and the number is what says this
/// transport has no socket rather than that its socket is number minus one.
fn failure(errno: libc::c_int, address: SocketAddr) -> BindFailure {
    BindFailure {
        source: std::io::Error::from_raw_os_error(errno),
        fd: -1,
        address,
    }
}

fn pack_peer(peer: Option<SocketAddr>) -> u64 {
    match peer.map(|peer| (peer.ip(), peer.port())) {
        Some((std::net::IpAddr::V4(address), port)) => {
            (u64::from(u32::from(address)) << 16) | u64::from(port)
        }
        // An IPv6 peer cannot arise — the stack has no IPv6, which the design
        // refuses at the channel (§4.11) — and one is stored as "not
        // connected" rather than as some address that could accidentally
        // match, so that the mistake shows up as a filter that is off rather
        // than as a filter that is on for the wrong peer.
        Some((std::net::IpAddr::V6(_), _)) | None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);

    fn table() -> PortTable {
        PortTable::new(SLOT_CAPACITY, EPHEMERAL_PORTS)
    }

    fn peer(address: &str, port: u16) -> SocketAddr {
        SocketAddr::from((address.parse::<Ipv4Addr>().expect("an address"), port))
    }

    /// G1: a channel that asks for port zero is given one, and it is one the
    /// kernel's own range would have picked.
    #[test]
    fn a_port_of_zero_comes_back_as_one_from_the_ephemeral_range() {
        let table = table();
        let binding = table
            .bind(Some(OWN), 0, None, SocketAddr::from((OWN, 0)))
            .expect("a bind");

        assert!(EPHEMERAL_PORTS.contains(&binding.port), "{}", binding.port);
        assert_eq!(
            Some(binding),
            table.lookup(OWN, OWN, binding.port, peer("10.10.0.9", 1))
        );

        let second = table
            .bind(Some(OWN), 0, None, SocketAddr::from((OWN, 0)))
            .expect("a bind");
        assert_ne!(binding.port, second.port, "and a second one is different");
    }

    /// G2: a second bind of one address is refused, in the shape the endpoints
    /// report.
    #[test]
    fn a_second_bind_on_one_address_is_refused() {
        let table = table();
        let address = SocketAddr::from((OWN, 10456));
        table.bind(Some(OWN), 10456, None, address).expect("a bind");

        let failure = table
            .bind(Some(OWN), 10456, None, address)
            .expect_err("the second bind");

        assert_eq!(Some(libc::EADDRINUSE), failure.source.raw_os_error());
        assert_eq!(address, failure.address);
        assert_eq!(-1, failure.fd, "a transport with no descriptor says so");
    }

    /// The wildcard and the explicit address are one port: binding either
    /// takes it from the other, which is what the kernel does without
    /// `SO_REUSEADDR`.
    #[test]
    fn the_wildcard_and_the_address_are_the_same_port() {
        let table = table();
        let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 10456));
        table.bind(None, 10456, None, address).expect("a bind");

        assert!(table.bind(Some(OWN), 10456, None, address).is_err());
    }

    /// G3: a wildcard binding hears what is addressed to the port's own
    /// address, and a specific binding hears only that address.
    #[test]
    fn a_wildcard_binding_hears_the_ports_own_address() {
        let table = table();
        let wildcard = table
            .bind(None, 5000, None, SocketAddr::from((OWN, 5000)))
            .expect("a bind");

        assert_eq!(
            Some(wildcard),
            table.lookup(OWN, OWN, 5000, peer("10.10.0.9", 1))
        );
        assert_eq!(
            None,
            table.lookup(OWN, Ipv4Addr::new(10, 10, 0, 2), 5000, peer("10.10.0.9", 1)),
            "and only that address"
        );
    }

    #[test]
    fn a_specific_binding_hears_only_the_address_it_named() {
        let table = table();
        let other = Ipv4Addr::new(10, 10, 0, 7);
        table
            .bind(Some(other), 5001, None, SocketAddr::from((other, 5001)))
            .expect("a bind");

        assert_eq!(None, table.lookup(OWN, OWN, 5001, peer("10.10.0.9", 1)));
        assert!(
            table
                .lookup(OWN, other, 5001, peer("10.10.0.9", 1))
                .is_some()
        );
    }

    /// G5: a connected transport hears its peer and nobody else. Turning this
    /// off is the falsification the design asks for, and what it would let
    /// through is another session's status messages on a shared port.
    #[test]
    fn a_connected_binding_hears_only_its_peer() {
        let table = table();
        let subscriber = peer("10.10.0.9", 10000);
        let binding = table
            .bind(
                Some(OWN),
                5002,
                Some(subscriber),
                SocketAddr::from((OWN, 5002)),
            )
            .expect("a bind");

        assert_eq!(Some(binding), table.lookup(OWN, OWN, 5002, subscriber));
        assert_eq!(
            None,
            table.lookup(OWN, OWN, 5002, peer("10.10.0.9", 10001)),
            "another port on the same host is somebody else"
        );
        assert_eq!(None, table.lookup(OWN, OWN, 5002, peer("10.10.0.8", 10000)));
    }

    /// G12: the port is free the moment the binding is released — no
    /// `TIME_WAIT`, no lingering socket, and the port can be bound again at
    /// once.
    #[test]
    fn a_released_port_can_be_bound_again() {
        let table = table();
        let address = SocketAddr::from((OWN, 5003));
        let first = table.bind(Some(OWN), 5003, None, address).expect("a bind");

        table.release(first);

        assert_eq!(None, table.lookup(OWN, OWN, 5003, peer("10.10.0.9", 1)));
        let second = table
            .bind(Some(OWN), 5003, None, address)
            .expect("a rebind");
        assert!(second.generation > first.generation, "a new binding of it");
        assert_eq!(
            Some(second),
            table.lookup(OWN, OWN, 5003, peer("10.10.0.9", 1))
        );
    }

    /// A binding that has been released is dead: a frame looked up under it
    /// does not reach whoever binds the port next.
    #[test]
    fn a_released_binding_cannot_be_looked_up_under() {
        let table = table();
        let address = SocketAddr::from((OWN, 5004));
        let first = table.bind(Some(OWN), 5004, None, address).expect("a bind");
        table.release(first);

        let second = table
            .bind(Some(OWN), 5004, None, address)
            .expect("a rebind");

        assert_eq!(first.slot, second.slot, "the same slot, reused");
        assert_ne!(first.generation, second.generation);

        let found = table
            .lookup(OWN, OWN, 5004, peer("10.10.0.9", 1))
            .expect("the port is bound again");
        assert_eq!(
            second.generation, found.generation,
            "and a push carries the new generation, not the old one"
        );
    }

    /// Releasing a binding somebody else already replaced must not free the
    /// port out from under the new one.
    #[test]
    fn releasing_a_stale_binding_leaves_the_new_one_alone() {
        let table = table();
        let address = SocketAddr::from((OWN, 5005));
        let first = table.bind(Some(OWN), 5005, None, address).expect("a bind");
        table.release(first);
        let second = table
            .bind(Some(OWN), 5005, None, address)
            .expect("a rebind");

        table.release(first);

        assert_eq!(
            Some(second),
            table.lookup(OWN, OWN, 5005, peer("10.10.0.9", 1))
        );
    }

    /// A stack with every slot taken refuses a bind rather than overwriting
    /// one, and the refusal is an error the caller can report.
    #[test]
    fn a_full_table_refuses_a_bind() {
        let table = PortTable::new(2, EPHEMERAL_PORTS);
        table
            .bind(Some(OWN), 6000, None, SocketAddr::from((OWN, 6000)))
            .expect("a bind");
        table
            .bind(Some(OWN), 6001, None, SocketAddr::from((OWN, 6001)))
            .expect("a bind");

        let failure = table
            .bind(Some(OWN), 6002, None, SocketAddr::from((OWN, 6002)))
            .expect_err("the third");

        assert_eq!(Some(libc::ENOBUFS), failure.source.raw_os_error());
        assert_eq!(2, table.in_use());
    }

    /// The ephemeral search skips ports that are taken, and gives up when the
    /// whole range is.
    #[test]
    fn an_exhausted_ephemeral_range_is_an_error() {
        let table = PortTable::new(4, 10000..=10001);
        table
            .bind(Some(OWN), 10000, None, SocketAddr::from((OWN, 10000)))
            .expect("a bind");
        table
            .bind(Some(OWN), 10001, None, SocketAddr::from((OWN, 10001)))
            .expect("a bind");

        let failure = table
            .bind(Some(OWN), 0, None, SocketAddr::from((OWN, 0)))
            .expect_err("a third");

        assert_eq!(Some(libc::EADDRNOTAVAIL), failure.source.raw_os_error());
    }

    /// A wire's worth of concurrent lookups against a table that is being
    /// bound and released underneath them: no lookup may report a binding the
    /// table does not hold, which is the one thing the atomics are for.
    ///
    /// A stress test and not a proof — the design's answer to "is this table
    /// sound" is a hand-written one, because the tables here are single-writer
    /// per field and the alternative (`loom`) would need a CI job of its own
    /// to run at all (§7 P-D2-4 of the plan).
    #[test]
    fn lookups_and_binds_do_not_tear() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;

        let table = Arc::new(table());
        let stop = Arc::new(AtomicBool::new(false));
        let port = 5006;

        let reader = {
            let table = Arc::clone(&table);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut found = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if let Some(binding) = table.lookup(OWN, OWN, port, peer("10.10.0.9", 1)) {
                        // Everything a binding says about itself has to be
                        // true of the table: the port that was asked for, a
                        // slot that exists, and a generation that was taken
                        // (a slot's first binding is 1, never 0).
                        assert_eq!(port, binding.port);
                        assert!(usize::from(binding.slot) < table.capacity());
                        assert!(binding.generation > 0);
                        found += 1;
                    }
                }
                found
            })
        };

        for _ in 0..1000 {
            let binding = table
                .bind(Some(OWN), port, None, SocketAddr::from((OWN, port)))
                .expect("a bind");
            table.release(binding);
        }

        stop.store(true, Ordering::Relaxed);
        reader.join().expect("the reader thread");
    }
}
