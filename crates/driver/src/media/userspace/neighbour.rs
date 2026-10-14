//! Next hop → MAC address: the table, how it is filled, and when it ages out.
//!
//! A frame cannot leave without one, and the kernel is not here to ask, so the
//! stack keeps the table a kernel would: what it learns from the traffic it
//! sees, what it is told statically, and what it has to ask for with ARP.
//!
//! # One lock, and why not two atomics
//!
//! The design first drew this table as a fixed array with two atomics per
//! entry — the address and the MAC — and the writer being the receive thread.
//! Taken seriously, that shape does not hold:
//!
//! * **There are two writers, not one.** An unresolved next hop has to send an
//!   ARP request, and the request is throttled per address; the sender thread
//!   (data) and the receiver thread (status messages) both run into misses, so
//!   both write the "asked at" state, and the receiver writes the MACs it
//!   learns.
//! * **Ageing needs a timestamp.** "300 seconds without a refresh" is a third
//!   value per entry, and 48 bits of MAC plus 16 bits of state is not room for
//!   a monotonic nanosecond count.
//! * **The read order has to be pinned.** "Read the address, read the MAC,
//!   read the address again" only works if the writer invalidates before it
//!   writes the address and validates after the MAC; nothing about the atomics
//!   enforces that, so a reader can see a new address with an old MAC and both
//!   reads agree.
//!
//! One mutex answers all three at once, and it is the same price this stack
//! already pays per send for a transmit port (§3.7 of the plan). The lock-free
//! version is a thing to measure before writing, not to assume.

use std::net::Ipv4Addr;
use std::time::Duration;

/// How many next hops one stack remembers at once.
///
/// A link's worth, not a routing table's: the entries are the peers this stack
/// is talking to plus whatever it has overheard, and a peer that falls out of
/// a table this size is one it will ask about again.
pub const NEIGHBOUR_CAPACITY: usize = 256;

/// One remembered next hop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    address: Ipv4Addr,
    hardware: [u8; 6],
    /// When the entry was last confirmed, in the stack's own nanoseconds.
    refreshed_ns: i64,
    /// When an ARP request for it was last sent, if one has been.
    asked_ns: Option<i64>,
    /// Whether a MAC is known. An entry that exists only because a request was
    /// sent has no MAC, and answering with the zeroes it holds would be a
    /// black hole rather than a miss.
    resolved: bool,
    /// A configured entry, which never ages out.
    fixed: bool,
}

/// What the table knows about one address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextHop {
    /// A MAC to send to.
    Known([u8; 6]),
    /// Nothing to send to yet.
    Unknown,
}

/// The table: addresses, MACs, and the two times that go with them.
#[derive(Debug)]
pub struct NeighbourTable {
    entries: Vec<Entry>,
    capacity: usize,
    timeout: Duration,
    interval: Duration,
}

impl NeighbourTable {
    /// A table that holds `capacity` entries, ages them out after `timeout`,
    /// and asks about one address at most every `interval`.
    pub fn new(capacity: usize, timeout: Duration, interval: Duration) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            capacity: capacity.max(1),
            timeout,
            interval,
        }
    }

    /// How many entries are held.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Add an address that was configured rather than learned: it never ages
    /// out, and a production deployment is expected to be all of these — a
    /// resolved address is a delay nobody has to pay.
    pub fn insert_fixed(&mut self, address: Ipv4Addr, hardware: [u8; 6]) {
        match self.position(address) {
            Some(index) => {
                self.entries[index].hardware = hardware;
                self.entries[index].resolved = true;
                self.entries[index].fixed = true;
            }
            None => self.push(Entry {
                address,
                hardware,
                refreshed_ns: 0,
                asked_ns: None,
                resolved: true,
                fixed: true,
            }),
        }
    }

    /// What to send to, for an address the stack is about to send to.
    ///
    /// `now` is the stack's clock: an entry that has not been confirmed within
    /// the timeout is not an answer any more, and the caller asks again.
    pub fn resolve(&self, address: Ipv4Addr, now: i64) -> NextHop {
        let Some(entry) = self.entries.iter().find(|entry| entry.address == address) else {
            return NextHop::Unknown;
        };

        if !entry.resolved {
            return NextHop::Unknown;
        }

        if entry.fixed || now.saturating_sub(entry.refreshed_ns) < nanos(self.timeout) {
            NextHop::Known(entry.hardware)
        } else {
            NextHop::Unknown
        }
    }

    /// Record what an address's MAC is, which is what gleaning and a reply
    /// both come to.
    ///
    /// A fixed entry is not overwritten: it was configured, and a
    /// configuration that can be changed by what arrives on the wire is not
    /// one.
    pub fn learn(&mut self, address: Ipv4Addr, hardware: [u8; 6], now: i64) {
        match self.position(address) {
            Some(index) => {
                if !self.entries[index].fixed {
                    self.entries[index].hardware = hardware;
                    self.entries[index].refreshed_ns = now;
                    self.entries[index].resolved = true;
                }
            }
            None => self.push(Entry {
                address,
                hardware,
                refreshed_ns: now,
                asked_ns: None,
                resolved: true,
                fixed: false,
            }),
        }
    }

    /// Whether an ARP request for `address` may be sent now, recording that it
    /// was.
    ///
    /// The throttle is the design's ("同一 IP 每 `arp.retry.interval` 最多一次",
    /// default one second): without it a stream of datagrams for an unresolved
    /// peer would be a stream of broadcasts.
    pub fn ask(&mut self, address: Ipv4Addr, now: i64) -> bool {
        let interval = nanos(self.interval);

        match self.position(address) {
            Some(index) => {
                let asked = self.entries[index].asked_ns;
                if asked.is_some_and(|asked| now.saturating_sub(asked) < interval) {
                    return false;
                }

                self.entries[index].asked_ns = Some(now);
                true
            }
            None => {
                self.push(Entry {
                    address,
                    hardware: [0; 6],
                    refreshed_ns: 0,
                    asked_ns: Some(now),
                    resolved: false,
                    fixed: false,
                });
                true
            }
        }
    }

    /// Whether a request for `address` is due, without recording one — what a
    /// caller that cannot send yet asks.
    pub fn ask_due(&self, address: Ipv4Addr, now: i64) -> bool {
        match self.entries.iter().find(|entry| entry.address == address) {
            Some(entry) => entry
                .asked_ns
                .is_none_or(|asked| now.saturating_sub(asked) >= nanos(self.interval)),
            None => true,
        }
    }

    /// Forget an address, which is what a `reconnect` to a *different* next
    /// hop's worth of resolution comes to.
    pub fn forget(&mut self, address: Ipv4Addr) {
        self.entries.retain(|entry| entry.address != address);
    }

    fn position(&self, address: Ipv4Addr) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.address == address)
    }

    /// Add an entry, displacing the oldest one when the table is full.
    ///
    /// Oldest is by last confirmation, and a fixed entry is never displaced:
    /// configuration outranks what was overheard.
    fn push(&mut self, entry: Entry) {
        if self.entries.len() < self.capacity {
            self.entries.push(entry);
            return;
        }

        let victim = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, candidate)| !candidate.fixed)
            .min_by_key(|(_, candidate)| candidate.refreshed_ns)
            .map(|(index, _)| index);

        // Every entry being configured is the one case with nothing to
        // displace: the entry that does not fit is the one that is dropped,
        // because a configured peer is worth more than a learned one.
        if let Some(index) = victim {
            self.entries[index] = entry;
        }
    }
}

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Whether two addresses are on the same link, given the port's own address
/// and how many leading bits of it are the subnet.
///
/// This is what decides whether a frame goes to the destination itself or to
/// the gateway: a next hop that is not on this link is only reachable through
/// something that is.
pub fn on_the_same_link(own: Ipv4Addr, other: Ipv4Addr, prefix_length: u8) -> bool {
    let prefix_length = prefix_length.min(32);
    if prefix_length == 0 {
        return true;
    }

    let mask = u32::MAX << (32 - u32::from(prefix_length));

    u32::from(own) & mask == u32::from(other) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 2);
    const FAR: Ipv4Addr = Ipv4Addr::new(10, 10, 9, 9);
    const MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x02];

    fn table() -> NeighbourTable {
        NeighbourTable::new(
            NEIGHBOUR_CAPACITY,
            Duration::from_secs(300),
            Duration::from_secs(1),
        )
    }

    #[test]
    fn an_address_nobody_has_learned_is_unknown() {
        let table = table();

        assert_eq!(NextHop::Unknown, table.resolve(PEER, 0));
    }

    #[test]
    fn what_is_learned_is_answered_with() {
        let mut table = table();
        table.learn(PEER, MAC, 1_000);

        assert_eq!(NextHop::Known(MAC), table.resolve(PEER, 1_000));
        assert_eq!(
            NextHop::Known(MAC),
            table.resolve(PEER, 1_000 + 299_999_999_999)
        );
    }

    /// The design's ageing: an entry not confirmed within the timeout is not
    /// an answer any more, and the caller asks again.
    #[test]
    fn an_entry_ages_out() {
        let mut table = table();
        table.learn(PEER, MAC, 0);

        assert_eq!(NextHop::Known(MAC), table.resolve(PEER, 299_000_000_000));
        assert_eq!(
            NextHop::Unknown,
            table.resolve(PEER, 300_000_000_000),
            "five minutes without a refresh"
        );

        table.learn(PEER, MAC, 300_000_000_000);
        assert_eq!(
            NextHop::Known(MAC),
            table.resolve(PEER, 300_000_000_000),
            "and a refresh brings it back"
        );
    }

    /// A configured entry never ages out: it is the deployment that does not
    /// want to wait for a resolution.
    #[test]
    fn a_configured_entry_never_ages_out() {
        let mut table = table();
        table.insert_fixed(FAR, [0x02, 0, 0, 0, 0, 0x99]);

        assert_eq!(
            NextHop::Known([0x02, 0, 0, 0, 0, 0x99]),
            table.resolve(FAR, i64::MAX)
        );
    }

    /// A configured entry is not overwritten by what arrives on the wire.
    #[test]
    fn what_arrives_does_not_change_what_was_configured() {
        let mut table = table();
        table.insert_fixed(PEER, MAC);
        table.learn(PEER, [0x02, 0, 0, 0, 0, 0x77], 1_000);

        assert_eq!(NextHop::Known(MAC), table.resolve(PEER, 1_000));
    }

    /// One address at a time: the throttle that keeps a stream of datagrams
    /// for an unresolved peer from being a stream of broadcasts.
    #[test]
    fn a_request_is_throttled_per_address() {
        let mut table = table();

        assert!(table.ask(PEER, 0), "the first one goes");
        assert!(!table.ask(PEER, 999_999_999), "the second is held back");
        assert!(table.ask(PEER, 1_000_000_000), "and the next second's goes");
        assert!(
            table.ask(FAR, 1_000_000_000),
            "another address has its own throttle"
        );
    }

    #[test]
    fn asking_does_not_make_an_address_resolvable() {
        let mut table = table();
        assert!(table.ask(PEER, 0));

        assert_eq!(
            NextHop::Unknown,
            table.resolve(PEER, 0),
            "asking is not answering"
        );
    }

    /// The table is bounded, and what it displaces is the entry that has gone
    /// longest without being confirmed.
    #[test]
    fn a_full_table_displaces_the_stalest_entry() {
        let mut table = NeighbourTable::new(2, Duration::from_secs(300), Duration::from_secs(1));
        let third = Ipv4Addr::new(10, 10, 0, 3);

        table.learn(PEER, MAC, 100);
        table.learn(third, [0x02, 0, 0, 0, 0, 0x03], 200);
        table.learn(FAR, [0x02, 0, 0, 0, 0, 0x09], 300);

        assert_eq!(2, table.len());
        assert_eq!(NextHop::Unknown, table.resolve(PEER, 300), "the staler one");
        assert_eq!(
            NextHop::Known([0x02, 0, 0, 0, 0, 0x03]),
            table.resolve(third, 300)
        );
        assert_eq!(
            NextHop::Known([0x02, 0, 0, 0, 0, 0x09]),
            table.resolve(FAR, 300)
        );
    }

    /// A configured entry is never the one displaced.
    #[test]
    fn a_full_table_does_not_displace_a_configured_entry() {
        let mut table = NeighbourTable::new(1, Duration::from_secs(300), Duration::from_secs(1));
        table.insert_fixed(FAR, [0x02, 0, 0, 0, 0, 0x09]);

        table.learn(PEER, MAC, 1_000);

        assert_eq!(1, table.len());
        assert_eq!(
            NextHop::Known([0x02, 0, 0, 0, 0, 0x09]),
            table.resolve(FAR, 1_000),
            "the configuration stays"
        );
        assert_eq!(NextHop::Unknown, table.resolve(PEER, 1_000));
    }

    #[test]
    fn forgetting_an_address_leaves_nothing_behind() {
        let mut table = table();
        table.learn(PEER, MAC, 0);
        table.forget(PEER);

        assert_eq!(NextHop::Unknown, table.resolve(PEER, 0));
        assert!(table.is_empty());
    }

    /// The next hop is the destination itself on this link, and the gateway
    /// off it.
    #[test]
    fn the_link_is_what_decides_the_next_hop() {
        assert!(on_the_same_link(OWN, PEER, 24));
        assert!(!on_the_same_link(OWN, FAR, 24));
        assert!(on_the_same_link(OWN, FAR, 16));
        assert!(on_the_same_link(OWN, FAR, 0), "no prefix is every address");
        assert!(
            on_the_same_link(OWN, OWN, 32),
            "a host route is one address"
        );
        assert!(
            !on_the_same_link(OWN, PEER, 32),
            "and nobody else's address is that address"
        );
    }
}
