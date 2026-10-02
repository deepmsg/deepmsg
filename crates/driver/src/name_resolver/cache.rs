//! The resolver's cache: the names its neighbors have told it about
//! (`aeron-driver/src/main/c/aeron_name_resolver_cache.c`, 161 lines, and the
//! header's 75).
//!
//! A resolver that gossips learns names from two directions at once — the
//! answers it receives, and the answers it is asked for — and this is the store
//! both sides touch: an entry arrives with the age the sender put on it, so an
//! answer that has travelled is not mistaken for a fresh one
//! (`aeron_driver_name_resolver.c:689`), and it is believed until a timeout
//! measured from **that** moment, not from its arrival (`:114`).
//!
//! The identity of an entry is the name **and the wire type**: the same name
//! may be known over IPv4 and over IPv6, and the two are different rows
//! (`aeron_name_resolver_cache_find_index_by_name_and_type`, `:46-62`). What is
//! *not* part of the identity is the address, so a neighbor that moves updates
//! its row instead of appearing twice.
//!
//! The store is a `Vec` in this build where the reference keeps an array, a
//! length and a capacity. Two things are deliberately the same shape: an entry
//! is removed by an **unordered** swap, so the last row takes the removed one's
//! place (`aeron_array_fast_unordered_remove`, `util/aeron_arrayutil.h:39-46`),
//! and the caller is handed the entries counter to publish, because in the
//! reference the value lives in the counter region the resolver was given
//! rather than in the cache.

use std::net::{IpAddr, SocketAddr};

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::protocol::{RES_TYPE_NAME_TO_IP4, RES_TYPE_NAME_TO_IP6, ResolutionDatagram};

/// An address as a resolver keeps it (`aeron_name_resolver_cache_addr_t`,
/// `aeron_name_resolver_cache.h:22-28`): the type the entry carried, the port
/// out of the entry's own header, and the address that type sizes.
///
/// The C struct is a fixed sixteen bytes of address with only the first four
/// used for an IPv4 one, which is what makes the type load-bearing rather than
/// decorative: it is the only thing that says how much of those bytes mean
/// anything (`aeron_driver_name_resolver_find_neighbor_by_addr`, `:612-620`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheAddress {
    /// `AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD` or `..._IP6_MD`.
    pub res_type: i8,
    /// The port, which lives in the entry's header and nowhere in its address.
    pub port: u16,
    /// The address, whose family and `res_type` always agree.
    pub address: IpAddr,
}

impl CacheAddress {
    /// The address an entry carries (`aeron_driver_name_resolver_receive`,
    /// `aeron_driver_name_resolver.c:763-792`).
    pub fn from_entry(entry: &ResolutionDatagram<'_>) -> Self {
        Self {
            res_type: entry.res_type,
            port: entry.udp_port,
            address: entry.address,
        }
    }

    /// The same address as a socket address, which is what a caller resolves a
    /// name *to* (`aeron_driver_name_resolver_to_sockaddr`, `:538-565`).
    pub const fn to_socket_addr(self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }

    /// The address a datagram came from, as a cache address
    /// (`aeron_driver_name_resolver_from_sockaddr`, `:567-594`, which reads the
    /// family to pick the type and the port out of the socket address).
    pub const fn from_socket_addr(address: SocketAddr) -> Self {
        Self {
            res_type: if address.is_ipv6() {
                RES_TYPE_NAME_TO_IP6
            } else {
                RES_TYPE_NAME_TO_IP4
            },
            port: address.port(),
            address: address.ip(),
        }
    }

    /// Whether the address is the wildcard for its family — `0.0.0.0`, or the
    /// first four bytes of the sixteen being zero
    /// (`aeron_driver_name_resolver_is_wildcard`, `:710-716`).
    ///
    /// The reference writes that test as two comparisons and the second one
    /// subsumes the first: it memcmp's the **first four bytes** of whatever
    /// address it was handed against `INADDR_ANY`, without asking what type the
    /// address is. So this is not "the address is unspecified" — an IPv6
    /// address that merely *starts* with four zero bytes reads as the wildcard,
    /// which `::1` does. It is only ever asked about a resolver's **own**
    /// address in a self-resolution, where the caller means "I booted on the
    /// wildcard and do not know my address" and replaces it with the source
    /// address the datagram came from (`:803-812`).
    pub fn is_wildcard(self) -> bool {
        match self.address {
            IpAddr::V4(address) => [0u8; 4] == address.octets(),
            IpAddr::V6(address) => [0u8; 4] == address.octets()[..4],
        }
    }
}

/// One name's answer (`aeron_name_resolver_cache_entry_t`, `:30-38`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    /// What the name stands for now.
    pub address: CacheAddress,
    /// When this answer stops being believed: the moment it was last about,
    /// plus the cache's timeout (`:114`).
    pub deadline_ms: i64,
    /// The moment the answer was last **about** — which is the datagram's own
    /// `now - age_in_ms` and not its arrival
    /// (`aeron_driver_name_resolver.c:689`).
    pub time_of_last_activity_ms: i64,
    /// The name: the whole channel name being resolved, not the host alone.
    pub name: String,
}

/// The cache (`aeron_name_resolver_cache_t`, `:40-51`).
#[derive(Clone, Debug, Default)]
pub struct NameResolverCache {
    /// How long an answer is believed after the moment it was about. A zero
    /// timeout is what the reference's own test uses to keep entries alive
    /// across the whole run (`aeron_name_resolver_cache_test.cpp:44`).
    timeout_ms: i64,
    entries: Vec<CacheEntry>,
}

impl NameResolverCache {
    /// An empty cache (`aeron_name_resolver_cache_init`, `:24-29`).
    pub const fn new(timeout_ms: i64) -> Self {
        Self {
            timeout_ms,
            entries: Vec::new(),
        }
    }

    /// The entries, in whatever order the swaps have left them in — the order
    /// is not part of any contract (`:46-62` searches by name).
    pub fn entries(&self) -> &[CacheEntry] {
        &self.entries
    }

    /// Whether a name is known over that wire type.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Add a name, or update the row it already has, answering whether it was
    /// **new** (`aeron_name_resolver_cache_add_or_update`, `:64-117`, whose
    /// `num_updated` is this).
    ///
    /// The counter is published only when a row is appended, which is what the
    /// reference does and all it does: an update moves a deadline and nothing a
    /// reader can see (`:102-104`).
    pub fn add_or_update(
        &mut self,
        name: &str,
        address: CacheAddress,
        time_of_last_activity_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        entries_counter: i32,
    ) -> bool {
        let deadline_ms = time_of_last_activity_ms + self.timeout_ms;

        match self.find_index(name, address.res_type) {
            Some(index) => {
                let entry = &mut self.entries[index];
                entry.address = address;
                entry.time_of_last_activity_ms = time_of_last_activity_ms;
                entry.deadline_ms = deadline_ms;

                false
            }
            None => {
                self.entries.push(CacheEntry {
                    address,
                    deadline_ms,
                    time_of_last_activity_ms,
                    name: name.to_owned(),
                });

                let _ = counters.set_value(regions, entries_counter, self.entries.len() as i64);

                true
            }
        }
    }

    /// The entry for a name over a wire type, if there is one
    /// (`aeron_name_resolver_cache_lookup_by_name`, `:119-134`, whose `-1` is
    /// this `None`).
    pub fn lookup(&self, name: &str, res_type: i8) -> Option<&CacheEntry> {
        self.find_index(name, res_type)
            .map(|index| &self.entries[index])
    }

    /// Drop every entry whose deadline has passed, answering how many went
    /// (`aeron_name_resolver_cache_timeout_old_entries`, `:136-161`).
    ///
    /// The comparison is inclusive — an entry whose deadline *is* now is gone
    /// (`entry->deadline_ms <= now_ms`, `:144`) — and the counter is published
    /// only when something was removed.
    pub fn timeout_old_entries(
        &mut self,
        now_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        entries_counter: i32,
    ) -> usize {
        let mut removed = 0;

        // Downward, because removal swaps the last row into the hole: every row
        // the index has not reached yet has already been looked at, and every
        // row it has still has to be.
        for index in (0..self.entries.len()).rev() {
            if self.entries[index].deadline_ms <= now_ms {
                self.entries.swap_remove(index);
                removed += 1;
            }
        }

        if removed > 0 {
            let _ = counters.set_value(regions, entries_counter, self.entries.len() as i64);
        }

        removed
    }

    /// The row a name and a wire type stand for, which is the whole of the
    /// cache's identity rule (`aeron_name_resolver_cache_find_index_by_name_and_type`,
    /// `:46-62`).
    fn find_index(&self, name: &str, res_type: i8) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.address.res_type == res_type && entry.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::AtomicBuffer;

    /// The two counter regions, which belong to whoever opens them — the cache
    /// is handed a view and never keeps one.
    struct Counters {
        metadata: Vec<u8>,
        values: Vec<u8>,
    }

    impl Counters {
        fn new() -> Self {
            Self {
                metadata: vec![0u8; 64 * 1024 * 4],
                values: vec![0u8; 64 * 1024],
            }
        }

        fn open(&mut self) -> CounterRegions<'_> {
            CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.values).expect("aligned"),
            )
            .expect("four-to-one")
        }
    }

    /// The counters, the region they live in, and the one counter a cache
    /// publishes its size through.
    struct Fixture {
        holder: Counters,
        counters: CounterManager,
        entries_counter: i32,
    }

    impl Fixture {
        fn new() -> Self {
            let mut fixture = Self {
                holder: Counters::new(),
                counters: CounterManager::new(64 * 1024, 1_000).expect("room"),
                entries_counter: 0,
            };

            let regions = fixture.holder.open();
            fixture.entries_counter = fixture
                .counters
                .allocate(
                    &regions,
                    crate::position::type_id::NAME_RESOLVER_CACHE_ENTRIES,
                    b"cache",
                    b"Resolver cache entries: name=A",
                    0,
                )
                .expect("one counter");

            fixture
        }

        /// What a reader of the counter region sees, which is what a test — and
        /// the reference's own — asserts against.
        fn counter(&mut self) -> i64 {
            let regions = self.holder.open();
            self.counters
                .value(&regions, self.entries_counter)
                .expect("an allocated counter")
        }
    }

    fn address(res_type: i8, port: u16, octets: [u8; 4]) -> CacheAddress {
        CacheAddress {
            res_type,
            port,
            address: IpAddr::from(octets),
        }
    }

    /// A thousand rows, half of them over IPv6, each found again by its name —
    /// and an IPv4 row is **not** an answer for an IPv6 lookup of the same
    /// name, which is the half of the identity that is easy to drop
    /// (`aeron_name_resolver_cache_test.cpp:42-72`, which asserts the same
    /// thousand the same way).
    #[test]
    fn a_row_is_found_by_its_name_and_its_wire_type() {
        let mut fixture = Fixture::new();
        let mut cache = NameResolverCache::new(0);

        for i in 0..1000u16 {
            let name = format!("hostname{i}");
            let res_type = if i % 2 == 1 {
                RES_TYPE_NAME_TO_IP6
            } else {
                RES_TYPE_NAME_TO_IP4
            };
            let expected = address(res_type, i, [1, 2, 3, 4]);
            let regions = fixture.holder.open();

            assert!(
                cache.add_or_update(
                    &name,
                    expected,
                    0,
                    &fixture.counters,
                    &regions,
                    fixture.entries_counter
                ),
                "row {i} is new"
            );

            assert_eq!(
                Some(&expected),
                cache.lookup(&name, res_type).map(|e| &e.address)
            );

            let other = if res_type == RES_TYPE_NAME_TO_IP4 {
                RES_TYPE_NAME_TO_IP6
            } else {
                RES_TYPE_NAME_TO_IP4
            };
            assert_eq!(
                None,
                cache.lookup(&name, other),
                "row {i} is not an answer over the other type"
            );
        }

        assert_eq!(1000, cache.len());
        assert_eq!(1000, fixture.counter(), "and the counter counts them");
    }

    /// An answer that arrives again moves the row's deadline rather than
    /// appending a second one, and leaves the counter alone
    /// (`aeron_name_resolver_cache_add_or_update`, `:106-116`).
    #[test]
    fn an_answer_that_arrives_again_updates_the_row() {
        let mut fixture = Fixture::new();
        let mut cache = NameResolverCache::new(2000);
        let name = "somewhere:40456";
        let first = address(RES_TYPE_NAME_TO_IP4, 40456, [127, 0, 0, 1]);
        let second = address(RES_TYPE_NAME_TO_IP4, 40457, [10, 0, 0, 1]);
        let regions = fixture.holder.open();

        assert!(cache.add_or_update(
            name,
            first,
            0,
            &fixture.counters,
            &regions,
            fixture.entries_counter
        ));
        assert!(!cache.add_or_update(
            name,
            second,
            1_000,
            &fixture.counters,
            &regions,
            fixture.entries_counter
        ));

        assert_eq!(1, cache.len());
        assert_eq!(1, fixture.counter(), "an update is not a new row");

        let entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4).expect("the row");
        assert_eq!(second, entry.address, "the address is the newer one");
        assert_eq!(1_000, entry.time_of_last_activity_ms);
        assert_eq!(
            2_000,
            entry.deadline_ms - entry.time_of_last_activity_ms,
            "and the deadline is still measured from the moment the answer was about"
        );
    }

    /// The reference's own timeout test, with its numbers
    /// (`aeron_name_resolver_cache_test.cpp:74-114`): five rows two seconds
    /// apart, a two-second timeout, one of them refreshed at the end — and two
    /// of the five are gone, the refreshed one is not, and the two that were
    /// most recent are still there.
    #[test]
    fn the_deadline_is_what_times_a_row_out() {
        let mut fixture = Fixture::new();
        let mut cache = NameResolverCache::new(2000);
        let expected = address(RES_TYPE_NAME_TO_IP4, 7, [127, 0, 0, 1]);
        let mut now_ms = 0;

        for i in 0..5 {
            now_ms = i * 1_000;
            let regions = fixture.holder.open();

            cache.add_or_update(
                &format!("hostname{i}"),
                expected,
                now_ms,
                &fixture.counters,
                &regions,
                fixture.entries_counter,
            );
        }

        assert_eq!(5, fixture.counter(), "the counter and the length agree");

        let regions = fixture.holder.open();
        cache.add_or_update(
            "hostname1",
            expected,
            now_ms,
            &fixture.counters,
            &regions,
            fixture.entries_counter,
        );
        assert_eq!(
            2,
            cache.timeout_old_entries(now_ms, &fixture.counters, &regions, fixture.entries_counter),
            "the two whose two seconds ran out"
        );

        assert_eq!(3, cache.len(), "five rows, two of them timed out");
        assert!(cache.lookup("hostname1", RES_TYPE_NAME_TO_IP4).is_some());
        assert!(cache.lookup("hostname3", RES_TYPE_NAME_TO_IP4).is_some());
        assert!(cache.lookup("hostname4", RES_TYPE_NAME_TO_IP4).is_some());
        assert!(cache.lookup("hostname0", RES_TYPE_NAME_TO_IP4).is_none());
        assert!(cache.lookup("hostname2", RES_TYPE_NAME_TO_IP4).is_none());

        assert_eq!(3, fixture.counter(), "and the counter followed it down");
    }

    /// The address a datagram came from, which is what replaces a resolver's
    /// own wildcard address in a self-resolution
    /// (`aeron_driver_name_resolver.c:803-812`).
    #[test]
    fn a_wildcard_is_a_wildcard_the_way_the_reference_tests_for_one() {
        assert!(address(RES_TYPE_NAME_TO_IP4, 1, [0, 0, 0, 0]).is_wildcard());
        assert!(!address(RES_TYPE_NAME_TO_IP4, 1, [0, 0, 0, 1]).is_wildcard());

        let loopback: SocketAddr = "[::1]:40456".parse().expect("an address");
        assert!(CacheAddress::from_socket_addr(loopback).is_wildcard());
        assert_eq!(
            RES_TYPE_NAME_TO_IP6,
            CacheAddress::from_socket_addr(loopback).res_type
        );
        assert_eq!(
            40456,
            CacheAddress::from_socket_addr(loopback)
                .to_socket_addr()
                .port()
        );

        let routable: SocketAddr = "[2001:db8::1]:40456".parse().expect("an address");
        assert!(!CacheAddress::from_socket_addr(routable).is_wildcard());
    }
}
