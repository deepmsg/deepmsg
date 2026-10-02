//! The resolver a driver keeps for itself (`aeron-driver/src/main/c/aeron_driver_name_resolver.c`,
//! 1318 lines).
//!
//! It is the one resolver with a **socket of its own** and a clock of its own,
//! and that is the whole of what it is: it gossips. Every other resolver answers
//! a question about a name by asking the system; this one answers out of what
//! its neighbors have told it, and tells them what it knows in return, so that
//! a name can change its address underneath a running stream and both ends find
//! out.
//!
//! Three kinds of datagram matter on that socket, each on its own interval:
//!
//! * **self-resolutions**, carrying this resolver's own name and address with
//!   the `SELF_FLAG` set (`:923-1021`), which is how a neighbor learns what to
//!   call this driver — and how a driver that booted on the wildcard address
//!   gets a real address on the other end, because the receiver substitutes the
//!   address the datagram came from (`:803-812`);
//! * **neighbor-resolutions**, carrying the whole cache as one or more
//!   datagrams (`:1023-1085`), which is how knowledge travels past the first
//!   hop;
//! * **bootstrap-resolutions**, which are not sent but *made*: the configured
//!   bootstrap neighbors are re-resolved through the ordinary default resolver
//!   on an interval (`:184-232`, `:1279-1284`).
//!
//! Everything it receives goes through the cache ([`super::cache`]) and through
//! the neighbor list kept beside it, and both are aged on the duty cycle:
//! entries by the cache timeout, neighbors by the neighbor timeout
//! (`:1087-1112`).
//!
//! # What is deliberately the same shape as the reference
//!
//! The **duty cycle** is ten milliseconds and it gates *all* of the work, not
//! just the sending (`:1257-1289`): a resolver whose deadline has not come does
//! not poll its socket either. The three clocks start at different moments —
//! the neighbor and bootstrap deadlines are set at init and the self-resolution
//! one is left at zero, so the first self-resolution goes out on the first pass
//! (`:469-474`).
//!
//! The **bootstrap neighbors are reversed on the way in** in the reference, and
//! that is not a bug either: `aeron_tokenise` walks the string backwards and
//! fills its array from the end (`util/aeron_strutil.c:112-161`), and the
//! reference undoes that (`:368-370`), so the list comes out in the order it was
//! configured. This build splits forwards and does not undo anything, which is
//! the same order.
//!
//! # Two things this build does not carry
//!
//! Interceptors (a resolver's datagrams are not loss-injected or timestamped —
//! `docs/compat.md`) and the driver agent's event log, which this file never
//! writes to. The system counters a failure moves **are** kept; the
//! distinct-error-log entry that goes with them arrives with the conductor
//! wiring, because that log belongs to the driver rather than to a resolver.

use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::media::Datagrams;
use crate::media::{Transport, TransportParams, UdpTransport};
use crate::name_resolver::cache::{CacheAddress, NameResolverCache};
use crate::name_resolver::{DefaultResolver, Lookup, Resolution, Resolver};
use crate::position::type_id;
use crate::protocol::{
    FrameHeader, HEADER_LENGTH, RES_FLAG_SELF, RES_TYPE_NAME_TO_IP4, RES_TYPE_NAME_TO_IP6,
    ResolutionDatagram, ResolutionFrame, VERSION, frame_type,
};
use crate::publication_params::MAX_UDP_PAYLOAD_LENGTH;
use crate::sys::AddressFamily;
use crate::sys::socket::MAX_BATCH;
use crate::system_counters;
use crate::udp_channel::{format_source_identity, resolve_host_and_port, resolve_interface};

/// `AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS` (`:54`): how often the resolver
/// does anything at all.
pub const DUTY_CYCLE_MS: i64 = 10;

/// `AERON_NAME_RESOLVER_DRIVER_MAX_BOOTSTRAP_NEIGHBORS` (`:57`), which is also
/// the point past which a configuration is refused rather than truncated
/// (`aeron_tokenise` answers `-ERANGE`).
pub const MAX_BOOTSTRAP_NEIGHBORS: usize = 20;

/// What a resolver is built from — the settings of the Java `Configuration`
/// (`aeron-driver/src/main/java/io/aeron/driver/Configuration.java:1032-1214`),
/// gathered into one place because a resolver with nine scalars in its
/// constructor is unreadable.
#[derive(Clone, Debug)]
pub struct Params {
    /// `aeron.driver.resolver.name` (`:1032`): what this driver answers to.
    /// Its own name resolves to its own address without asking anyone
    /// (`:1219-1223`).
    pub name: String,
    /// `aeron.driver.resolver.interface` (`:1040`): what the resolver binds its
    /// socket to, and the address it announces.
    pub interface: String,
    /// `aeron.driver.resolver.bootstrap.neighbor` (`:1049`): comma-separated
    /// names to start gossiping with, at most [`MAX_BOOTSTRAP_NEIGHBORS`].
    pub bootstrap_neighbor: Option<String>,
    /// The context's MTU, which is how large a neighbor-resolution datagram may
    /// grow — a full cache is split across several (`:1045`).
    pub mtu_length: usize,
    /// `SO_RCVBUF`, in bytes; zero leaves the kernel's default.
    pub socket_rcvbuf: usize,
    /// `SO_SNDBUF`, in bytes; zero leaves the kernel's default.
    pub socket_sndbuf: usize,
    /// `aeron.driver.resolver.neighbor.timeout` (`:1172-1178`): how long an
    /// unheard-from neighbor, and a cache entry, stay — the reference hands the
    /// same number to both (`:463`).
    pub neighbor_timeout_ms: i64,
    /// `aeron.driver.resolver.self.resolution.interval` (`:1184-1190`).
    pub self_resolution_interval_ms: i64,
    /// `aeron.driver.resolver.neighbor.resolution.interval` (`:1196-1202`).
    pub neighbor_resolution_interval_ms: i64,
    /// `aeron.driver.resolver.bootstrap.neighbor.resolution.interval`
    /// (`:1208-1214`).
    pub bootstrap_neighbor_resolution_interval_ms: i64,
}

impl Default for Params {
    /// The reference's own defaults, from the Java `Configuration` lines above.
    fn default() -> Self {
        Self {
            name: String::new(),
            interface: String::new(),
            bootstrap_neighbor: None,
            mtu_length: 1408,
            socket_rcvbuf: 0,
            socket_sndbuf: 0,
            neighbor_timeout_ms: 10_000,
            self_resolution_interval_ms: 1_000,
            neighbor_resolution_interval_ms: 2_000,
            bootstrap_neighbor_resolution_interval_ms: 10_000,
        }
    }
}

/// One neighbor: who it is on the wire, and when it was last heard from
/// (`aeron_driver_name_resolver_neighbor_t`, `:59-65`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Neighbor {
    /// What the datagram said, which is also what the neighbor list is
    /// searched by (`:605-624`).
    cache_addr: CacheAddress,
    /// The moment the last answer from it was **about** — the datagram's own
    /// `now - age_in_ms`, not its arrival (`:689`).
    time_of_last_activity_ms: i64,
    /// The same address as a socket address, kept beside it because that is
    /// what a send needs and what the two log callbacks are handed
    /// (`:647-654`).
    socket_addr: SocketAddr,
}

/// One configured bootstrap neighbor (`:78-90`): the name as written, the
/// address it last resolved to, and the counter a reader sees it through.
#[derive(Clone, Debug)]
struct BootstrapNeighbor {
    /// The name as it appeared in the configuration, which is what its
    /// counter's label carries (`:164-182`).
    name: String,
    /// The address it resolves to, or `None` while it does not resolve — which
    /// is `ss_family = AF_UNSPEC` in the reference, and prints as nothing
    /// (`:213`).
    address: Option<SocketAddr>,
    /// `AERON_COUNTER_NAME_RESOLVER_BOOTSTRAP_NEIGHBOR_COUNTER_TYPE_ID`, one
    /// per configured name, whose value says whether that neighbor is currently
    /// in the neighbor list (`:1013-1018`).
    counter_id: i32,
}

/// The driver's own resolver.
pub struct DriverResolver {
    name: String,
    /// What the socket is bound to, and what this resolver announces as its own
    /// address (`local_socket_addr`, `:71`).
    local_socket_addr: SocketAddr,
    /// The same, in the shape the cache and the wire use (`local_cache_addr`,
    /// `:72`).
    local_cache_addr: CacheAddress,
    bootstrap_neighbors: Vec<BootstrapNeighbor>,
    transport: UdpTransport,
    cache: NameResolverCache,
    neighbors: Vec<Neighbor>,
    mtu_length: usize,
    neighbor_timeout_ms: i64,
    self_resolution_interval_ms: i64,
    neighbor_resolution_interval_ms: i64,
    bootstrap_neighbor_resolution_interval_ms: i64,
    work_deadline_ms: i64,
    bootstrap_neighbor_resolve_deadline_ms: i64,
    self_resolutions_deadline_ms: i64,
    neighbor_resolutions_deadline_ms: i64,
    /// The last `now_ms` the resolver worked at, which is what an entry's
    /// `age_in_ms` is subtracted from.
    last_work_ms: i64,
    /// `AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID` (`:477-489`).
    neighbor_counter: i32,
    /// `AERON_COUNTER_NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID` (`:493-497`).
    cache_entries_counter: i32,
    invalid_packets_counter: i32,
    short_sends_counter: i32,
    error_counter: i32,
    /// The buffers one receive batch fills. Taken out of `self` for the
    /// duration of a poll so that a datagram can be handled while the socket is
    /// borrowed — the reference's single receive buffer (`:55`, `:828-843`)
    /// widened to the batch the rest of this driver uses.
    receive_buffers: Vec<Vec<u8>>,
    receive_datagrams: Datagrams,
    /// The buffer a frame is built in before it is sent, the reference's
    /// `aligned_buffer` (`:136-137`): reused, because a resolver sends on a
    /// clock and neither path allocates.
    send_buffer: Vec<u8>,
    /// The resolver this one asks when its own knowledge runs out — and the
    /// answer to a name it does not know, which is every name that is neither
    /// its own nor in its cache (`bootstrap_resolver`, `:96`, used at
    /// `:1226-1227`).
    ///
    /// The reference builds it from a supplier of its own
    /// (`driver_name_resolver_bootstrap_resolver_supplier_func`, `:299-307`),
    /// defaulting to the default resolver; no setting in `aeronmd.h` or
    /// `Configuration.java` names another, so this build holds that default —
    /// but it holds it **as a resolver**, which is the part that matters:
    /// [`Resolver::resolve`] takes a host, not `host:port`.
    ///
    /// This is where `DriverNameResolverSystemTest`'s
    /// `shouldResolveDriverNameAndAllowConnection` failed: the fall-through
    /// called `udp_channel::resolve_host_and_port`, whose contract is
    /// `host:port`, so a channel naming `localhost:24325` was answered with
    /// `port invalid: '': localhost` — the port half of an address the
    /// resolver was never given. Same shape as the slice's other contract
    /// corrections: the wrapper's signature is not the callee's contract.
    ///
    /// `Send` beside `Resolver` because the whole resolver is: the supplier
    /// hands one back as `Box<dyn Resolver + Send>`, and the reference's own
    /// reason for that is the one written up in `docs/compat.md` — it runs on
    /// the native-resource agent's thread, not the conductor's.
    bootstrap_resolver: Box<dyn Resolver + Send>,
}

impl DriverResolver {
    /// Build a resolver: bind its socket, take its counters, and read the
    /// bootstrap list (`aeron_driver_name_resolver_init`, `:264-523`).
    ///
    /// The counters are taken here and **not given back on close**, which is
    /// the reference's own shape: they belong to the driver's counter region,
    /// which is torn down with the driver (`:245-262` frees memory and not a
    /// single counter).
    ///
    /// # Errors
    ///
    /// What could not be done: an interface that is not an address, a bootstrap
    /// list of more than [`MAX_BOOTSTRAP_NEIGHBORS`] names, a socket that could
    /// not be bound, or a counter that could not be taken.
    pub fn new(
        params: &Params,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> Result<Self, String> {
        let local_socket_addr = resolve_interface(&params.interface)
            .map_err(|error| format!("failed to parse interface: {}: {error}", params.interface))?;
        let local_cache_addr = CacheAddress::from_socket_addr(local_socket_addr);

        let neighbor_label = neighbor_counter_label(local_socket_addr)?;
        let neighbor_counter = counters
            .allocate(
                regions,
                type_id::NAME_RESOLVER_NEIGHBORS,
                &[],
                neighbor_label.as_bytes(),
                now_ms,
            )
            .ok_or_else(|| "could not allocate the neighbor counter".to_owned())?;

        let cache_label = format!("Resolver cache entries: name={}", params.name);
        let cache_entries_counter = counters
            .allocate(
                regions,
                type_id::NAME_RESOLVER_CACHE_ENTRIES,
                &[],
                cache_label.as_bytes(),
                now_ms,
            )
            .ok_or_else(|| "could not allocate the cache counter".to_owned())?;

        let mut bootstrap_neighbors = Vec::new();

        if let Some(configured) = &params.bootstrap_neighbor {
            let names: Vec<&str> = configured.split(',').collect();

            if names.len() > MAX_BOOTSTRAP_NEIGHBORS {
                return Err(format!(
                    "failed to parse bootstrap neighbors list: {configured}"
                ));
            }

            for (index, name) in names.iter().enumerate() {
                // The key is the neighbor's **index**, as an `int`
                // (`:382-386`, which passes `&i`), so a reader can tell which
                // configured name a counter belongs to without parsing the
                // label.
                let key = i32::try_from(index)
                    .map_err(|_| "too many bootstrap neighbors".to_owned())?
                    .to_le_bytes();
                let label = bootstrap_neighbor_label(name, None);

                let counter_id = counters
                    .allocate(
                        regions,
                        type_id::NAME_RESOLVER_BOOTSTRAP_NEIGHBOR,
                        &key,
                        label.as_bytes(),
                        now_ms,
                    )
                    .ok_or_else(|| format!("could not allocate a counter for {name}"))?;

                bootstrap_neighbors.push(BootstrapNeighbor {
                    name: (*name).to_owned(),
                    address: None,
                    counter_id,
                });
            }
        }

        let transport = UdpTransport::open(
            local_socket_addr,
            None, // Unicast only.
            None, // No connected.
            &TransportParams {
                socket_rcvbuf: params.socket_rcvbuf,
                socket_sndbuf: params.socket_sndbuf,
                ..TransportParams::default()
            },
        )
        .map_err(|error| {
            format!(
                "resolver, name={} interface_name={} bootstrap_neighbor={}: {error}",
                params.name,
                params.interface,
                params.bootstrap_neighbor.as_deref().unwrap_or("")
            )
        })?;

        Ok(Self {
            name: params.name.clone(),
            local_socket_addr,
            local_cache_addr,
            bootstrap_neighbors,
            transport,
            cache: NameResolverCache::new(params.neighbor_timeout_ms),
            neighbors: Vec::new(),
            mtu_length: params.mtu_length,
            neighbor_timeout_ms: params.neighbor_timeout_ms,
            self_resolution_interval_ms: params.self_resolution_interval_ms,
            neighbor_resolution_interval_ms: params.neighbor_resolution_interval_ms,
            bootstrap_neighbor_resolution_interval_ms: params
                .bootstrap_neighbor_resolution_interval_ms,
            work_deadline_ms: 0,
            bootstrap_neighbor_resolve_deadline_ms: now_ms
                + params.bootstrap_neighbor_resolution_interval_ms,
            self_resolutions_deadline_ms: 0,
            neighbor_resolutions_deadline_ms: now_ms + params.neighbor_resolution_interval_ms,
            last_work_ms: now_ms,
            neighbor_counter,
            cache_entries_counter,
            invalid_packets_counter: system_counters::id::INVALID_PACKETS,
            short_sends_counter: system_counters::id::SHORT_SENDS,
            error_counter: system_counters::id::ERRORS,
            receive_buffers: (0..MAX_BATCH)
                .map(|_| vec![0u8; MAX_UDP_PAYLOAD_LENGTH as usize])
                .collect(),
            receive_datagrams: Datagrams::new(),
            send_buffer: vec![0u8; MAX_UDP_PAYLOAD_LENGTH as usize],
            bootstrap_resolver: Box::new(DefaultResolver),
        })
    }

    /// The address the resolver's socket is bound to, and the one it announces
    /// as its own.
    pub const fn local_socket_addr(&self) -> SocketAddr {
        self.local_socket_addr
    }

    /// How many neighbors it currently believes in — which is the value of the
    /// neighbor counter.
    pub fn neighbors(&self) -> usize {
        self.neighbors.len()
    }

    /// How many names it currently knows — the value of the cache counter.
    pub fn cached_names(&self) -> usize {
        self.cache.len()
    }

    /// The counters this resolver took, for a caller that wants to read them.
    pub fn counter_ids(&self) -> Vec<i32> {
        let mut ids = vec![self.neighbor_counter, self.cache_entries_counter];
        ids.extend(
            self.bootstrap_neighbors
                .iter()
                .map(|neighbor| neighbor.counter_id),
        );

        ids
    }

    /// One pass of the resolver's own clock (`aeron_driver_name_resolver_do_work`,
    /// `:1252-1292`).
    ///
    /// Everything hangs off the one deadline: while it has not come, not even
    /// the socket is polled. The three senders have deadlines of their own
    /// inside it, and each is pushed to `now + interval` — **not** by the
    /// interval, so a resolver that was not run for a while does not send a
    /// burst of catch-up datagrams.
    pub fn work(
        &mut self,
        now_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        self.last_work_ms = now_ms;

        if self.work_deadline_ms > now_ms {
            return 0;
        }

        let mut work_count = self.poll(counters, regions);

        work_count +=
            self.cache
                .timeout_old_entries(now_ms, counters, regions, self.cache_entries_counter);
        work_count += self.timeout_neighbors(now_ms, counters, regions);

        if self.self_resolutions_deadline_ms <= now_ms {
            work_count += self.send_self_resolutions(counters, regions);
            self.self_resolutions_deadline_ms = now_ms + self.self_resolution_interval_ms;
        }

        if self.neighbor_resolutions_deadline_ms <= now_ms {
            work_count += self.send_neighbor_resolutions(now_ms, counters, regions);
            self.neighbor_resolutions_deadline_ms = now_ms + self.neighbor_resolution_interval_ms;
        }

        if !self.bootstrap_neighbors.is_empty()
            && self.bootstrap_neighbor_resolve_deadline_ms <= now_ms
        {
            work_count += self.resolve_bootstrap_neighbors(counters, regions);
            self.bootstrap_neighbor_resolve_deadline_ms =
                now_ms + self.bootstrap_neighbor_resolution_interval_ms;
        }

        self.work_deadline_ms = now_ms + DUTY_CYCLE_MS;

        work_count
    }

    /// Read whatever is waiting on the socket and handle it, answering with how
    /// many bytes arrived (`aeron_driver_name_resolver_poll`, `:826-862`).
    ///
    /// A poll that failed is not nothing: it costs the resolver's error counter
    /// (`:855-859`) and the pass carries on, because the cache and the neighbor
    /// list are aged either way.
    fn poll(&mut self, counters: &CounterManager, regions: &CounterRegions<'_>) -> usize {
        let mut buffers = std::mem::take(&mut self.receive_buffers);
        let mut datagrams = std::mem::take(&mut self.receive_datagrams);

        let received = match self.transport.receive(&mut buffers, &mut datagrams) {
            Ok(received) => received,
            Err(error) => {
                if error.kind() != std::io::ErrorKind::WouldBlock {
                    self.record_error(counters, regions);
                }

                self.receive_buffers = buffers;
                self.receive_datagrams = datagrams;

                return 0;
            }
        };

        let mut bytes = 0;

        for (index, datagram) in datagrams.as_slice().iter().take(received).enumerate() {
            bytes += datagram.length;
            let Some(buffer) = buffers.get(index) else {
                break;
            };

            self.receive(
                &buffer[..datagram.length],
                datagram.source,
                counters,
                regions,
            );
        }

        self.receive_buffers = buffers;
        self.receive_datagrams = datagrams;

        bytes
    }

    /// Handle one datagram (`aeron_driver_name_resolver_receive`, `:725-824`).
    ///
    /// Three ways to be refused, and the reference counts them differently: a
    /// datagram that is not a whole `RES` frame of this version, or an entry
    /// whose padding the datagram does not carry, is an **invalid packet**
    /// (`:740-742`, `:769-774`); an entry whose type is neither of the two is an
    /// **error**, and it stops the walk rather than being skipped
    /// (`:794-799`).
    fn receive(
        &mut self,
        packet: &[u8],
        source: Option<SocketAddr>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) {
        let Some(frame) = ResolutionFrame::read(packet) else {
            system_counters::increment(counters, regions, self.invalid_packets_counter);

            return;
        };

        let mut remaining = frame.entry_bytes();

        while !remaining.is_empty() {
            if remaining.len() < HEADER_LENGTH {
                system_counters::increment(counters, regions, self.invalid_packets_counter);

                return;
            }

            let Some(entry) = ResolutionDatagram::read(remaining) else {
                if is_known_res_type(remaining[0] as i8) {
                    system_counters::increment(counters, regions, self.invalid_packets_counter);
                } else {
                    self.record_error(counters, regions);
                }

                return;
            };

            if remaining.len() < entry.entry_length() {
                system_counters::increment(counters, regions, self.invalid_packets_counter);

                return;
            }

            let mut cache_addr = CacheAddress::from_entry(&entry);
            let is_self = RES_FLAG_SELF == (entry.res_flags & RES_FLAG_SELF);

            // A resolver that booted on the wildcard does not know its own
            // address, so what it announces is the wildcard — and the one thing
            // the receiver *does* know is where the datagram came from
            // (`:803-812`).
            if is_self && cache_addr.is_wildcard() {
                let Some(source) = source else {
                    self.record_error(counters, regions);

                    return;
                };

                cache_addr = CacheAddress::from_socket_addr(source);
            }

            if self
                .on_resolution_entry(&entry, cache_addr, is_self, counters, regions)
                .is_err()
            {
                self.record_error(counters, regions);
            }

            remaining = &remaining[entry.entry_length()..];
        }
    }

    /// Take one entry into the cache and the neighbor list
    /// (`aeron_driver_name_resolver_on_resolution_entry`, `:672-708`).
    ///
    /// The entry about **this** resolver itself is dropped, which is how a
    /// resolver that hears its own self-resolution back does not add itself as
    /// its own neighbor (`:681-687`, which compares the port and the name).
    fn on_resolution_entry(
        &mut self,
        entry: &ResolutionDatagram<'_>,
        cache_addr: CacheAddress,
        is_self: bool,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Result<(), String> {
        let name = String::from_utf8_lossy(entry.name).into_owned();

        if cache_addr.port == self.local_socket_addr.port() && name == self.name {
            return Ok(());
        }

        let time_of_last_activity_ms = self.last_work_ms - i64::from(entry.age_in_ms);

        self.cache.add_or_update(
            &name,
            cache_addr,
            time_of_last_activity_ms,
            counters,
            regions,
            self.cache_entries_counter,
        );

        self.add_neighbor(
            cache_addr,
            is_self,
            time_of_last_activity_ms,
            counters,
            regions,
        );

        Ok(())
    }

    /// Add a neighbor, or refresh one that is already there when the entry said
    /// it was that neighbor's own (`aeron_driver_name_resolver_add_neighbor`,
    /// `:626-670`, whose three answers are the three arms here).
    ///
    /// The counter is published only when the list grows (`:658`), and the
    /// `is_self` refresh is what keeps a neighbor alive across its timeout
    /// without the counter moving.
    fn add_neighbor(
        &mut self,
        cache_addr: CacheAddress,
        is_self: bool,
        time_of_last_activity_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) {
        match self.find_neighbor(cache_addr) {
            None => {
                self.neighbors.push(Neighbor {
                    cache_addr,
                    time_of_last_activity_ms,
                    socket_addr: cache_addr.to_socket_addr(),
                });

                let _ =
                    counters.set_value(regions, self.neighbor_counter, self.neighbors.len() as i64);
            }
            Some(index) if is_self => {
                self.neighbors[index].time_of_last_activity_ms = time_of_last_activity_ms;
            }
            Some(_) => {}
        }
    }

    /// Where a neighbor with that address sits in the list, if it is there
    /// (`aeron_driver_name_resolver_find_neighbor_by_addr`, `:605-624`).
    fn find_neighbor(&self, cache_addr: CacheAddress) -> Option<usize> {
        self.neighbors
            .iter()
            .position(|neighbor| neighbor.cache_addr == cache_addr)
    }

    /// Drop the neighbors that have gone quiet
    /// (`aeron_driver_name_resolver_timeout_neighbors`, `:1087-1112`), whose
    /// test is inclusive: a neighbor whose last activity plus the timeout **is**
    /// now is gone.
    fn timeout_neighbors(
        &mut self,
        now_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        let mut removed = 0;

        for index in (0..self.neighbors.len()).rev() {
            if self.neighbors[index].time_of_last_activity_ms + self.neighbor_timeout_ms <= now_ms {
                self.neighbors.swap_remove(index);
                removed += 1;
            }
        }

        if removed > 0 {
            let _ = counters.set_value(regions, self.neighbor_counter, self.neighbors.len() as i64);
        }

        removed
    }

    /// Resolve every bootstrap neighbor that is not a neighbor yet, and label
    /// its counter with what it resolved to
    /// (`aeron_driver_name_resolver_resolve_bootstrap_neighbors`, `:184-232`).
    ///
    /// The resolution is an ordinary one — the same synchronous call a channel
    /// makes — so a bootstrap name is as re-resolvable as any other; it is the
    /// *interval* that makes it periodic (`:1279-1284`).
    fn resolve_bootstrap_neighbors(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        let mut work_count = 0;

        for index in 0..self.bootstrap_neighbors.len() {
            let in_neighbor_list = self.bootstrap_neighbors[index]
                .address
                .is_some_and(|address| self.neighbors.iter().any(|n| n.socket_addr == address));

            if in_neighbor_list {
                continue;
            }

            let name = self.bootstrap_neighbors[index].name.clone();
            let address = match resolve_host_and_port(&name) {
                Ok(address) => Some(address),
                Err(_) => {
                    self.record_error(counters, regions);

                    None
                }
            };

            self.bootstrap_neighbors[index].address = address;
            work_count += 1;

            let label = bootstrap_neighbor_label(&name, address);
            let _ = counters.update_label(
                regions,
                self.bootstrap_neighbors[index].counter_id,
                label.as_bytes(),
            );
        }

        work_count
    }

    /// Send this resolver's own name and address to everyone it knows
    /// (`aeron_driver_name_resolver_send_self_resolutions`, `:923-1021`).
    ///
    /// Nobody to tell is nothing to do (`:926-929`), which is what keeps a lone
    /// resolver quiet. The datagram carries **one** entry, tagged `SELF_FLAG`,
    /// whose age is zero — there is nothing stale about "I am here"
    /// (`:954`).
    ///
    /// The bootstrap neighbors are sent to even when they are not yet in the
    /// neighbor list, because that is the point of them, and each of their
    /// counters is then set to whether that address *is* in the list
    /// (`:1013-1018`) — how a deployment sees a bootstrap that never answered.
    fn send_self_resolutions(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        if self.bootstrap_neighbors.is_empty() && self.neighbors.is_empty() {
            return 0;
        }

        let frame_length = {
            let entry = ResolutionDatagram {
                res_type: self.local_cache_addr.res_type,
                res_flags: RES_FLAG_SELF,
                udp_port: self.local_cache_addr.port,
                age_in_ms: 0,
                address: self.local_cache_addr.address,
                name: self.name.as_bytes(),
            };

            let Some(frame_length) = build_frame(&mut self.send_buffer, &[entry]) else {
                return 0;
            };

            frame_length
        };

        let mut work_count = 0;
        let mut sent_to_bootstrap: Vec<SocketAddr> = Vec::new();

        for index in 0..self.bootstrap_neighbors.len() {
            let Some(address) = self.bootstrap_neighbors[index].address else {
                continue;
            };

            if self.send_frame(frame_length, address, counters, regions) {
                work_count += 1;
            }

            sent_to_bootstrap.push(address);
        }

        for index in 0..self.neighbors.len() {
            let address = self.neighbors[index].socket_addr;

            if sent_to_bootstrap.contains(&address) {
                continue;
            }

            if self.send_frame(frame_length, address, counters, regions) {
                work_count += 1;
            }
        }

        for index in 0..self.bootstrap_neighbors.len() {
            let connected = self.bootstrap_neighbors[index]
                .address
                .is_some_and(|address| self.neighbors.iter().any(|n| n.socket_addr == address));

            let _ = counters.set_value(
                regions,
                self.bootstrap_neighbors[index].counter_id,
                i64::from(connected),
            );
        }

        work_count
    }

    /// Send the whole cache to every neighbor, in as many datagrams as the MTU
    /// takes (`aeron_driver_name_resolver_send_neighbor_resolutions`, `:1023-1085`).
    ///
    /// This is how knowledge gets past the first hop: a resolver that heard
    /// about `C` from `B` tells `A` on its own interval, and `A` never had to be
    /// configured with anything.
    ///
    /// An entry that does not fit the MTU goes into the next datagram. If **no**
    /// entry fits, the reference's loop would spin on it forever (`:1081`, where
    /// `i = j` and `j` never moved); this build stops there instead, and an MTU
    /// too small for one entry is refused at the channel, which is where the
    /// reference checks it too.
    fn send_neighbor_resolutions(
        &mut self,
        now_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        if self.neighbors.is_empty() || self.cache.is_empty() {
            return 0;
        }

        let count = self.cache.len();
        let mut work_count = 0;
        let mut index = 0;

        while index < count {
            // How many entries fit this datagram, which is the reference's
            // `capacity = mtu_length - entry_offset` test inside
            // `set_resolution_header` (`:1043-1049`, `:1146-1173`).
            let mut frame_length = HEADER_LENGTH;
            let mut packed = 0;

            while index + packed < count {
                let entry_length = entry_length_of(&self.cache.entries()[index + packed]);

                if frame_length + entry_length > self.mtu_length {
                    break;
                }

                frame_length += entry_length;
                packed += 1;
            }

            if 0 == packed {
                break;
            }

            let entries = &self.cache.entries()[index..index + packed];

            if write_neighbor_frame(&mut self.send_buffer, entries, now_ms).is_none() {
                break;
            }

            for neighbor in 0..self.neighbors.len() {
                let address = self.neighbors[neighbor].socket_addr;

                if self.send_frame(frame_length, address, counters, regions) {
                    work_count += 1;
                }
            }

            index += packed;
        }

        work_count
    }

    /// Send the frame sitting in the send buffer, answering whether it left
    /// (`aeron_driver_name_resolver_do_send`, `:890-921`).
    ///
    /// A send that left less than it was given is a **short send** — the system
    /// counter the whole driver shares — and a send that failed costs the
    /// resolver's own error counter.
    fn send_frame(
        &mut self,
        length: usize,
        address: SocketAddr,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        match self
            .transport
            .send(Some(address), &[&self.send_buffer[..length]])
        {
            Ok(sent) if sent < length => {
                system_counters::increment(counters, regions, self.short_sends_counter);

                true
            }
            Ok(_) => true,
            Err(_) => {
                self.record_error(counters, regions);

                false
            }
        }
    }

    /// Count a failure (`aeron_name_resolver_log_and_clear_error`, `:718-723`,
    /// which also records it in the distinct error log — that part needs the
    /// driver's log, which arrives with the conductor wiring).
    fn record_error(&self, counters: &CounterManager, regions: &CounterRegions<'_>) {
        system_counters::increment(counters, regions, self.error_counter);
    }
}

/// `aeron_driver_name_resolver_build_neighbor_counter_label` (`:234-243`),
/// `Resolver neighbors: bound <address>` — which the reference's own test
/// asserts verbatim (`aeron_name_resolver_test.cpp:536-537`).
fn neighbor_counter_label(local_socket_addr: SocketAddr) -> Result<String, String> {
    let identity = format_source_identity(local_socket_addr).map_err(|error| error.to_string())?;

    Ok(format!("Resolver neighbors: bound {identity}"))
}

/// `aeron_driver_name_resolver_build_bootstrap_neighbor_counter_label`
/// (`:164-182`), `Bootstrap neighbor: name=<name> resolved=<address>`, where an
/// unresolved neighbor leaves the address empty — the reference formats
/// `AF_UNSPEC` as nothing at all, and its own test asserts that empty
/// (`aeron_name_resolver_test.cpp:986`).
fn bootstrap_neighbor_label(name: &str, address: Option<SocketAddr>) -> String {
    let resolved = match address {
        Some(address) => format_source_identity(address).unwrap_or_default(),
        None => String::new(),
    };

    format!("Bootstrap neighbor: name={name} resolved={resolved}")
}

/// Whether a type is one of the two the wire has
/// (`AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD`, `..._IP6_MD`).
const fn is_known_res_type(res_type: i8) -> bool {
    res_type == RES_TYPE_NAME_TO_IP4 || res_type == RES_TYPE_NAME_TO_IP6
}

/// A frame header, then the entries, in the send buffer — what
/// `aeron_driver_name_resolver_send_self_resolutions` and
/// `..._send_neighbor_resolutions` each write before they send
/// (`:931-954`, `:1026-1064`).
fn build_frame(buffer: &mut [u8], entries: &[ResolutionDatagram<'_>]) -> Option<usize> {
    let mut length = HEADER_LENGTH;

    for entry in entries {
        length += entry.write(buffer.get_mut(length..)?)?;
    }

    FrameHeader {
        frame_length: i32::try_from(length).ok()?,
        version: VERSION,
        flags: 0,
        frame_type: frame_type::RES,
    }
    .write(buffer)?;

    Some(length)
}

/// The entry a cache row makes on the wire, with the age it has **now**: a
/// neighbor is told how old the answer is by this resolver's clock, not the age
/// it arrived with (`:1043-1053`, which writes `now_ms -
/// cache_entry->time_of_last_activity_ms` into the entry it is about to send).
///
/// The age is truncated to an `int32` by a plain cast, exactly as the reference
/// writes it (`(int32_t)(...)`, `:1053`): an answer that old is nonsense either
/// way, and the receiver clamps nothing.
fn cache_entry_datagram<'a>(
    entry: &'a crate::name_resolver::cache::CacheEntry,
    now_ms: i64,
) -> ResolutionDatagram<'a> {
    ResolutionDatagram {
        res_type: entry.address.res_type,
        res_flags: 0,
        udp_port: entry.address.port,
        age_in_ms: (now_ms - entry.time_of_last_activity_ms) as i32,
        address: entry.address.address,
        name: entry.name.as_bytes(),
    }
}

/// How long one cache entry is on the wire once its padding is counted.
fn entry_length_of(entry: &crate::name_resolver::cache::CacheEntry) -> usize {
    cache_entry_datagram(entry, 0).entry_length()
}

/// A whole neighbor-resolution datagram: the frame header and every entry
/// given, which is one datagram's worth of the cache (`:1034-1064`).
fn write_neighbor_frame(
    buffer: &mut [u8],
    entries: &[crate::name_resolver::cache::CacheEntry],
    now_ms: i64,
) -> Option<usize> {
    let mut length = HEADER_LENGTH;

    for entry in entries {
        length += cache_entry_datagram(entry, now_ms).write(buffer.get_mut(length..)?)?;
    }

    FrameHeader {
        frame_length: i32::try_from(length).ok()?,
        version: VERSION,
        flags: 0,
        frame_type: frame_type::RES,
    }
    .write(buffer)?;

    Some(length)
}

impl Resolver for DriverResolver {
    /// Answer out of the cache, and only out of the cache
    /// (`aeron_driver_name_resolver_resolve`, `:1201-1236`).
    ///
    /// Three arms, in the reference's order: what a neighbor said; else **this
    /// resolver's own name**, which is its own address and costs no lookup
    /// (`:1219-1223`); else the default resolver, synchronously.
    ///
    /// The wire type is the caller's: the reference reads it off the family of
    /// the socket address it was handed, which is the family the answer has to
    /// come back in (`:1210-1211`) — so an IPv4 question is not answered with an
    /// IPv6 row, even for the same name.
    fn resolve(
        &mut self,
        name: &str,
        uri_param_name: &str,
        is_re_resolution: bool,
        family: AddressFamily,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Resolution {
        let res_type = match family {
            AddressFamily::Inet6 => RES_TYPE_NAME_TO_IP6,
            AddressFamily::Inet => RES_TYPE_NAME_TO_IP4,
        };

        if let Some(entry) = self.cache.lookup(name, res_type) {
            return Resolution::Found(entry.address.to_socket_addr());
        }

        if name == self.name {
            return Resolution::Found(self.local_socket_addr);
        }

        // The answered address carries port zero — the contract is a **host**
        // — and the caller writes the channel's port back
        // (`aeron_name_resolver.c:181-191`).
        self.bootstrap_resolver.resolve(
            name,
            uri_param_name,
            is_re_resolution,
            family,
            counters,
            regions,
        )
    }

    /// The reference's driver resolver asks its **bootstrap resolver** and
    /// nothing else (`:1189-1199`): its own cache is for addresses, and a lookup
    /// answers with a name.
    fn lookup(
        &mut self,
        name: &str,
        uri_param_name: &str,
        is_re_lookup: bool,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Lookup {
        self.bootstrap_resolver
            .lookup(name, uri_param_name, is_re_lookup, counters, regions)
    }

    /// Resolve the bootstrap neighbors, which is the whole of what starting
    /// this resolver does (`aeron_driver_name_resolver_on_start`, `:1238-1250`).
    fn start(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Result<(), String> {
        self.resolve_bootstrap_neighbors(counters, regions);

        Ok(())
    }

    fn do_work(
        &mut self,
        now_ms: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> usize {
        self.work(now_ms, counters, regions)
    }

    /// Let the socket go (`aeron_driver_name_resolver_close`, `:525-536`).
    ///
    /// The counters stay: the reference gives none of them back (`:245-262`),
    /// and a driver's counter region is torn down once, with the driver.
    fn close(
        &mut self,
        _counters: &mut CounterManager,
        _regions: &CounterRegions<'_>,
        _now_ms: i64,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{Duration, Instant};

    use deepmsg_core::buffer::AtomicBuffer;

    use crate::system_counters::{SystemCounters, allocate_all};

    /// The two counter regions, which belong to whoever opens them — a resolver
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

    /// A driver's counters: the forty-six system ones a resolver moves, and the
    /// region they all live in.
    struct Fixture {
        holder: Counters,
        counters: CounterManager,
        system: Option<SystemCounters>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                holder: Counters::new(),
                counters: CounterManager::new(1024 * 1024, 1_000).expect("room"),
                system: None,
            }
        }

        fn allocate_system_counters(&mut self) {
            let regions = self.holder.open();
            self.system = Some(
                allocate_all(&mut self.counters, &regions, 0, 0, "", 5_000_000_000).expect("room"),
            );
        }

        /// What a reader sees *about* a counter, which is where the labels and
        /// type ids are — the reference's own tests assert on these
        /// (`aeron_name_resolver_test.cpp:536-537`, `:972`).
        fn descriptor(&mut self, counter_id: i32) -> deepmsg_cnc::CounterDescriptor {
            let regions = self.holder.open();

            regions
                .reader()
                .get(counter_id)
                .expect("an allocated counter")
        }
    }

    /// A port nobody is using, taken by binding and letting go.
    fn free_port() -> u16 {
        std::net::UdpSocket::bind("127.0.0.1:0")
            .expect("a port")
            .local_addr()
            .expect("an address")
            .port()
    }

    fn resolver(
        fixture: &mut Fixture,
        name: &str,
        port: u16,
        bootstrap_neighbor: Option<&str>,
    ) -> DriverResolver {
        let regions = fixture.holder.open();
        let params = Params {
            name: name.to_owned(),
            interface: format!("0.0.0.0:{port}"),
            bootstrap_neighbor: bootstrap_neighbor.map(str::to_owned),
            ..Params::default()
        };

        let mut resolver =
            DriverResolver::new(&params, &mut fixture.counters, &regions, 0).expect("a resolver");

        resolver
            .start(&fixture.counters, &regions)
            .expect("bootstrap neighbors resolve");

        resolver
    }

    /// A resolver takes three kinds of counter and every one of them is a byte
    /// contract: what a reader finds it by is the type id and the label, and
    /// the reference's own test asserts both verbatim.
    #[test]
    fn a_resolver_is_found_by_the_counters_it_takes() {
        let mut fixture = Fixture::new();
        let port = free_port();
        let resolver = resolver(
            &mut fixture,
            "A",
            port,
            Some("just:wrong,non_existing_host:8050,localhost:8050"),
        );
        let ids = resolver.counter_ids();

        assert_eq!(5, ids.len(), "neighbors, cache, and three bootstrap names");

        assert_eq!(
            15,
            fixture.descriptor(ids[0]).type_id,
            "AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID"
        );
        assert_eq!(
            format!("Resolver neighbors: bound 0.0.0.0:{port}"),
            fixture.descriptor(ids[0]).label
        );

        assert_eq!(
            16,
            fixture.descriptor(ids[1]).type_id,
            "AERON_COUNTER_NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID"
        );
        assert_eq!(
            "Resolver cache entries: name=A",
            fixture.descriptor(ids[1]).label
        );

        for (index, name) in ["just:wrong", "non_existing_host:8050", "localhost:8050"]
            .iter()
            .enumerate()
        {
            assert_eq!(
                21,
                fixture.descriptor(ids[2 + index]).type_id,
                "AERON_COUNTER_NAME_RESOLVER_BOOTSTRAP_NEIGHBOR_COUNTER_TYPE_ID"
            );
            assert!(
                fixture
                    .descriptor(ids[2 + index])
                    .label
                    .starts_with(&format!("Bootstrap neighbor: name={name} resolved=")),
                "the list keeps the order it was configured in: {name}"
            );
        }

        // `start` resolves them, and the one that resolves gets its address in
        // the label — the reference's own test walks the three operations of a
        // CSV table through exactly this label
        // (`aeron_name_resolver_test.cpp:972-986`).
        assert_eq!(
            "Bootstrap neighbor: name=localhost:8050 resolved=127.0.0.1:8050",
            fixture.descriptor(ids[4]).label
        );
        assert_eq!(
            "Bootstrap neighbor: name=just:wrong resolved=",
            fixture.descriptor(ids[2]).label,
            "a name that does not resolve leaves the address empty"
        );
    }

    /// Two resolvers that were told about each other gossip until each knows
    /// the other's name — which is the reference's own
    /// `shouldSeeNeighborFromBootstrap` (`aeron_name_resolver_test.cpp:496-538`),
    /// and the wildcard substitution is the half of it that is easy to miss: A
    /// announces `0.0.0.0:<port>` and B has to learn a real address from the
    /// datagram's source.
    #[test]
    fn two_resolvers_learn_each_others_names() {
        let mut fixture = Fixture::new();
        let port_b = free_port();

        let mut b = resolver(&mut fixture, "B", port_b, None);
        let port_a = free_port();
        let mut a = resolver(
            &mut fixture,
            "A",
            port_a,
            Some(&format!("127.0.0.1:{port_b}")),
        );

        let regions = fixture.holder.open();
        let mut now_ms = 0;
        let deadline = Instant::now() + Duration::from_secs(10);

        while Instant::now() < deadline {
            now_ms += DUTY_CYCLE_MS;
            a.work(now_ms, &fixture.counters, &regions);
            b.work(now_ms, &fixture.counters, &regions);

            if 1 <= a.neighbors() && 1 <= b.neighbors() {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(1, a.neighbors(), "A heard B");
        assert_eq!(1, b.neighbors(), "and B heard A");

        assert_eq!(
            Resolution::Found(format!("127.0.0.1:{port_b}").parse().expect("an address")),
            a.resolve(
                "B",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            ),
            "the announced wildcard was replaced by where the datagram came from"
        );
        assert_eq!(
            Resolution::Found(format!("127.0.0.1:{port_a}").parse().expect("an address")),
            b.resolve(
                "A",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            )
        );

        assert_eq!(
            1,
            fixture
                .counters
                .value(&regions, a.counter_ids()[0])
                .expect("an allocated counter"),
            "the neighbor counter"
        );
        assert_eq!(
            1,
            fixture
                .counters
                .value(&regions, a.counter_ids()[1])
                .expect("an allocated counter"),
            "and the cache counter"
        );
    }

    /// A name gets past the **first** hop: C is only ever configured on B, and
    /// A learns it anyway, because B sends its whole cache to everyone it knows
    /// (`aeron_driver_name_resolver_send_neighbor_resolutions`, `:1023-1085`) —
    /// the reference's own `shouldSeeNeighborFromGossip`
    /// (`aeron_name_resolver_test.cpp:575-629`, which asserts the same three-way
    /// knowledge among three resolvers).
    #[test]
    fn a_name_travels_past_the_first_hop() {
        let mut fixture = Fixture::new();
        let port_c = free_port();

        let mut c = resolver(&mut fixture, "C", port_c, None);
        let port_b = free_port();
        let mut b = resolver(
            &mut fixture,
            "B",
            port_b,
            Some(&format!("127.0.0.1:{port_c}")),
        );
        let port_a = free_port();
        let mut a = resolver(
            &mut fixture,
            "A",
            port_a,
            Some(&format!("127.0.0.1:{port_b}")),
        );

        let regions = fixture.holder.open();
        let mut now_ms = 0;
        let deadline = Instant::now() + Duration::from_secs(10);

        while Instant::now() < deadline {
            now_ms += DUTY_CYCLE_MS;
            a.work(now_ms, &fixture.counters, &regions);
            b.work(now_ms, &fixture.counters, &regions);
            c.work(now_ms, &fixture.counters, &regions);

            // A knows B and, through B, C: two names it was never told about.
            if 2 <= a.cached_names() {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(2, a.cached_names(), "A heard about B and about C");
        assert_eq!(
            Resolution::Found(format!("127.0.0.1:{port_c}").parse().expect("an address")),
            a.resolve(
                "C",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            ),
            "and C's address came from B, which was the only one configured with it"
        );
        assert!(
            matches!(
                a.resolve(
                    "D",
                    "endpoint",
                    false,
                    AddressFamily::Inet,
                    &fixture.counters,
                    &regions
                ),
                crate::name_resolver::Resolution::Failed(_)
            ),
            "and a name nobody knows is still nobody's"
        );
    }

    /// A list is believed for as long as its entries are: with the neighbor
    /// timeout reached, the neighbor and the cached name are both gone and both
    /// counters are back to zero (`:1087-1112`, and the cache's own timeout
    /// beside it).
    #[test]
    fn a_neighbor_that_goes_quiet_times_out() {
        let mut fixture = Fixture::new();
        let port_b = free_port();

        let mut b = resolver(&mut fixture, "B", port_b, None);
        let port_a = free_port();
        let mut a = resolver(
            &mut fixture,
            "A",
            port_a,
            Some(&format!("127.0.0.1:{port_b}")),
        );

        let regions = fixture.holder.open();
        let mut now_ms = 0;
        let deadline = Instant::now() + Duration::from_secs(10);

        while Instant::now() < deadline && (1 > a.neighbors() || 1 > b.neighbors()) {
            now_ms += DUTY_CYCLE_MS;
            a.work(now_ms, &fixture.counters, &regions);
            b.work(now_ms, &fixture.counters, &regions);

            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(1, a.neighbors(), "A heard B first");

        // B stops working, so nothing refreshes A's list — and A's own clock
        // walks past the timeout it was built with.
        let timeout_ms = Params::default().neighbor_timeout_ms;
        now_ms += timeout_ms + DUTY_CYCLE_MS;
        a.work(now_ms, &fixture.counters, &regions);

        assert_eq!(0, a.neighbors(), "and then it did not");
        assert_eq!(
            0,
            fixture
                .counters
                .value(&regions, a.counter_ids()[0])
                .expect("an allocated counter")
        );
        assert_eq!(
            0,
            fixture
                .counters
                .value(&regions, a.counter_ids()[1])
                .expect("an allocated counter")
        );
        assert_eq!(
            Resolution::Found(format!("0.0.0.0:{port_a}").parse().expect("an address")),
            a.resolve(
                "A",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            ),
            "a resolver always knows its own name, which is what it bound"
        );
    }

    /// The duty cycle gates **everything**, the socket included
    /// (`aeron_driver_name_resolver.c:1257-1259`, where the poll is inside the
    /// deadline and not beside it): a resolver whose deadline has not come does
    /// not look at what arrived, which is what keeps a driver's own resolver
    /// from spinning on a socket that has nothing new to say.
    #[test]
    fn the_duty_cycle_gates_the_socket_too() {
        let mut fixture = Fixture::new();
        let port = free_port();
        let mut resolver = resolver(&mut fixture, "A", port, None);

        let regions = fixture.holder.open();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").expect("a socket");
        let to: SocketAddr = format!("127.0.0.1:{port}").parse().expect("an address");

        fn hello<'a>(name: &'a [u8], port: u16) -> ResolutionDatagram<'a> {
            ResolutionDatagram {
                res_type: RES_TYPE_NAME_TO_IP4,
                res_flags: 0,
                udp_port: port,
                age_in_ms: 0,
                address: "127.0.0.1".parse().expect("an address"),
                name,
            }
        }

        let mut frame = [0u8; 64];
        let length = build_frame(&mut frame, &[hello(b"a:1", 40451)]).expect("room");
        sender.send_to(&frame[..length], to).expect("sent");

        assert!(
            0 < resolver.work(0, &fixture.counters, &regions),
            "work was done"
        );
        assert_eq!(1, resolver.neighbors(), "the first datagram was read");

        // A second datagram arrives, and the resolver is asked to work again at
        // the same moment: the deadline has not come, so it does not look.
        let length = build_frame(&mut frame, &[hello(b"b:1", 40452)]).expect("room");
        sender.send_to(&frame[..length], to).expect("sent");
        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(
            0,
            resolver.work(0, &fixture.counters, &regions),
            "nothing is done before the deadline"
        );
        assert_eq!(1, resolver.neighbors(), "and the datagram is still unread");

        resolver.work(DUTY_CYCLE_MS, &fixture.counters, &regions);
        assert_eq!(2, resolver.neighbors(), "the next pass reads it");
    }

    /// Three datagrams, three different things wrong with them, and three
    /// different counters — which is the whole of what the receive path
    /// decides (`:740-799`).
    #[test]
    fn what_the_receive_path_counts_as_invalid_and_as_an_error() {
        let mut fixture = Fixture::new();
        fixture.allocate_system_counters();

        let port = free_port();
        let mut resolver = resolver(&mut fixture, "A", port, None);

        let regions = fixture.holder.open();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").expect("a socket");
        let source_port = sender.local_addr().expect("an address").port();
        let to: SocketAddr = format!("127.0.0.1:{port}").parse().expect("an address");

        // 1. A datagram that is not a RES frame at all — no header, so nothing
        //    about it can be read.
        sender.send_to(b"not a frame", to).expect("sent");

        // 2. A RES frame whose only entry is of a type the wire does not have.
        let mut unknown_type = [0u8; 16];
        FrameHeader {
            frame_length: 16,
            version: VERSION,
            flags: 0,
            frame_type: frame_type::RES,
        }
        .write(&mut unknown_type)
        .expect("room");
        unknown_type[HEADER_LENGTH] = 0x07;

        sender.send_to(&unknown_type, to).expect("sent");

        // 3. And a real self-resolution from a resolver nobody configured,
        //    which is a neighbor and a cached name rather than a fault.
        let entry = ResolutionDatagram {
            res_type: RES_TYPE_NAME_TO_IP4,
            res_flags: RES_FLAG_SELF,
            udp_port: 40456,
            age_in_ms: 5,
            address: "0.0.0.0".parse().expect("an address"),
            name: b"stranger:40456",
        };
        let mut frame = [0u8; 64];
        let length = build_frame(&mut frame, &[entry]).expect("room");

        sender.send_to(&frame[..length], to).expect("sent");

        let mut now_ms = 0;
        let deadline = Instant::now() + Duration::from_secs(5);

        while Instant::now() < deadline && 1 > resolver.neighbors() {
            now_ms += DUTY_CYCLE_MS;
            resolver.work(now_ms, &fixture.counters, &regions);

            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(1, resolver.neighbors(), "the third datagram was a neighbor");
        assert_eq!(
            1,
            fixture
                .counters
                .value(&regions, system_counters::id::INVALID_PACKETS)
                .expect("an allocated counter"),
            "the frameless one"
        );
        assert_eq!(
            1,
            fixture
                .counters
                .value(&regions, system_counters::id::ERRORS)
                .expect("an allocated counter"),
            "and the entry of an unknown type is an error, not an invalid packet"
        );
        assert_eq!(
            Resolution::Found(
                format!("127.0.0.1:{source_port}")
                    .parse()
                    .expect("an address")
            ),
            resolver.resolve(
                "stranger:40456",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            ),
            "the wildcard it announced was replaced by the source address — and the \
             substitution takes the **whole** address, the announced port included, \
             because the reference reads both out of the datagram's source \
             (`aeron_driver_name_resolver_from_sockaddr`, `:567-594`)"
        );
    }
}
