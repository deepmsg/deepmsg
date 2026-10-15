//! The counters a DPDK media publishes (design §4.10).
//!
//! A counter is a small record in the CnC file that a client reads without
//! asking the driver anything, and the reference's DPDK media has a set of its
//! own with type ids of their own — 75 to 95, with 80 and 81 unassigned
//! (`aeron-client/src/main/c/aeron_counters.h:155-173`). What this module owns
//! is which of them a media publishes, what each one is called, and where its
//! value comes from.
//!
//! # Where the values come from, and why that splits the set
//!
//! Three kinds of number end up here, and they are written from three places:
//!
//! * **What only the device knows** — the link's state, the frames the NIC
//!   dropped for want of room, the frames it could not receive because the pool
//!   had no buffer, and the four sent/received packet-and-byte counts
//!   (`rte_eth_stats`). Read by whoever runs on a timer
//!   ([`DpdkCounters::sample`]), because they are a call into the PMD.
//! * **What the stack counts** — the frames it discarded and why
//!   ([`StackCounters`], whose atomics the receiving thread bumps as it works).
//!   Forwarded, not re-counted ([`DpdkCounters::publish`]): the stack has
//!   already counted them, and a second counter would be a second thing to keep
//!   in step.
//! * **What the pool knows** — how many frames are free, which is also a timer's
//!   question and comes from the media rather than the port.
//!
//! # Why this is not behind the `dpdk` feature
//!
//! Allocation, labels and forwarding need neither DPDK nor a port: they are the
//! CnC file and a handful of atomics. The one method that reads a device —
//! [`DpdkCounters::sample`] — reads it **through the stack**, which is where
//! the port and the pool are reachable from ([`crate::media::userspace::stack::DeviceParts`]),
//! so nothing here is gated at all and the criteria for the export path (a
//! counter allocated here is a counter a client can read) run in the gate every
//! change goes through rather than in the one that needs DPDK.
//!
//! # Port and thread, not "per ready queue"
//!
//! The design's table gives counter 88 "one per cross-thread ring, one per
//! ready queue". This build has no cross-thread ring — D2 collapsed it into the
//! per-slot ready ring, of which a stack has up to `SLOT_CAPACITY` (4096) — so
//! one counter per queue would be four thousand counters for a number nobody
//! reads per queue. It is one per port here, which is the level a drop rate is
//! read at, and the deviation is in the D4 record.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};

use deepmsg_cnc::layout;
use deepmsg_cnc::{CounterManager, CounterRegions};

use super::stack::{Stack, StackCounters};

/// The reference's own type ids for these counters
/// (`aeron-client/src/main/c/aeron_counters.h:155-173`).
///
/// They are the *type* of a counter rather than its index: the index is the
/// manager's to hand out, and two drivers' counters with the same type are the
/// same kind of number. That is what makes them readable by anything that
/// knows the reference's catalogue, which is the whole of what a counter is
/// for.
pub mod type_id {
    /// 75 `AERON_COUNTER_DPDK_PORT_INFO_TYPE_ID` — one per port; the value is
    /// the link's state.
    pub const PORT_INFO: i32 = 75;
    /// 76 `AERON_COUNTER_DPDK_TRANSPORT_TYPE_ID` — one per transport; the value
    /// is what that transport has received.
    pub const TRANSPORT: i32 = 76;
    /// 77 `AERON_COUNTER_DPDK_NOBUFS_TYPE_ID` — frames the pool could not give
    /// the port.
    pub const NOBUFS: i32 = 77;
    /// 78 `AERON_COUNTER_DPDK_TX_EAGAIN_TYPE_ID` — frames a transmit port would
    /// not take.
    pub const TX_EAGAIN: i32 = 78;
    /// 79 `AERON_COUNTER_DPDK_ERROR_TYPE_ID` — failures of the port's own
    /// calls.
    pub const ERROR: i32 = 79;
    /// 82 `AERON_COUNTER_DPDK_PKTS_TYPE_ID` — frames through the port, one
    /// counter each way.
    pub const PKTS: i32 = 82;
    /// 83 `AERON_COUNTER_DPDK_BYTES_TYPE_ID` — bytes through the port, one
    /// counter each way.
    pub const BYTES: i32 = 83;
    /// 84 `AERON_COUNTER_DPDK_MISSED_PACKETS_TYPE_ID` — frames the device
    /// dropped.
    pub const MISSED_PACKETS: i32 = 84;
    /// 85 `AERON_COUNTER_DPDK_ARP_MISS_TYPE_ID` — sends that went nowhere for
    /// want of a MAC.
    pub const ARP_MISS: i32 = 85;
    /// 86 `AERON_COUNTER_DPDK_RX_SENDER_DISCARD_TYPE_ID` — control frames no
    /// transport claimed.
    pub const RX_SENDER_DISCARD: i32 = 86;
    /// 87 `AERON_COUNTER_DPDK_POLLER_TYPE_ID` — one per poller; the pass count.
    pub const POLLER: i32 = 87;
    /// 88 `AERON_COUNTER_DPDK_QUEUE_DROP_COUNT_TYPE_ID` — frames a ready ring
    /// had no room for.
    pub const QUEUE_DROP_COUNT: i32 = 88;
    /// 89 `AERON_COUNTER_DPDK_CHECKSUM_FAILURE_TYPE_ID` — frames whose
    /// checksums did not hold.
    pub const CHECKSUM_FAILURE: i32 = 89;
    /// 90 `AERON_COUNTER_DPDK_FRAGMENTED_PACKETS_TYPE_ID` — fragments, counted
    /// with the optioned headers this stack will not parse either.
    pub const FRAGMENTED_PACKETS: i32 = 90;
    /// 91 `AERON_COUNTER_DPDK_MEMPOOL_AVAILABLE_TYPE_ID` — frames the pool has
    /// free.
    pub const MEMPOOL_AVAILABLE: i32 = 91;
    /// 92 `AERON_COUNTER_DPDK_EXTENDED_STATS_TYPE_ID` — **not allocated**: one
    /// counter per extended statistic is a counter per hardware register, and
    /// the design defers the set to D6.
    pub const EXTENDED_STATS: i32 = 92;
    /// 93 `AERON_COUNTER_DPDK_RX_UNSUPPORTED_ETHERNET_TYPE_TYPE_ID` —
    /// ethertypes that are neither IPv4 nor ARP.
    pub const RX_UNSUPPORTED_ETHERNET_TYPE: i32 = 93;
    /// 94 `AERON_COUNTER_DPDK_RX_UNSUPPORTED_PROTOCOL_TYPE_ID` — IPv4 that is
    /// not UDP or ICMP.
    pub const RX_UNSUPPORTED_PROTOCOL: i32 = 94;
    /// 95 `AERON_COUNTER_DPDK_RX_RECEIVER_DISCARD_TYPE_ID` — data frames no
    /// transport claimed.
    pub const RX_RECEIVER_DISCARD: i32 = 95;
}

/// What one of the driver's own counters is registered as.
///
/// The media's counters belong to the driver rather than to a client, so there
/// is no registration to name: zero, with no owner. The client-owned counters
/// (`path`, `publication`, ...) carry a registration id because that is how
/// their owner finds them again; these are found by their type.
const NO_CLIENT: i64 = 0;

/// What a port says about itself, which is what its counters are labelled with.
///
/// The label is the design's own (`dpdk port: <name> mac=<..> ip=<..>/<len>
/// driver=<pmd>`, §4.10) followed by what the instance counts. One shape for
/// all of a port's counters is what makes a set of them readable as a set: a
/// reader that has found one has found the name of the port they all belong to.
#[derive(Clone, Copy, Debug)]
pub struct PortIdentity<'a> {
    /// The name EAL knows the port by.
    pub name: &'a str,
    /// The port's hardware address.
    pub mac: [u8; 6],
    /// The address this media answers for.
    pub address: Ipv4Addr,
    /// How many leading bits of it are the subnet.
    pub prefix_length: u8,
    /// The PMD's name.
    pub driver: &'a str,
}

impl PortIdentity<'_> {
    /// The label a per-port counter starts with.
    fn label(&self, what: &str) -> String {
        format!(
            "dpdk port: {} mac={} ip={}/{} driver={} {what}",
            self.name,
            self.mac
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join(":"),
            self.address,
            self.prefix_length,
            self.driver,
        )
    }

    /// The key a per-port counter carries: the instance's name within its type,
    /// which is what a reader that knows the type looks up.
    fn key(what: &str) -> [u8; 4] {
        // Four bytes, the length the system counters use, and the same four
        // for a counter that is one per port.
        let _ = what;
        *b"port"
    }
}

/// Allocate the counter that says what one transport has received — type 76,
/// one per transport rather than one per port (design §4.10).
///
/// A per-port count cannot answer the question a quiet driver raises: *which*
/// channel is quiet. The transport is the thing a channel's socket is, so the
/// label names the address it is bound to and, where there is one, the peer it
/// was connected to — a wildcard bind keeps its wildcard and its chosen port,
/// which is the same reason the reference writes a local-address counter at all
/// (`crate::position`).
///
/// **The value is written by the thread that owns the transport**, one datagram
/// batch at a time ([`count_received_packets`]), and not by the conductor's
/// timer: it moves every time something arrives, and the thread receiving it is
/// the one that knows. That is also why this is allocated where the transport is
/// opened rather than on the timer — the transport's life is the endpoint's, and
/// so is its counter's.
///
/// # Errors
///
/// `None` when the manager has no room; nothing is left behind.
pub fn allocate_transport_counter(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    local: SocketAddr,
    peer: Option<SocketAddr>,
    now_ms: i64,
) -> Option<i32> {
    let label = transport_label(local, peer);

    manager.allocate(
        regions,
        type_id::TRANSPORT,
        &transport_key(local, peer),
        label.as_bytes(),
        now_ms,
    )
}

/// Add the datagrams one pass received to a transport's counter.
///
/// The counter **is** the total, read back and written again rather than kept in
/// a second place that could drift — the same shape this media's own 79 is
/// counted in, and safe because the thread that reads the transport is the only
/// writer.
///
/// [`layout::NULL_COUNTER_ID`] is not a failure: it is a transport on the
/// kernel's media, which publishes no 76, and there is nothing to count into.
pub fn count_received_packets(
    manager: &CounterManager,
    regions: &CounterRegions<'_>,
    counter_id: i32,
    received: usize,
) {
    if counter_id == layout::NULL_COUNTER_ID {
        return;
    }

    let total = manager.value(regions, counter_id).unwrap_or(0);
    let received = i64::try_from(received).unwrap_or(i64::MAX);

    let _ = manager.set_value(regions, counter_id, total.saturating_add(received));
}

/// What a transport's counter is called: the address it is bound to, and who it
/// expects to hear from.
///
/// A transport that was not connected hears from anyone, and `*` says so rather
/// than naming a peer it does not have — a subscription is the case, and one
/// that never connects is the reference's own shape for it.
fn transport_label(local: SocketAddr, peer: Option<SocketAddr>) -> String {
    format!("dpdk transport: {local} -> {}", peer_or_any(peer))
}

/// The key, which is what tells two of these apart: the same two addresses as
/// text, so a reader that finds a counter by its key is comparing what it knows
/// against what the media bound.
fn transport_key(local: SocketAddr, peer: Option<SocketAddr>) -> Vec<u8> {
    format!("{local}->{}", peer_or_any(peer)).into_bytes()
}

fn peer_or_any(peer: Option<SocketAddr>) -> String {
    peer.map_or_else(|| "*".to_owned(), |peer| peer.to_string())
}

/// What the device answered, in this module's own words.
///
/// The crate's `PortStats` and `Link` in one value: what only a port can
/// answer, and what the counters above are filled from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Device {
    /// Whether the link is up — counter 75's value.
    pub up: bool,
    /// Frames the pool had none of.
    pub no_buffers: u64,
    /// Frames the port handed over, and frames it took for sending.
    pub received: u64,
    pub transmitted: u64,
    /// The bytes of those frames.
    pub received_bytes: u64,
    pub transmitted_bytes: u64,
    /// Frames the device dropped.
    pub missed: u64,
    /// Frames the pool has free.
    pub mempool_available: u32,
}

/// A DPDK media's counters, allocated and ready to be written.
///
/// Held by whoever owns the media and the timer that samples it. Nothing here
/// takes `&mut`, so the same value can be read by a timer on one thread and
/// written by another — which is what a counter is.
pub struct DpdkCounters {
    /// 75 — the link's state, one when it is up.
    link: i32,
    /// 77 — frames the pool had none of.
    no_buffers: i32,
    /// 82 — frames received, and frames sent.
    received_packets: i32,
    transmitted_packets: i32,
    /// 83 — the bytes of those frames.
    received_bytes: i32,
    transmitted_bytes: i32,
    /// 84 — frames the device dropped.
    missed: i32,
    /// 91 — frames the pool has free.
    mempool: i32,
    /// 87 — the passes a poller has made, one per thread role.
    poller: i32,
    /// 79 — reads of the device that failed.
    errors: i32,
    /// The rest: what the stack has already counted, published under the type
    /// the reference gives it.
    forwarded: Vec<Forwarded>,
}

/// One of the stack's own counters, and the counter it is published as.
struct Forwarded {
    counter_id: i32,
    /// Which of the stack's atomics this one follows. A function rather than a
    /// field offset because the table is a table: what varies between the rows
    /// is which counter and which atomic, and this is the pair written down
    /// once.
    source: fn(&StackCounters) -> &AtomicU64,
}

impl DpdkCounters {
    /// Allocate this port's counters and register them with the manager.
    ///
    /// `registration_id` is zero and the owner is nobody: these belong to the
    /// driver, and their type is what a reader finds them by.
    ///
    /// # Errors
    ///
    /// `None` when the manager has no room — the caller's `CounterManager`
    /// answers the same way for every allocation, and a driver that cannot
    /// publish its counters should say so where it allocated them.
    pub fn allocate(
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        identity: &PortIdentity<'_>,
        now_ms: i64,
    ) -> Option<Self> {
        let mut allocate = |what: &str, type_id: i32| -> Option<i32> {
            let counter_id = manager.allocate(
                regions,
                type_id,
                &PortIdentity::key(what),
                identity.label(what).as_bytes(),
                now_ms,
            )?;
            manager
                .set_registration_id(regions, counter_id, NO_CLIENT)
                .and_then(|()| manager.set_owner_id(regions, counter_id, layout::NULL_VALUE))?;

            Some(counter_id)
        };

        let link = allocate("link", type_id::PORT_INFO)?;
        let no_buffers = allocate("no-buffers", type_id::NOBUFS)?;
        let received_packets = allocate("packets-received", type_id::PKTS)?;
        let transmitted_packets = allocate("packets-sent", type_id::PKTS)?;
        let received_bytes = allocate("bytes-received", type_id::BYTES)?;
        let transmitted_bytes = allocate("bytes-sent", type_id::BYTES)?;
        let missed = allocate("missed-packets", type_id::MISSED_PACKETS)?;
        let mempool = allocate("mempool-available", type_id::MEMPOOL_AVAILABLE)?;
        // 79 is the port's own failures. Its writer is the timer that reads the
        // device: a read that fails is a failure of this media's port and of
        // nothing else, which is what makes one number enough for it.
        let errors = allocate("error", type_id::ERROR)?;

        // 87 is one per poller rather than one per port, and in this build
        // there is one poller per agent role.
        let poller = allocate("poller", type_id::POLLER)?;

        // The rest are the stack's own counts, under the reference's types.
        let mut forwarded = Vec::new();
        for (what, type_id, source) in [
            (
                "tx-eagain",
                type_id::TX_EAGAIN,
                (|counters: &StackCounters| &counters.tx_eagain)
                    as fn(&StackCounters) -> &AtomicU64,
            ),
            ("arp-miss", type_id::ARP_MISS, |c| &c.arp_miss),
            ("rx-sender-discard", type_id::RX_SENDER_DISCARD, |c| {
                &c.rx_sender_discard
            }),
            ("queue-drop", type_id::QUEUE_DROP_COUNT, |c| &c.queue_drop),
            ("checksum-failure", type_id::CHECKSUM_FAILURE, |c| {
                &c.checksum_failure
            }),
            ("fragmented", type_id::FRAGMENTED_PACKETS, |c| &c.fragmented),
            (
                "unsupported-ethernet-type",
                type_id::RX_UNSUPPORTED_ETHERNET_TYPE,
                |c| &c.unsupported_ethernet_type,
            ),
            (
                "unsupported-protocol",
                type_id::RX_UNSUPPORTED_PROTOCOL,
                |c| &c.unsupported_protocol,
            ),
            ("rx-receiver-discard", type_id::RX_RECEIVER_DISCARD, |c| {
                &c.rx_receiver_discard
            }),
        ] {
            let counter_id = allocate(what, type_id)?;
            forwarded.push(Forwarded { counter_id, source });
        }

        Some(Self {
            link,
            no_buffers,
            received_packets,
            transmitted_packets,
            received_bytes,
            transmitted_bytes,
            missed,
            mempool,
            poller,
            errors,
            forwarded,
        })
    }

    /// Publish what the stack has counted.
    ///
    /// Not a copy into a second counter that then has to be kept in step: the
    /// stack's atomics are the count, and this is where a reader outside the
    /// process can see them.
    ///
    /// # Errors
    ///
    /// `None` if any of the counters cannot be written, which means the CnC
    /// file is not the one these were allocated on.
    pub fn publish(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
        counters: &StackCounters,
    ) -> Option<()> {
        for forwarded in &self.forwarded {
            let value = (forwarded.source)(counters).load(Ordering::Relaxed);
            manager.set_value(regions, forwarded.counter_id, value as i64)?;
        }

        Some(())
    }

    /// Publish the passes a poller has made.
    ///
    /// Separate from [`publish`](Self::publish) because it is not the stack's
    /// number: 87 is one per poller and the count lives with the thread that
    /// made the passes, so whoever runs the timer passes it in.
    pub fn publish_poller(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
        passes: u64,
    ) -> Option<()> {
        manager.set_value(regions, self.poller, passes as i64)
    }

    /// Read what only the device and the pool know, and publish it.
    ///
    /// This is the timer's half, and the reason the design asks which PMD a
    /// statistics read may run against a busy queue
    /// (`rte_eth_stats_get` is documented as callable, but a PMD's own
    /// implementation is its own).
    ///
    /// **The read is the stack's**, which is what keeps this module free of the
    /// DPDK crate: a stack that drives a device knows how to read it and one
    /// that does not answers nothing, so the timer asks the same question of
    /// both and neither has to be told which it is.
    ///
    /// # Errors
    ///
    /// The device's own words, which the caller records as it records any other
    /// failure of the media — a device read that fails is what this build's 79
    /// is for.
    pub fn sample(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
        stack: &Stack,
    ) -> Result<(), String> {
        let Some(reading) = stack.device_reading() else {
            return Ok(());
        };

        let device = reading?;

        let _ = self.publish_device(manager, regions, &device);

        Ok(())
    }

    /// Count a read of the device that failed.
    ///
    /// The one thing this media's own 79 is for: a port that will not answer is
    /// a driver whose numbers stop moving, and a reader that can see the failure
    /// counted can tell that from an idle port.
    ///
    /// # Errors
    ///
    /// `None` if the counter cannot be read or written, which means the CnC
    /// file is not the one it was allocated on.
    pub fn record_error(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Option<()> {
        let value = manager.value(regions, self.errors).unwrap_or(0);
        manager.set_value(regions, self.errors, value.saturating_add(1))?;

        Some(())
    }

    /// Publish what the device answered.
    ///
    /// Separate from [`sample`](Self::sample) by one type, and that is the
    /// whole of why: the numbers are the part that can be wrong in a way nobody
    /// notices — one of six same-shaped counters filed under another's — so the
    /// mapping from numbers to counters is written and judged without a port,
    /// and the port is only needed by whoever reads it.
    ///
    /// # Errors
    ///
    /// `None` if any of the counters cannot be written.
    pub fn publish_device(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
        device: &Device,
    ) -> Option<()> {
        for (counter_id, value) in [
            (self.link, i64::from(device.up)),
            (self.no_buffers, device.no_buffers as i64),
            (self.received_packets, device.received as i64),
            (self.transmitted_packets, device.transmitted as i64),
            (self.received_bytes, device.received_bytes as i64),
            (self.transmitted_bytes, device.transmitted_bytes as i64),
            (self.missed, device.missed as i64),
            (self.mempool, i64::from(device.mempool_available)),
        ] {
            manager.set_value(regions, counter_id, value)?;
        }

        Some(())
    }
}

impl std::fmt::Debug for DpdkCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkCounters")
            .field("port_info", &self.link)
            .field("missed", &self.missed)
            .field("mempool", &self.mempool)
            .field("errors", &self.errors)
            .field("forwarded", &self.forwarded.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_cnc::CounterDescriptor;

    const VALUES_LENGTH: usize = 1024 * 1024;

    use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned region")
        }
    }

    /// A CnC-sized pair of counter regions, for a case that wants to allocate
    /// into one without a driver.
    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                metadata: Region::zeroed(VALUES_LENGTH * 4),
                values: Region::zeroed(VALUES_LENGTH),
            }
        }

        fn open(&mut self) -> (CounterManager, CounterRegions<'_>) {
            let regions = CounterRegions::new(self.metadata.writable(), self.values.writable())
                .expect("four-to-one");
            let manager = CounterManager::new(VALUES_LENGTH, 1_000).expect("room for counters");
            (manager, regions)
        }
    }

    fn identity() -> PortIdentity<'static> {
        PortIdentity {
            name: "0000:03:00.1",
            mac: [0x02, 0, 0, 0, 0, 0x01],
            address: Ipv4Addr::new(10, 10, 0, 1),
            prefix_length: 24,
            driver: "net_ixgbe",
        }
    }

    /// Every counter this media publishes, as a client reads it back.
    fn published(regions: &CounterRegions<'_>, type_id: i32) -> Vec<CounterDescriptor> {
        let mut found = Vec::new();
        regions.reader().for_each(|descriptor| {
            if descriptor.type_id == type_id {
                found.push(descriptor.clone());
            }
        });
        found
    }

    /// The export path: what a media allocates is what a client reads.
    ///
    /// This is the half of the design's §4.10 that has to work before the
    /// values mean anything, and it needs no DPDK — a CnC file, the manager
    /// that owns it, and the reader clients use.
    #[test]
    fn a_counter_a_media_allocates_is_a_counter_a_client_reads() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        DpdkCounters::allocate(&mut manager, &regions, &identity(), 1).expect("room");

        // 75, one per port, and the label is the design's own shape.
        let port_info = published(&regions, type_id::PORT_INFO);
        assert_eq!(1, port_info.len(), "one port-info counter");
        assert_eq!(
            "dpdk port: 0000:03:00.1 mac=02:00:00:00:00:01 ip=10.10.0.1/24 driver=net_ixgbe link",
            port_info[0].label
        );
        assert_eq!(
            0, port_info[0].registration_id,
            "the media's counters belong to the driver, not to a client"
        );
        assert_eq!(layout::NULL_VALUE, port_info[0].owner_id);

        // Two of 82 and two of 83, one each way, told apart by their labels.
        let packets: Vec<String> = published(&regions, type_id::PKTS)
            .into_iter()
            .map(|descriptor| descriptor.label)
            .collect();
        assert_eq!(2, packets.len(), "{packets:?}");
        assert!(packets[0].ends_with("packets-received"), "{packets:?}");
        assert!(packets[1].ends_with("packets-sent"), "{packets:?}");

        assert_eq!(2, published(&regions, type_id::BYTES).len());
        assert_eq!(1, published(&regions, type_id::MISSED_PACKETS).len());
        assert_eq!(1, published(&regions, type_id::MEMPOOL_AVAILABLE).len());
        assert_eq!(1, published(&regions, type_id::NOBUFS).len());
        assert_eq!(1, published(&regions, type_id::TX_EAGAIN).len());
        assert_eq!(1, published(&regions, type_id::ERROR).len());
        assert_eq!(1, published(&regions, type_id::POLLER).len());
    }

    /// The nine the stack already counts are published under the reference's
    /// types, and the value is the stack's own number rather than a copy that
    /// could drift.
    #[test]
    fn the_stacks_counters_are_published_under_their_own_types() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let counters =
            DpdkCounters::allocate(&mut manager, &regions, &identity(), 1).expect("room");

        let stack = StackCounters::default();
        counters
            .publish(&manager, &regions, &stack)
            .expect("a writable file");
        assert!(
            published(&regions, type_id::QUEUE_DROP_COUNT)
                .iter()
                .all(|descriptor| descriptor.value == 0),
            "nothing has been dropped yet"
        );

        // The stack counts; the counter follows — the same atomic, read.
        stack.queue_drop.fetch_add(7, Ordering::Relaxed);
        stack.unsupported_protocol.fetch_add(2, Ordering::Relaxed);
        counters
            .publish(&manager, &regions, &stack)
            .expect("a writable file");

        assert_eq!(7, published(&regions, type_id::QUEUE_DROP_COUNT)[0].value);
        assert_eq!(
            2,
            published(&regions, type_id::RX_UNSUPPORTED_PROTOCOL)[0].value
        );

        counters
            .publish_poller(&manager, &regions, 41)
            .expect("a writable file");
        assert_eq!(41, published(&regions, type_id::POLLER)[0].value);
    }

    /// What the device says is filed under the counter it belongs to.
    ///
    /// The device's four packet-and-byte counts and its two drop counts are
    /// six numbers of the same shape, so putting one under another's counter is
    /// a mistake that produces a plausible file. Each is given a different
    /// value here, and each counter's is asserted to be its own.
    #[test]
    fn the_devices_numbers_are_filed_under_their_own_counters() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        let media = DpdkCounters::allocate(&mut manager, &regions, &identity(), 1).expect("room");

        media
            .publish_device(
                &manager,
                &regions,
                &Device {
                    up: true,
                    no_buffers: 11,
                    received: 22,
                    transmitted: 33,
                    received_bytes: 44,
                    transmitted_bytes: 55,
                    missed: 66,
                    mempool_available: 777,
                },
            )
            .expect("a writable file");

        let read = |type_id: i32, label_ends: &str| -> i64 {
            published(&regions, type_id)
                .into_iter()
                .find(|descriptor| descriptor.label.ends_with(label_ends))
                .unwrap_or_else(|| panic!("{type_id} {label_ends} is allocated"))
                .value
        };

        assert_eq!(1, read(type_id::PORT_INFO, "link"));
        assert_eq!(11, read(type_id::NOBUFS, "no-buffers"));
        assert_eq!(22, read(type_id::PKTS, "packets-received"));
        assert_eq!(33, read(type_id::PKTS, "packets-sent"));
        assert_eq!(44, read(type_id::BYTES, "bytes-received"));
        assert_eq!(55, read(type_id::BYTES, "bytes-sent"));
        assert_eq!(66, read(type_id::MISSED_PACKETS, "missed-packets"));
        assert_eq!(777, read(type_id::MEMPOOL_AVAILABLE, "mempool-available"));
    }

    /// 92 is not allocated: the design defers the per-xstat set to D6, and a
    /// counter per hardware register is not a set to publish by accident.
    #[test]
    fn the_deferred_type_is_not_allocated() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        DpdkCounters::allocate(&mut manager, &regions, &identity(), 1).expect("room");

        assert!(published(&regions, type_id::EXTENDED_STATS).is_empty());
        assert!(
            published(&regions, 80).is_empty() && published(&regions, 81).is_empty(),
            "80 and 81 are unassigned in the reference's catalogue"
        );
    }

    /// A transport's counter says which transport it is, and counts into
    /// itself.
    ///
    /// 76 is the one counter here that belongs to a *transport* rather than to
    /// the port, and it is the one whose value the media does not write: the
    /// thread reading the socket adds to it ([`count_received_packets`]), one
    /// pass at a time. Both halves are here because both are what a reader
    /// outside the process sees.
    #[test]
    fn a_transports_counter_names_it_and_counts_into_itself() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        let local = SocketAddr::from((Ipv4Addr::new(10, 10, 0, 1), 40456));
        let peer = SocketAddr::from((Ipv4Addr::new(10, 10, 0, 2), 40500));

        let connected =
            allocate_transport_counter(&mut manager, &regions, local, Some(peer), 1).expect("room");
        let listening =
            allocate_transport_counter(&mut manager, &regions, local, None, 1).expect("room");

        let counters = published(&regions, type_id::TRANSPORT);
        assert_eq!(2, counters.len(), "one per transport");
        assert_eq!(
            "dpdk transport: 10.10.0.1:40456 -> 10.10.0.2:40500",
            counters[0].label
        );
        assert_eq!(
            "dpdk transport: 10.10.0.1:40456 -> *", counters[1].label,
            "a transport that was not connected hears from anyone, and says so"
        );
        assert_eq!(0, manager.value(&regions, connected).expect("a value"));
        count_received_packets(&manager, &regions, connected, 3);
        count_received_packets(&manager, &regions, connected, 4);
        assert_eq!(
            7,
            manager.value(&regions, connected).expect("a value"),
            "the counter is the total, not the last pass"
        );
        assert_eq!(
            0,
            manager.value(&regions, listening).expect("a value"),
            "and one transport's traffic is not another's"
        );

        // The kernel's media publishes none, and says so with the null id
        // rather than with a counter nobody would read.
        count_received_packets(&manager, &regions, layout::NULL_COUNTER_ID, 5);
        assert_eq!(2, published(&regions, type_id::TRANSPORT).len());
    }
}
