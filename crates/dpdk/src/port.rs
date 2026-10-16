//! The port, its queues, and the frames coming off them.

use std::ffi::{CStr, CString, c_char, c_uint, c_void};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::Error;
use crate::mempool::{Mempool, ParkedFrame};
use crate::sys::{self, SHIM_BURST_MAX, SHIM_CKSUM_BAD, SHIM_CKSUM_GOOD};

/// How a port is configured before it is started.
#[derive(Clone, Copy, Debug)]
pub struct PortSettings {
    /// Receive queues to set up. The driver's own mapping of queues to threads
    /// decides how many, and this crate only hands them out.
    pub rx_queues: u16,
    /// Transmit queues to set up.
    pub tx_queues: u16,
    /// Descriptors per receive queue.
    pub rx_descriptors: u16,
    /// Descriptors per transmit queue.
    pub tx_descriptors: u16,
    /// Receive offloads to ask for — `RTE_ETH_RX_OFFLOAD_*` bits, which this
    /// build only ever asks for on a hardware port (D0: the virtual devices
    /// have none beyond scatter, so the software path is the one CI exercises).
    pub rx_offloads: u64,
    /// Transmit offloads to ask for.
    pub tx_offloads: u64,
}

impl Default for PortSettings {
    fn default() -> Self {
        Self {
            rx_queues: 1,
            tx_queues: 1,
            rx_descriptors: 1024,
            tx_descriptors: 1024,
            rx_offloads: 0,
            tx_offloads: 0,
        }
    }
}

impl PortSettings {
    /// Ask `port` for the checksum offloads this build reads back — the ones
    /// [`PortInfo::can_judge_ipv4_checksum`] and
    /// [`PortInfo::can_judge_udp_checksum`] answer about — and leave every
    /// other setting as it is.
    ///
    /// What a port *can* do and what it *does* are the same thing only because
    /// a caller asks: `rte_eth_dev_configure` refuses a bit the device does not
    /// have rather than ignoring it, so the bits come from the port's own
    /// capabilities and nothing else is asked for. A port with neither — every
    /// virtual device this build's tests run on — is asked for nothing, and its
    /// settings come back unchanged.
    #[must_use = "the settings it builds are the ones to start the port with"]
    pub fn asking_for_checksum_offloads(mut self, port: &PortInfo) -> Self {
        let (ipv4, udp) = checksum_offload_bits();

        if port.can_judge_ipv4_checksum() {
            self.rx_offloads |= ipv4;
        }
        if port.can_judge_udp_checksum() {
            self.rx_offloads |= udp;
        }

        // **And the same question about the transmit side**, which is where the
        // two directions stop being one decision: a frame this stack hands over
        // carries checksums somebody wrote, and a port that can compute them is
        // a port this stack may stop writing the ones it has no reason to
        // compute. Only what the port says it can do is asked for — the same
        // rule as the receive bits above, and `rte_eth_dev_configure` refuses a
        // bit the device has not got.
        let (ipv4, udp) = checksum_offload_bits_tx();

        if port.can_compute_ipv4_checksum() {
            self.tx_offloads |= ipv4;
        }
        if port.can_compute_udp_checksum() {
            self.tx_offloads |= udp;
        }

        self
    }
}

/// The two receive offloads this build asks about, as the bits DPDK numbers
/// them by.
fn checksum_offload_bits() -> (u64, u64) {
    // SAFETY: header constants, wrapped as functions by the shim (`shim.c`);
    // they read nothing and cannot fail.
    unsafe {
        (
            sys::shim_rx_offload_ipv4_checksum(),
            sys::shim_rx_offload_udp_checksum(),
        )
    }
}

/// What a port says it is, read once when it is started.
#[derive(Clone, Debug)]
pub struct PortInfo {
    /// The name EAL knows it by: a PCI address, or a virtual device's name.
    pub name: String,
    /// The PMD's name.
    pub driver: String,
    pub max_rx_queues: u16,
    pub max_tx_queues: u16,
    pub min_mtu: u16,
    pub max_mtu: u16,
    /// The socket the device is on, `None` where it reports `-1` — which is
    /// every port this build has measured, virtual and real alike (D0).
    pub socket_id: Option<u32>,
    pub rx_offloads: u64,
    pub tx_offloads: u64,
}

impl PortInfo {
    /// Whether this port judges the IPv4 header checksum of the frames it
    /// hands over — `RTE_ETH_RX_OFFLOAD_IPV4_CKSUM` among its receive
    /// capabilities.
    ///
    /// A capability, not a promise: the port judges checksums only if it was
    /// asked to ([`PortSettings::rx_offloads`]), and what it answers here is
    /// what it *can* do. The two are the same value for a caller that asks for
    /// what it reads back, which is what the driver does.
    pub fn can_judge_ipv4_checksum(&self) -> bool {
        let (ipv4, _) = checksum_offload_bits();
        self.rx_offloads & ipv4 != 0
    }

    /// Whether it judges the UDP checksum; see
    /// [`can_judge_ipv4_checksum`](Self::can_judge_ipv4_checksum).
    pub fn can_judge_udp_checksum(&self) -> bool {
        let (_, udp) = checksum_offload_bits();
        self.rx_offloads & udp != 0
    }

    /// Whether this port can **compute** an IPv4 header's checksum for the
    /// frames it sends — the transmit side's twin of
    /// [`can_judge_ipv4_checksum`](Self::can_judge_ipv4_checksum), and a
    /// different capability: a port that computes on the way out and one that
    /// judges on the way in are two bits, and a device may have either, both
    /// or neither.
    ///
    /// **A capability, not a promise**, for the same reason as the two above:
    /// the hardware computes for the frames it sends only if it was asked to
    /// ([`PortSettings::tx_offloads`]), and a caller that asks for what it read
    /// back is the one that can take this answer at face value.
    pub fn can_compute_ipv4_checksum(&self) -> bool {
        let (ipv4, _) = checksum_offload_bits_tx();
        self.tx_offloads & ipv4 != 0
    }

    /// Whether it computes the UDP checksum; see
    /// [`can_compute_ipv4_checksum`](Self::can_compute_ipv4_checksum).
    pub fn can_compute_udp_checksum(&self) -> bool {
        let (_, udp) = checksum_offload_bits_tx();
        self.tx_offloads & udp != 0
    }

    /// Whether asking this port for everything it can do about checksums would
    /// get both directions.
    pub fn can_compute_both_checksums(&self) -> bool {
        self.can_compute_ipv4_checksum() && self.can_compute_udp_checksum()
    }
}

/// The two transmit offloads this build asks about, as DPDK numbers them.
fn checksum_offload_bits_tx() -> (u64, u64) {
    // SAFETY: as in `checksum_offload_bits`.
    unsafe {
        (
            sys::shim_tx_offload_ipv4_checksum(),
            sys::shim_tx_offload_udp_checksum(),
        )
    }
}

/// **What one frame asks the NIC to compute**, which is the transmit side's
/// answer to [`PortInfo::can_judge_ipv4_checksum`]'s question.
///
/// A frame whose checksums a NIC is to compute is not the frame a stack writes
/// on its own: DPDK's contract is that the L4 checksum field holds the
/// **pseudo-header** sum and that the mbuf carries the two header lengths
/// (`rte_mbuf_core.h:283-296`, `rte_net.h:89-96`). Both of those are the
/// caller's to get right, and this type is how it says which ones it did —
/// so it is built by the code that wrote the bytes, never passed in beside
/// them.
///
/// [`Self::NONE`] is the whole of what a port with no such offload ever sees,
/// and what every frame that carries no IPv4/UDP header asks for (an ARP
/// request, an ICMP reply): a frame's flags are **its own**, which is why this
/// travels with the frame and not with the burst
/// (`rte_eth_tx_burst`'s mbufs each carry their own `ol_flags`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct TxOffloads {
    /// The bits the mbuf carries, or zero for "the NIC computes nothing".
    flags: u64,
    /// The Ethernet header's length, read only when `flags` is not zero.
    l2_len: u16,
    /// The IPv4 header's length, likewise.
    l3_len: u16,
}

impl TxOffloads {
    /// A frame the NIC computes nothing for: every checksum in it was written
    /// by the sender, and is the one the receiver will find.
    pub const NONE: Self = Self {
        flags: 0,
        l2_len: 0,
        l3_len: 0,
    };

    /// A frame whose IPv4 header checksum and whose UDP checksum the NIC is to
    /// compute, over headers `l2_len` and `l3_len` bytes long.
    ///
    /// The caller has written the **pseudo-header** sum into the UDP checksum
    /// field, which is what the hardware adds the rest of the datagram to.
    #[must_use]
    pub fn ipv4_and_udp_checksums(l2_len: u16, l3_len: u16) -> Self {
        // SAFETY: header constants, wrapped as functions by the shim; they read
        // nothing and cannot fail.
        let flags = unsafe {
            sys::shim_mbuf_tx_ipv4()
                | sys::shim_mbuf_tx_ip_checksum()
                | sys::shim_mbuf_tx_udp_checksum()
        };

        Self {
            flags,
            l2_len,
            l3_len,
        }
    }

    /// What this frame asks the NIC for, as the shim's slot holds it.
    const fn parts(self) -> (u64, u16, u16) {
        (self.flags, self.l2_len, self.l3_len)
    }
}

/// What a port's own counters say (`rte_eth_stats`).
///
/// The device's account rather than this build's: a frame the NIC dropped for
/// want of room is in `missed` and nowhere else, which is why a driver that
/// wants to know what it lost must read here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PortStats {
    /// Frames the port handed to this process.
    pub received: u64,
    /// Frames the port took for transmission.
    pub transmitted: u64,
    /// Bytes of those frames.
    pub received_bytes: u64,
    /// Bytes of those frames.
    pub transmitted_bytes: u64,
    /// Frames the device dropped because it had nowhere to put them — the one
    /// loss a driver cannot count for itself.
    pub missed: u64,
    /// Receive errors the PMD counted.
    pub receive_errors: u64,
    /// Transmit errors the PMD counted.
    pub transmit_errors: u64,
    /// Frames dropped because the pool had no buffer for them.
    pub no_buffers: u64,
}

/// What a port's link is doing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Link {
    /// Whether the link is up.
    pub up: bool,
    /// Whether it is full duplex.
    pub full_duplex: bool,
    /// Its speed, in megabits per second — zero when it is down, and the
    /// unknown-speed sentinel above that.
    pub speed_mbps: u32,
}

/// A started port.
///
/// Held in an [`Arc`]: the queues a thread was handed keep it alive, and the
/// last of them dropping is what stops and closes the device — so a port cannot
/// be closed while a thread is still receiving on it.
pub struct Port {
    id: u16,
    info: PortInfo,
}

impl Port {
    /// The names of every port EAL has.
    pub fn names() -> Vec<String> {
        // SAFETY: a plain count of the devices EAL probed.
        let count = unsafe { sys::shim_port_count() };
        (0..count).map(|id| name_of(id as u16)).collect()
    }

    /// What this port is.
    pub fn info(&self) -> &PortInfo {
        &self.info
    }

    /// What the port named `name` is, before anything is configured on it.
    ///
    /// A caller has to be able to read a port before it starts it, because what
    /// a port can do decides what it is asked for: a capability the device does
    /// not have makes [`Port::start`] fail rather than be ignored
    /// ([`PortSettings::asking_for_checksum_offloads`]), and a queue count past
    /// the device's maximum is a configuration it will refuse.
    pub fn describe(name: &str) -> Result<PortInfo, Error> {
        info_of(id_of(name)?)
    }

    /// This port's hardware address.
    pub fn mac(&self) -> [u8; 6] {
        let mut mac = [0u8; 6];
        // SAFETY: the buffer is six bytes, which is what the call writes.
        let rc = unsafe { sys::shim_port_mac(c_uint::from(self.id), mac.as_mut_ptr()) };
        debug_assert_eq!(rc, 0, "a started port has a MAC address");
        mac
    }

    /// The port's MTU.
    pub fn mtu(&self) -> u16 {
        let mut mtu = 0u16;
        // SAFETY: the buffer is a `u16`, which is what the call writes.
        let rc = unsafe { sys::shim_port_mtu(c_uint::from(self.id), &mut mtu) };
        debug_assert_eq!(rc, 0, "a started port reports its MTU");
        mtu
    }

    /// What the port's own counters say (`rte_eth_stats`).
    ///
    /// The call is a plain read of the PMD's counters and is documented as
    /// callable while the queues are in use; whether a given PMD's
    /// implementation really is, D3 checks per PMD (the design's §4.10). What
    /// this build needs from it is `missed` — the frames the *device* dropped,
    /// which is the only loss a driver can see from its own side of the wire.
    ///
    /// # Errors
    ///
    /// The error from the PMD, with DPDK's own words when it set an errno.
    pub fn stats(&self) -> Result<PortStats, Error> {
        let mut raw = sys::ShimPortStats::default();

        // SAFETY: the struct is this crate's own layout, mirrored field for
        // field in the shim, and the port id is one this value was started
        // with.
        let rc = unsafe { sys::shim_port_stats(c_uint::from(self.id), &mut raw) };
        if rc != 0 {
            return Err(Error::from_call(
                "the port would not report its counters",
                rc,
            ));
        }

        Ok(PortStats {
            received: raw.ipackets,
            transmitted: raw.opackets,
            received_bytes: raw.ibytes,
            transmitted_bytes: raw.obytes,
            missed: raw.imissed,
            receive_errors: raw.ierrors,
            transmit_errors: raw.oerrors,
            no_buffers: raw.rx_nombuf,
        })
    }

    /// The names of this port's extended statistics — the numbers the PMD
    /// exposes by hardware register name, over and above `rte_eth_stats`.
    ///
    /// **Once per port, not once per read.** A PMD's set of statistics is a
    /// property of the driver, so the names are the same every time; what
    /// changes is the values ([`extended_stats`](Self::extended_stats)), which
    /// come back in this order and are read against it.
    ///
    /// A device can answer none — and that is not a failure, which is worth
    /// saying because the two are easy to conflate: `-ENOTSUP` is DPDK's way of
    /// saying the PMD does not do extended statistics, and it becomes an empty
    /// list here. The virtual devices this build tests on do **not** take that
    /// path: `net_pcap` and `net_null` each answer thirteen of them (measured
    /// through `show port xstats`, and read back in the gate by
    /// `crates/driver/tests/dpdk_port.rs`), because DPDK hands a PMD that
    /// implements none of its own the basic set.
    ///
    /// # Errors
    ///
    /// The error from the PMD, with DPDK's own words when it set an errno.
    pub fn extended_stat_names(&self) -> Result<Vec<String>, Error> {
        let port = c_uint::from(self.id);

        // SAFETY: the port id is one this value was started with, and a null
        // array with a count of zero is the call's own way of asking for the
        // number of names.
        let count = unsafe { sys::shim_port_xstats_count(port) };
        if count < 0 {
            return Err(Error::from_call(
                "the port would not report its extended statistics",
                count,
            ));
        }
        let count = count.unsigned_abs();

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut raw = vec![
            sys::ShimXstatName {
                name: [0; sys::SHIM_XSTAT_NAME_MAX],
            };
            count as usize
        ];

        // SAFETY: the array is `count` elements of the struct DPDK fills, and
        // the shim asserts at compile time that its length is DPDK's own.
        let written = unsafe { sys::shim_port_xstats_names(port, raw.as_mut_ptr(), count) };
        if written != count as i32 {
            return Err(Error::from_call(
                "the port would not report the names of its extended statistics",
                written,
            ));
        }

        Ok(raw
            .into_iter()
            .map(|name| {
                // SAFETY: the shim writes a NUL-terminated name into every
                // element it filled, and every element was filled.
                unsafe { CStr::from_ptr(name.name.as_ptr()) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect())
    }

    /// Read this port's extended statistics into `out`, in the order
    /// [`extended_stat_names`](Self::extended_stat_names) gave them.
    ///
    /// **Into the caller's array**, because this is read on a timer: nothing
    /// here allocates, and a buffer the caller keeps is a buffer that is not
    /// built again every second. One call into the PMD, which is where the
    /// registers are actually read — on the X540 that is the expensive half of
    /// a media's counter work, which is why it is not on a data path.
    ///
    /// # Errors
    ///
    /// The error from the PMD. A port that answers fewer values than `out`
    /// holds is one whose PMD changed under a caller that read the names
    /// earlier, and it is an error rather than a partial fill.
    pub fn extended_stats(&self, out: &mut [u64]) -> Result<(), Error> {
        let n = u32::try_from(out.len()).unwrap_or(u32::MAX);

        // SAFETY: the array is the caller's, it is `n` elements long by
        // construction, and the port id is one this value was started with.
        let written =
            unsafe { sys::shim_port_xstats_values(c_uint::from(self.id), out.as_mut_ptr(), n) };
        if written != n as i32 {
            return Err(Error::from_call(
                "the port would not report its extended statistics",
                written,
            ));
        }

        Ok(())
    }

    /// What the link is doing, without waiting for it.
    ///
    /// The non-blocking form on purpose: a link that is still auto-negotiating
    /// makes the other one wait, and this is read on a timer.
    ///
    /// # Errors
    ///
    /// The error from the PMD.
    pub fn link(&self) -> Result<Link, Error> {
        let mut raw = sys::ShimPortLink::default();

        // SAFETY: as in `stats` above.
        let rc = unsafe { sys::shim_port_link(c_uint::from(self.id), &mut raw) };
        if rc != 0 {
            return Err(Error::from_call("the port would not report its link", rc));
        }

        Ok(Link {
            up: raw.up != 0,
            full_duplex: raw.full_duplex != 0,
            speed_mbps: raw.speed_mbps,
        })
    }

    /// Configure and start the port with this name, and hand out its queues.
    ///
    /// One queue pair per index, up to `settings.rx_queues` (and as many
    /// transmit queues): which thread gets which pair is the driver's mapping,
    /// not this crate's. A failure after the port was configured closes it
    /// again, so a caller that gives up does not leave a half-configured device
    /// behind.
    pub fn start(
        name: &str,
        settings: &PortSettings,
        pool: &Mempool,
    ) -> Result<(Arc<Self>, Vec<QueuePair>), Error> {
        let id = id_of(name)?;
        let info = info_of(id)?;

        let configured = (|| -> Result<(), Error> {
            // SAFETY: every call below takes this port's id, which came from
            // `rte_eth_dev_get_port_by_name`, and numbers that came from the
            // caller's settings. DPDK's order is configure, then the queues,
            // then start.
            unsafe {
                let rc = sys::shim_port_configure(
                    c_uint::from(id),
                    c_uint::from(settings.rx_queues),
                    c_uint::from(settings.tx_queues),
                    settings.rx_offloads,
                    settings.tx_offloads,
                );
                if rc != 0 {
                    return Err(Error::from_call("the port refused that configuration", rc));
                }

                for queue in 0..settings.rx_queues {
                    let rc = sys::shim_rx_queue_setup(
                        c_uint::from(id),
                        c_uint::from(queue),
                        c_uint::from(settings.rx_descriptors),
                        pool.handle().as_ptr(),
                    );
                    if rc != 0 {
                        return Err(Error::from_call("a receive queue was refused", rc));
                    }
                }

                for queue in 0..settings.tx_queues {
                    let rc = sys::shim_tx_queue_setup(
                        c_uint::from(id),
                        c_uint::from(queue),
                        c_uint::from(settings.tx_descriptors),
                    );
                    if rc != 0 {
                        return Err(Error::from_call("a transmit queue was refused", rc));
                    }
                }

                let rc = sys::shim_port_start(c_uint::from(id));
                if rc != 0 {
                    return Err(Error::from_call("the port would not start", rc));
                }
            }
            Ok(())
        })();

        if let Err(error) = configured {
            // SAFETY: the port was configured and is not started, which is
            // exactly what `rte_eth_dev_close` is documented to accept.
            unsafe { sys::shim_port_close(c_uint::from(id)) };
            return Err(error);
        }

        let port = Arc::new(Self { id, info });
        let pairs = (0..settings.rx_queues)
            .map(|queue| QueuePair {
                port: Arc::clone(&port),
                rx_queue: queue,
                tx_queue: queue,
                _one_thread: PhantomData,
            })
            .collect();

        Ok((port, pairs))
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        // SAFETY: no queue can be alive — each holds an `Arc` of this port —
        // so no thread is receiving or transmitting on it, which is what
        // stopping requires. Closing releases the device.
        unsafe {
            sys::shim_port_stop(c_uint::from(self.id));
            sys::shim_port_close(c_uint::from(self.id));
        }
    }
}

impl std::fmt::Debug for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Port")
            .field("name", &self.info.name)
            .field("driver", &self.info.driver)
            .finish()
    }
}

/// One receive queue and one transmit queue, and the thread they belong to.
///
/// `Send` and neither `Sync` nor `Clone`, which is the invariant written as a
/// type: `rte_eth_rx_burst` on one queue from two threads at once is not a
/// thing DPDK offers, and a queue pair crossing to the thread that will use it
/// is exactly what the driver does with it.
pub struct QueuePair {
    port: Arc<Port>,
    rx_queue: u16,
    tx_queue: u16,
    _one_thread: PhantomData<*mut ()>,
}

// SAFETY: the pair is moved to the thread that owns it and never shared;
// nothing in it is bound to the thread that created it (the port's id is a
// number and DPDK's queues are not thread-affine, only single-threaded).
unsafe impl Send for QueuePair {}

impl QueuePair {
    /// The port this pair belongs to.
    pub fn port(&self) -> &Arc<Port> {
        &self.port
    }

    /// Take up to `max` frames off the receive queue, handing each to `f`.
    ///
    /// A frame's bytes are borrowed for the length of the call; `f` answers
    /// [`Disposition::Keep`] to keep the frame — it is then pushed onto `kept`
    /// as a [`ParkedFrame`] — and [`Disposition::Release`] to give it straight
    /// back to the pool. Everything released goes back in **one** call, so a
    /// pass pays a constant number of crossings whatever the burst carries.
    ///
    /// `pool` is the pool the queue was set up with: the frames it hands out
    /// borrow it, so they cannot outlive it. A callback that panics leaks the
    /// frames the burst had not reached yet — a panic on this path is already
    /// the end of the process's media, and the alternative is catching it.
    pub fn rx_burst<'pool>(
        &mut self,
        pool: &'pool Mempool,
        max: usize,
        f: &mut dyn FnMut(RxFrame<'_>) -> Disposition,
        kept: &mut Vec<ParkedFrame<'pool>>,
    ) -> usize {
        let _ = pool;
        let max = max.min(SHIM_BURST_MAX);
        if max == 0 {
            return 0;
        }

        let mut descs = [sys::ShimRxDesc {
            mbuf: std::ptr::null_mut(),
            data: std::ptr::null_mut(),
            len: 0,
            checksum: sys::SHIM_CKSUM_NONE,
        }; SHIM_BURST_MAX];

        // SAFETY: the descriptor array holds `SHIM_BURST_MAX` writable
        // descriptors and `max` is at most that; the queue index is one this
        // pair was set up with, and the pair's `Send + !Sync` means no other
        // thread is in this queue.
        let count = unsafe {
            sys::shim_rx_burst(
                c_uint::from(self.port.id),
                c_uint::from(self.rx_queue),
                descs.as_mut_ptr(),
                max as c_uint,
            )
        } as usize;

        let mut released = [std::ptr::null_mut::<c_void>(); SHIM_BURST_MAX];
        let mut released_count = 0usize;

        for desc in &descs[..count] {
            // SAFETY: the shim filled this descriptor from a frame the NIC
            // handed over: `data` runs for `len` bytes, and this code is the
            // only owner until either branch below takes it.
            let bytes = unsafe { std::slice::from_raw_parts(desc.data, usize::from(desc.len)) };
            let frame = RxFrame {
                bytes,
                checksum: Checksum::of(desc.checksum),
            };

            match f(frame) {
                Disposition::Keep => {
                    let handle = NonNull::new(desc.mbuf).expect("a received frame has a handle");
                    kept.push(ParkedFrame::own(handle, desc.data, desc.len));
                }
                Disposition::Release => {
                    released[released_count] = desc.mbuf;
                    released_count += 1;
                }
            }
        }

        if released_count > 0 {
            // SAFETY: the first `released_count` entries are live mbufs this
            // code owns and is giving back exactly once.
            unsafe { sys::shim_mbuf_free_bulk(released.as_mut_ptr(), released_count as c_uint) };
        }

        count
    }
}

impl QueuePair {
    /// Allocate up to `count` frames, let `fill` write each, and hand them to
    /// the NIC. Answers how many left.
    ///
    /// `fill` is given the frame's room — headers and payload both, because
    /// this is the whole frame — and answers **how many bytes it wrote and what
    /// the NIC is to compute for them** ([`TxOffloads`]); more than the room is
    /// refused by an assertion rather than truncated silently. The frames the
    /// NIC did not take are freed **inside the burst**, so the number this
    /// answers is exactly what left the machine, which is what a sender's
    /// position advances by. A `fill` that panics leaks the frames the burst
    /// had not reached; as on the receive side, a panic here is already the
    /// end of the process's media.
    ///
    /// **The offload is per frame, and it comes back from `fill` rather than
    /// going in beside it** — one call's frames are not one kind of frame. The
    /// port this queue belongs to carries ARP requests and ICMP replies as well
    /// as UDP datagrams, and a port asked to compute a UDP checksum for an ARP
    /// frame would write one into the middle of it: the sender would see a
    /// burst that left, and the peer would see a frame that resolves nothing.
    /// DPDK's own shape is the same one (`rte_eth_tx_burst`'s mbufs each carry
    /// their own `ol_flags`), and it is why the answer comes from the code that
    /// wrote the bytes: the flags for a frame can only be produced by a
    /// constructor that knows what the frame is.
    ///
    /// Zero frames come back when the pool has none: that is back pressure, and
    /// the caller tries again next pass.
    pub fn tx_burst(
        &mut self,
        pool: &Mempool,
        count: usize,
        fill: &mut dyn FnMut(&mut [u8]) -> (usize, TxOffloads),
    ) -> usize {
        let count = count.min(SHIM_BURST_MAX);
        if count == 0 {
            return 0;
        }

        let mut slots = [sys::ShimTxSlot::EMPTY; SHIM_BURST_MAX];
        // SAFETY: the slot array holds `SHIM_BURST_MAX` writable slots and
        // `count` is at most that; the pool outlives the frames it hands over,
        // which is what the borrow says.
        let allocated = unsafe {
            sys::shim_tx_alloc(pool.handle().as_ptr(), slots.as_mut_ptr(), count as c_uint)
        } as usize;
        if allocated == 0 {
            return 0;
        }

        for slot in &mut slots[..allocated] {
            // SAFETY: the shim allocated this frame and wrote down the bytes
            // this side may fill; the borrow of the room ends before the frame
            // is handed over, so nothing is aliased when it goes.
            let (written, offloads) = unsafe {
                let room = std::slice::from_raw_parts_mut(slot.data, usize::from(slot.room));
                let (written, offloads) = fill(room);
                assert!(
                    written <= room.len(),
                    "a frame cannot be longer than the room it was given: {written} > {}",
                    room.len()
                );
                (written, offloads)
            };
            slot.len = written as u16;
            let (flags, l2_len, l3_len) = offloads.parts();
            slot.ol_flags = flags;
            slot.l2_len = l2_len;
            slot.l3_len = l3_len;
        }

        // SAFETY: the first `allocated` slots hold live frames this code owns,
        // with the lengths it just wrote; the queue index is one this pair was
        // set up with, and `Send + !Sync` means no other thread is in it.
        let sent = unsafe {
            sys::shim_tx_burst(
                c_uint::from(self.port.id),
                c_uint::from(self.tx_queue),
                slots.as_mut_ptr(),
                allocated as c_uint,
            )
        };

        sent as usize
    }
}

impl std::fmt::Debug for QueuePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePair")
            .field("port", &self.port.info.name)
            .field("rx_queue", &self.rx_queue)
            .field("tx_queue", &self.tx_queue)
            .finish()
    }
}

/// A frame on its way in, borrowed for the length of the callback.
pub struct RxFrame<'a> {
    bytes: &'a [u8],
    checksum: Checksum,
}

impl<'a> RxFrame<'a> {
    /// The frame's bytes, headers and all.
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// The NIC's verdict on the frame's checksums.
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }

    /// How long the frame is.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the frame is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl std::fmt::Debug for RxFrame<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RxFrame")
            .field("len", &self.bytes.len())
            .field("checksum", &self.checksum)
            .finish()
    }
}

/// What the callback wants done with the frame it was handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Give it back to the pool when the callback returns.
    Release,
    /// Keep it: it comes out of the burst as a [`ParkedFrame`].
    Keep,
}

/// What the NIC said about a frame's checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checksum {
    /// The port has no checksum offload, so nothing judged it. Every virtual
    /// device this build's tests run on answers this (D0).
    NotJudged,
    /// The NIC checked it and it was right.
    Good,
    /// The NIC checked it and it was wrong.
    Bad,
}

impl Checksum {
    fn of(shim_verdict: u32) -> Self {
        match shim_verdict {
            SHIM_CKSUM_GOOD => Self::Good,
            SHIM_CKSUM_BAD => Self::Bad,
            _ => Self::NotJudged,
        }
    }
}

/// The port with this name, as EAL knows it.
fn id_of(name: &str) -> Result<u16, Error> {
    let name = CString::new(name)
        .map_err(|_| Error::refused(format!("a port name contains a NUL byte: {name:?}")))?;

    // SAFETY: a NUL-terminated name that outlives the call; the answer is a
    // port id or `-1`, which is checked here.
    let id = unsafe { sys::shim_port_by_name(name.as_ptr()) };
    u16::try_from(id)
        .map_err(|_| Error::refused(format!("no port is named {}", name.to_string_lossy())))
}

/// A port's name, from its id.
fn name_of(id: u16) -> String {
    let mut buffer = [0 as c_char; 64];
    // SAFETY: the buffer is 64 bytes, which is what the shim is told.
    unsafe { sys::shim_port_name(c_uint::from(id), buffer.as_mut_ptr(), 64) };
    // SAFETY: the shim NUL-terminates what it writes.
    unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

/// What the port is, from its id.
fn info_of(id: u16) -> Result<PortInfo, Error> {
    let mut raw = sys::ShimPortInfo {
        driver: [0; 64],
        max_rx_queues: 0,
        max_tx_queues: 0,
        max_mtu: 0,
        min_mtu: 0,
        socket_id: -1,
        rx_offloads: 0,
        tx_offloads: 0,
    };

    // SAFETY: the struct is this crate's own layout, mirrored field for field
    // in the shim, and the port id is one EAL answered with.
    let rc = unsafe { sys::shim_port_info(c_uint::from(id), &mut raw) };
    if rc != 0 {
        return Err(Error::from_rte_errno("the port would not describe itself"));
    }

    // SAFETY: the shim NUL-terminates the driver name it copies in.
    let driver = unsafe { CStr::from_ptr(raw.driver.as_ptr()) }
        .to_string_lossy()
        .into_owned();

    Ok(PortInfo {
        name: name_of(id),
        driver,
        max_rx_queues: raw.max_rx_queues,
        max_tx_queues: raw.max_tx_queues,
        min_mtu: raw.min_mtu,
        max_mtu: raw.max_mtu,
        socket_id: u32::try_from(raw.socket_id).ok(),
        rx_offloads: raw.rx_offloads,
        tx_offloads: raw.tx_offloads,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A port's description, for the cases below that ask it something without
    /// an EAL to make a real one.
    fn described(rx_offloads: u64) -> PortInfo {
        PortInfo {
            name: "a-port".to_owned(),
            driver: "a-pmd".to_owned(),
            max_rx_queues: 4,
            max_tx_queues: 4,
            min_mtu: 68,
            max_mtu: 1500,
            socket_id: None,
            rx_offloads,
            tx_offloads: 0,
        }
    }

    /// What a port is asked for is exactly what it can judge.
    ///
    /// The asymmetry matters: `rte_eth_dev_configure` refuses a bit the device
    /// does not have rather than ignoring it, so a caller that asked for both
    /// regardless would fail to *start* on every virtual device this build
    /// tests on — and a caller that asked for neither would leave a real NIC's
    /// checksum hardware idle, which is the whole of what an offload buys.
    #[test]
    fn a_port_is_asked_for_the_checksums_it_can_judge() {
        let (ipv4, udp) = checksum_offload_bits();
        assert_ne!(ipv4, udp, "the two offloads are two bits, not one");

        assert_eq!(
            0,
            PortSettings::default()
                .asking_for_checksum_offloads(&described(0))
                .rx_offloads,
            "a port that judges nothing is asked for nothing"
        );
        assert_eq!(
            ipv4 | udp,
            PortSettings::default()
                .asking_for_checksum_offloads(&described(ipv4 | udp))
                .rx_offloads
        );
        assert_eq!(
            ipv4,
            PortSettings::default()
                .asking_for_checksum_offloads(&described(ipv4))
                .rx_offloads,
            "and only the half of it the port has"
        );

        assert!(described(ipv4).can_judge_ipv4_checksum());
        assert!(
            !described(ipv4).can_judge_udp_checksum(),
            "one bit set is not both offloads"
        );
    }

    /// Asking is not a rewrite: an offload the caller set for its own reasons
    /// survives, because this adds to the settings rather than replacing them.
    #[test]
    fn asking_leaves_the_other_offloads_alone() {
        let (ipv4, _) = checksum_offload_bits();
        let other = 1u64; // whatever bit a caller set for itself

        let asked = PortSettings {
            rx_offloads: other,
            ..PortSettings::default()
        }
        .asking_for_checksum_offloads(&described(ipv4));

        assert_eq!(other | ipv4, asked.rx_offloads);
    }
}
