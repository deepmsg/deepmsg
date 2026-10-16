//! The shim's signatures, and nothing else.
//!
//! Every declaration here names a function in `shim/shim.c`, compiled against
//! the installed DPDK headers. No DPDK type appears: a pointer into DPDK is
//! `*mut c_void` on this side, the three structs below are this file's own and
//! mirrored field for field in `shim.c`, and no DPDK structure's layout is
//! visible to Rust — the reason this zone can be reviewed in one sitting
//! (ADR-0006 decision 2).
//!
//! Nothing here is safe to call. The wrappers above this module carry the
//! invariants, and each of their `unsafe` blocks says which one it holds.

use std::ffi::{c_char, c_int, c_uint, c_void};

/// One frame received: the handle, its bytes, and the NIC's verdict.
///
/// `checksum` is `SHIM_CKSUM_*` — a three-valued answer the shim computes, so
/// that no DPDK flag reaches this side.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShimRxDesc {
    pub mbuf: *mut c_void,
    pub data: *mut u8,
    pub len: u16,
    pub checksum: u32,
}

pub const SHIM_CKSUM_NONE: u32 = 0;
pub const SHIM_CKSUM_GOOD: u32 = 1;
pub const SHIM_CKSUM_BAD: u32 = 2;

/// One frame to transmit: allocated by the shim, written here, handed back in
/// a burst. `ol_flags` is the offload the caller wants for this frame.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShimTxSlot {
    pub mbuf: *mut c_void,
    pub data: *mut u8,
    pub room: u16,
    pub len: u16,
    pub ol_flags: u64,
    /// Read only when `ol_flags` asks for a checksum the NIC computes: the two
    /// header lengths DPDK's offload contract wants in the mbuf.
    pub l2_len: u16,
    pub l3_len: u16,
}

impl ShimTxSlot {
    /// An empty slot, for the array a burst is filled into.
    pub const EMPTY: Self = Self {
        mbuf: std::ptr::null_mut(),
        data: std::ptr::null_mut(),
        room: 0,
        len: 0,
        ol_flags: 0,
        l2_len: 0,
        l3_len: 0,
    };
}

/// What a port is, as far as this build asks (mirrors `shim_port_info_t`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShimPortInfo {
    pub driver: [c_char; 64],
    pub max_rx_queues: u16,
    pub max_tx_queues: u16,
    pub max_mtu: u16,
    pub min_mtu: u16,
    pub socket_id: c_int,
    pub rx_offloads: u64,
    pub tx_offloads: u64,
}

/// What a port's own counters are, as this build reads them (mirrors
/// `shim_port_stats_t`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ShimPortStats {
    pub ipackets: u64,
    pub opackets: u64,
    pub ibytes: u64,
    pub obytes: u64,
    pub imissed: u64,
    pub ierrors: u64,
    pub oerrors: u64,
    pub rx_nombuf: u64,
}

/// The link's state, with DPDK's bitfields already resolved by the shim
/// (mirrors `shim_port_link_t`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ShimPortLink {
    pub speed_mbps: u32,
    pub up: u8,
    pub full_duplex: u8,
}

/// The name one extended statistic answers to (mirrors `shim_xstat_name_t`,
/// which the shim holds to DPDK's own `RTE_ETH_XSTATS_NAME_SIZE` by an
/// assertion — the array is handed to DPDK, so the two lengths have to be the
/// same one).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ShimXstatName {
    pub name: [c_char; SHIM_XSTAT_NAME_MAX],
}

/// See [`ShimXstatName`].
pub const SHIM_XSTAT_NAME_MAX: usize = 64;

/// One burst carries at most this many frames (`SHIM_BURST_MAX` in the shim).
pub const SHIM_BURST_MAX: usize = 64;

unsafe extern "C" {
    // The EAL: one `init` per process, and a `cleanup` that does not undo it.
    pub fn shim_eal_init(argc: c_int, argv: *mut *mut c_char) -> c_int;
    pub fn shim_eal_cleanup() -> c_int;
    pub fn shim_rte_errno() -> c_int;
    pub fn shim_rte_strerror(err: c_int) -> *const c_char;

    // The calling thread's affinity, for a caller that has to put it back.
    pub fn shim_save_affinity() -> c_int;
    pub fn shim_restore_affinity() -> c_int;

    // Threads EAL did not start: lcore ids, and with them the mempool cache.
    pub fn shim_thread_register() -> c_int;
    pub fn shim_thread_unregister();
    pub fn shim_lcore_id() -> c_uint;
    pub fn shim_lcore_count() -> c_uint;

    // The pool the frames come from.
    pub fn shim_pool_create(
        name: *const c_char,
        count: c_uint,
        cache: c_uint,
        data_room: c_uint,
        socket_id: c_int,
    ) -> *mut c_void;
    pub fn shim_pool_free(pool: *mut c_void);
    pub fn shim_pool_avail(pool: *mut c_void) -> c_uint;

    // The port, and its queues.
    pub fn shim_port_count() -> c_uint;
    pub fn shim_port_by_name(name: *const c_char) -> c_int;
    pub fn shim_port_name(port: c_uint, out: *mut c_char, cap: c_uint);
    pub fn shim_port_info(port: c_uint, out: *mut ShimPortInfo) -> c_int;
    pub fn shim_rx_offload_ipv4_checksum() -> u64;
    pub fn shim_rx_offload_udp_checksum() -> u64;
    pub fn shim_tx_offload_ipv4_checksum() -> u64;
    pub fn shim_tx_offload_udp_checksum() -> u64;
    pub fn shim_mbuf_tx_ipv4() -> u64;
    pub fn shim_mbuf_tx_ip_checksum() -> u64;
    pub fn shim_mbuf_tx_udp_checksum() -> u64;
    pub fn shim_port_configure(
        port: c_uint,
        nb_rx: c_uint,
        nb_tx: c_uint,
        rx_offloads: u64,
        tx_offloads: u64,
    ) -> c_int;
    pub fn shim_rx_queue_setup(
        port: c_uint,
        queue: c_uint,
        descriptors: c_uint,
        pool: *mut c_void,
    ) -> c_int;
    pub fn shim_tx_queue_setup(port: c_uint, queue: c_uint, descriptors: c_uint) -> c_int;
    pub fn shim_port_start(port: c_uint) -> c_int;
    pub fn shim_port_stop(port: c_uint);
    pub fn shim_port_close(port: c_uint);
    pub fn shim_port_mac(port: c_uint, out: *mut u8) -> c_int;
    pub fn shim_port_mtu(port: c_uint, out: *mut u16) -> c_int;
    pub fn shim_port_stats(port: c_uint, out: *mut ShimPortStats) -> c_int;
    pub fn shim_port_link(port: c_uint, out: *mut ShimPortLink) -> c_int;
    pub fn shim_port_xstats_count(port: c_uint) -> c_int;
    pub fn shim_port_xstats_names(port: c_uint, out: *mut ShimXstatName, n: c_uint) -> c_int;
    pub fn shim_port_xstats_values(port: c_uint, out: *mut u64, n: c_uint) -> c_int;

    // The data path, in batches: one call per burst, whatever it carries.
    pub fn shim_rx_burst(port: c_uint, queue: c_uint, out: *mut ShimRxDesc, max: c_uint) -> c_uint;
    pub fn shim_tx_alloc(pool: *mut c_void, out: *mut ShimTxSlot, count: c_uint) -> c_uint;
    pub fn shim_tx_burst(
        port: c_uint,
        queue: c_uint,
        slots: *mut ShimTxSlot,
        count: c_uint,
    ) -> c_uint;
    pub fn shim_mbuf_info_bulk(mbufs: *const *mut c_void, out: *mut ShimRxDesc, count: c_uint);
    pub fn shim_mbuf_free_bulk(mbufs: *mut *mut c_void, count: c_uint);

    // A ring, for the frames that cross threads.
    pub fn shim_ring_create(name: *const c_char, capacity: c_uint) -> *mut c_void;
    pub fn shim_ring_free(ring: *mut c_void);
    pub fn shim_ring_enqueue_burst(
        ring: *mut c_void,
        mbufs: *const *mut c_void,
        count: c_uint,
    ) -> c_uint;
    pub fn shim_ring_dequeue_burst(
        ring: *mut c_void,
        mbufs: *mut *mut c_void,
        count: c_uint,
    ) -> c_uint;
}
