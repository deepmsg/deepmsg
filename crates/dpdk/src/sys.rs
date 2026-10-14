//! The shim's signatures, and nothing else.
//!
//! Every declaration here names a function in `shim/shim.c`, compiled against
//! the installed DPDK headers. No DPDK type appears: a pointer into DPDK is
//! `*mut c_void` on this side, and no struct's layout is visible to Rust — the
//! reason this zone can be reviewed in one sitting (ADR-0006 decision 2).
//!
//! Nothing here is safe to call. The wrappers above this module carry the
//! invariants, and each of their `unsafe` blocks says which one it holds.

use std::ffi::{c_char, c_int, c_uint};

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
}
