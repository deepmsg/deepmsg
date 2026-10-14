//! The kernel-bypass media seam — DPDK, behind a shim (ADR-0006).
//!
//! This crate is ADR-0002's sixth named `unsafe` zone, and it is the only place
//! in the workspace where a C library is called: everything above it is safe
//! code over the types below, and the driver's own media is safe code over this
//! API (ADR-0006 decision 3).
//!
//! # What is here, and what it buys
//!
//! The NIC boundary and nothing above it: EAL, ports and their queues, mbufs
//! and their pool, and the two burst calls that cross into the PMD. Ethernet
//! and IP framing, ARP, the port and neighbour tables, demultiplexing — all of
//! that is the driver's, in safe Rust (ADR-0006 decision 4).
//!
//! # One EAL per process
//!
//! `rte_eal_init` may be called once in a process, and `rte_eal_cleanup` does
//! not undo that: after cleanup the process should exit (D0 measured that the
//! threads EAL started outlive the cleanup). [`Eal::init`] is therefore a
//! singleton — the second call answers the first initialisation — and every
//! test of this crate that wants an EAL of its own runs in a process of its
//! own.
//!
//! # Compiled in, not loaded
//!
//! The `dpdk` feature is off by default; with it off this crate is empty and no
//! machine needs DPDK to build the workspace. With it on, the shim is compiled
//! against the installed headers and DPDK is linked as shared libraries. The
//! name that selects this media in the driver resolves against a table this
//! binary was compiled with, which is why nothing here is the dynamic loading
//! `docs/compat.md` refuses (ADR-0006, context).

#![cfg(feature = "dpdk")]
#![deny(unsafe_op_in_unsafe_fn)]

mod sys;

use std::ffi::{CString, OsStr, c_char, c_int};
use std::fmt;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// The lcore id of a thread EAL knows nothing about.
const LCORE_ID_ANY: u32 = u32::MAX;

/// What went wrong inside EAL.
#[derive(Debug)]
pub struct EalError {
    /// DPDK's `rte_errno` where it was DPDK's error, `0` where this crate
    /// refused the request itself.
    pub rte_errno: i32,
    message: String,
}

impl EalError {
    /// The current `rte_errno`, worded by DPDK.
    fn from_rte_errno() -> Self {
        // SAFETY: `rte_errno` is a thread-local in DPDK and `rte_strerror`
        // answers a static string for every value it is given, including the
        // ones it does not know.
        let (errno, message) = unsafe {
            let errno = sys::shim_rte_errno();
            let text = sys::shim_rte_strerror(errno);
            debug_assert!(
                !text.is_null(),
                "rte_strerror answers a string for any errno"
            );
            (
                errno,
                std::ffi::CStr::from_ptr(text)
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        Self {
            rte_errno: errno,
            message,
        }
    }

    fn refused(message: impl Into<String>) -> Self {
        Self {
            rte_errno: 0,
            message: message.into(),
        }
    }
}

impl fmt::Display for EalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.rte_errno == 0 {
            write!(f, "{}", self.message)
        } else {
            write!(f, "{} (rte_errno={})", self.message, self.rte_errno)
        }
    }
}

impl std::error::Error for EalError {}

/// The process's EAL, initialised.
///
/// A witness rather than an owner: EAL is process-global state, so this is a
/// zero-sized handle to it, and the singleton in [`Eal::init`] is what keeps a
/// second initialisation from being attempted.
#[derive(Debug)]
pub struct Eal {
    _private: (),
}

static EAL: OnceLock<Eal> = OnceLock::new();

/// Set by [`Eal::cleanup`]: the `Eal` handle above still exists, but what it
/// witnesses does not, and DPDK will not start again in this process.
static CLEANED_UP: AtomicBool = AtomicBool::new(false);

impl Eal {
    /// Start DPDK for this process, or answer the initialisation already done.
    ///
    /// `args` is the whole command line — `argv[0]` first, as DPDK expects it —
    /// and what it carries is the caller's decision, not this crate's: the
    /// driver adds the lcore set it computes from its own cpuset and the
    /// `--no-huge`/`--in-memory` choice its memory layout needs, and a test
    /// adds the virtual device it wants (ADR-0006 decision 5; D0 measured both
    /// the lcore argument and the mutual exclusion of those two flags).
    ///
    /// The first call initialises EAL. A second call in the same process — from
    /// anywhere, with any arguments — answers the same `Eal` rather than
    /// initialising again, because that is what the library allows.
    pub fn init<I, S>(args: I) -> Result<&'static Eal, EalError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        if CLEANED_UP.load(Ordering::Acquire) {
            return Err(EalError::refused(
                "EAL was cleaned up in this process, and it does not start twice",
            ));
        }
        if let Some(eal) = EAL.get() {
            return Ok(eal);
        }

        let mut argv: Vec<CString> = Vec::new();
        for arg in args {
            let arg = arg.as_ref();
            argv.push(CString::new(arg.as_encoded_bytes()).map_err(|_| {
                EalError::refused(format!("an EAL argument contains a NUL byte: {arg:?}"))
            })?);
        }
        if argv.is_empty() {
            return Err(EalError::refused(
                "EAL needs argv[0], the program's own name",
            ));
        }
        let argc = argv.len() as c_int;
        let mut pointers: Vec<*mut c_char> =
            argv.iter().map(|arg| arg.as_ptr().cast_mut()).collect();
        pointers.push(std::ptr::null_mut());

        // SAFETY: `pointers` holds `argc` valid NUL-terminated strings plus the
        // null terminator `rte_eal_init` expects, and each `CString` in `argv`
        // outlives the call. One call per process is the library's contract and
        // the singleton above is what enforces it: the only way to reach here
        // twice is two threads racing their first `init`, and then DPDK itself
        // refuses the second — the loser gets an `EalError`, never a second
        // EAL.
        let consumed = unsafe { sys::shim_eal_init(argc, pointers.as_mut_ptr()) };
        if consumed < 0 {
            return Err(EalError::from_rte_errno());
        }

        let _ = EAL.set(Eal { _private: () });
        Ok(EAL
            .get()
            .expect("set just above, or by the thread that won the race"))
    }

    /// Shut EAL down. The process should exit afterwards.
    ///
    /// `rte_eal_cleanup` releases what EAL allocated but does not collect the
    /// threads EAL started (`dpdk-intr`, and any worker it was told to make),
    /// and it does not make a second `init` possible. So this is the last thing
    /// a process does with DPDK, and it is on the caller because the order it
    /// belongs in — threads stopped, transports closed, ports stopped, pool
    /// freed, then this — is the caller's to know (ADR-0006, consequences).
    pub fn cleanup() -> Result<(), EalError> {
        if EAL.get().is_none() {
            return Err(EalError::refused(
                "EAL was never initialised in this process",
            ));
        }
        if CLEANED_UP.swap(true, Ordering::AcqRel) {
            return Err(EalError::refused(
                "EAL was already cleaned up in this process",
            ));
        }
        // SAFETY: called at most once per process — the singleton above means
        // an `Eal` exists, and a caller that runs this twice is asking DPDK to
        // clean up twice, which answers a non-zero code rather than
        // misbehaving.
        let rc = unsafe { sys::shim_eal_cleanup() };
        if rc != 0 {
            return Err(EalError::from_rte_errno());
        }
        Ok(())
    }

    /// The calling thread's lcore id, or `None` if EAL does not know it.
    pub fn lcore_id() -> Option<u32> {
        // SAFETY: `rte_lcore_id` reads a thread-local EAL maintains; it is
        // `LCORE_ID_ANY` for a thread that never registered.
        let id = unsafe { sys::shim_lcore_id() };
        (id != LCORE_ID_ANY).then_some(id)
    }

    /// How many lcores EAL has: the main one plus every registered thread.
    pub fn lcore_count() -> u32 {
        // SAFETY: a plain read of EAL's own bookkeeping.
        unsafe { sys::shim_lcore_count() }
    }
}

/// A thread EAL knows about, so that its allocations use the pool's per-lcore
/// cache.
///
/// A driver thread is a `std::thread` EAL did not start: without registering
/// it, `rte_pktmbuf_alloc` goes to the shared ring instead of the cache this
/// thread would otherwise own (R7). Registering does not move the thread — the
/// affinity the driver set stays (D0) — so this belongs after the driver's own
/// pinning, and the guard unregisters on the way out.
#[derive(Debug)]
#[must_use = "unregisters on drop"]
pub struct RegisteredThread {
    _private: (),
}

impl RegisteredThread {
    /// Register the calling thread with EAL.
    pub fn register() -> Result<Self, EalError> {
        // SAFETY: `rte_thread_register` is documented for any thread that EAL
        // did not start, at most once at a time per thread; the guard makes the
        // matching unregister happen exactly once.
        let rc = unsafe { sys::shim_thread_register() };
        if rc != 0 {
            return Err(EalError::from_rte_errno());
        }
        Ok(Self { _private: () })
    }

    /// This thread's lcore id, which registering is what gives it.
    pub fn lcore_id(&self) -> u32 {
        // SAFETY: the guard means this thread is registered.
        unsafe { sys::shim_lcore_id() }
    }
}

impl Drop for RegisteredThread {
    fn drop(&mut self) {
        // SAFETY: registered by `register` above, and unregistered once here.
        unsafe { sys::shim_thread_unregister() };
    }
}

/// Save the calling thread's affinity, for a caller that has to put it back.
///
/// DPDK pins the main thread to its main lcore at init. A driver that computes
/// its lcore argument from the cpuset it was started in does not need this —
/// nothing moves — but the pair stays for the case where it does.
pub fn save_affinity() -> Result<(), EalError> {
    // SAFETY: `pthread_getaffinity_np` with a buffer owned by the shim's own
    // static is its documented use.
    let rc = unsafe { sys::shim_save_affinity() };
    if rc != 0 {
        return Err(EalError::refused(format!(
            "pthread_getaffinity_np answered {rc}"
        )));
    }
    Ok(())
}

/// Put back the affinity [`save_affinity`] saved.
pub fn restore_affinity() -> Result<(), EalError> {
    // SAFETY: as above; the saved set is the one this thread had.
    let rc = unsafe { sys::shim_restore_affinity() };
    if rc != 0 {
        return Err(EalError::refused(format!(
            "pthread_setaffinity_np answered {rc}"
        )));
    }
    Ok(())
}
