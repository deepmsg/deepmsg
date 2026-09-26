//! The process's signal seam.
//!
//! This is ADR-0002 zone 4, which was written for the media syscall shim
//! (`sendmmsg`, `recvmmsg`) and is amended here to cover the other thing a
//! driver process needs from the kernel: being told to stop. The reference
//! installs a handler for `SIGINT` and `SIGTERM` and records the signal number
//! (`aeron-driver/src/main/c/aeronmd.c:37-42`, registered at `:112-113`), then
//! returns it from `main` — so a driver stopped with a signal is
//! distinguishable, by exit code, from one that stopped because it was asked
//! to. That distinction is a contract: `tests/interop/c_driver_terminate.rs`
//! asserts the reference makes it.
//!
//! # Why this is `unsafe`, and why it is small
//!
//! `libc::signal` takes a C function pointer, and Rust cannot spell that
//! without `unsafe`. Everything else here is an atomic store, which is the only
//! kind of write a signal handler may perform without a lock — a handler runs
//! on whichever thread the signal interrupted, so anything that could block or
//! allocate is off limits. The reference's handler is a `volatile int` store
//! for the same reason.

use std::io;
use std::sync::atomic::{AtomicI32, Ordering};

/// No signal has asked this process to stop.
pub const NOT_STOPPED: i32 = -1;

/// The signal that asked, once one has.
static STOP_SIGNAL: AtomicI32 = AtomicI32::new(NOT_STOPPED);

/// Ask to be told about `SIGINT` and `SIGTERM`.
///
/// # Errors
///
/// The error from `signal(2)` if either registration fails.
pub fn install_stop_handler() -> Result<(), io::Error> {
    // SAFETY: `signal` is given a handler with the right signature for a
    // `sighandler_t` and the two signals this process wants to stop on. The
    // handler touches nothing but a static atomic, so it is async-signal-safe
    // in the sense POSIX requires; the only cost of the call is that this
    // process's default disposition for those two signals is replaced, which
    // is the point.
    let handler = handle_stop as extern "C" fn(libc::c_int) as libc::sighandler_t;

    for signal in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: as above; `signal` with a valid signal number and a valid
        // handler is the documented contract, and `SIG_ERR` is the documented
        // failure value.
        if libc::SIG_ERR == unsafe { libc::signal(signal, handler) } {
            return Err(io::Error::last_os_error());
        }
    }

    Ok(())
}

/// The signal that has asked this process to stop, if any.
pub fn stop_signal() -> Option<i32> {
    match STOP_SIGNAL.load(Ordering::SeqCst) {
        NOT_STOPPED => None,
        signal => Some(signal),
    }
}

/// Record the signal and return.
extern "C" fn handle_stop(signal: libc::c_int) {
    STOP_SIGNAL.store(signal, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signal_is_recorded_and_the_process_keeps_running() {
        // Raising it here is the real thing: if the handler were not installed
        // the default disposition would end the test process, so a passing
        // test is also the assertion that the handler is in place.
        install_stop_handler().expect("install");
        assert_eq!(None, stop_signal());

        // SAFETY: `raise` on the calling thread with a signal whose handler
        // this test just installed.
        assert_eq!(0, unsafe { libc::raise(libc::SIGTERM) });

        assert_eq!(Some(libc::SIGTERM), stop_signal());
    }
}
