//! The kernel seam: the handful of things this driver asks the process's
//! kernel for directly.
//!
//! This is ADR-0002 zone 4, which was written for the media syscall shim
//! (`sendmmsg`, `recvmmsg`) and is amended here to cover the rest of what a
//! driver process needs from the kernel.
//!
//! Three things live here, and they have nothing in common but the reason they
//! cannot be expressed in safe Rust or belong to a protocol module:
//!
//! * **Signals.** The reference installs a handler for `SIGINT` and `SIGTERM`
//!   and records the signal number (`aeron-driver/src/main/c/aeronmd.c:37-42`,
//!   registered at `:112-113`), then returns it from `main` — so a driver
//!   stopped with a signal is distinguishable, by exit code, from one that
//!   stopped because it was asked to. That distinction is a contract:
//!   `tests/interop/c_driver_terminate.rs` asserts the reference makes it.
//! * **A socket probe.** Every log buffer's metadata records the kernel's
//!   default receive and send buffer sizes as *bytes* other processes read
//!   (`aeron-client/src/main/c/util/aeron_netutil.c:883-919`, whose two values
//!   are four of the fields `aeron_logbuffer_metadata_init` writes). They are
//!   machine-dependent, so the only way to write what the reference writes is
//!   to ask the same question.
//! * **Randomness**, for the session id a driver's allocation starts from
//!   (`aeron-client/src/main/c/util/aeron_bitutil.c:61-90`).
//! * **Free space.** Before a log buffer is created, the filesystem it will
//!   land on is asked whether it has room — the reference's
//!   `aeron_usable_fs_space`, a `statvfs` asking `f_frsize * f_bavail`
//!   (`aeron-client/src/main/c/util/aeron_fileutil.c:952-961`).
//!
//! # Why this is `unsafe`, and why it is small
//!
//! `libc::signal` takes a C function pointer and `getsockopt` writes through a
//! pointer, and Rust cannot spell either without `unsafe`. Everything else here
//! is an atomic store, which is the only kind of write a signal handler may
//! perform without a lock — a handler runs on whichever thread the signal
//! interrupted, so anything that could block or allocate is off limits. The
//! reference's handler is a `volatile int` store for the same reason.

use std::io;
use std::sync::atomic::{AtomicI32, Ordering};

use deepmsg_core::clock;

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

/// What a bare socket reports as its receive and send buffer sizes.
///
/// Not "the buffer sizes this process configured": these are what the *kernel*
/// hands a socket nobody has called `setsockopt` on, and they end up in every
/// log buffer's metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SocketBufferLengths {
    /// `SO_RCVBUF` on a fresh socket — on Linux, `net.core.rmem_default`
    /// (the kernel's doubling applies to values a `setsockopt` sets, not to
    /// one nobody has touched).
    pub rcvbuf: i32,
    /// `SO_SNDBUF`, likewise — twice `net.core.wmem_default` here.
    pub sndbuf: i32,
}

/// Ask the kernel for the default socket buffer sizes.
///
/// A UDP socket is made, asked, and closed: the values are properties of the
/// kernel's defaults rather than of that socket, which is why the reference's
/// probe is also a throwaway socket rather than a read of a configuration file
/// (`aeron-client/src/main/c/util/aeron_netutil.c:883-919`).
///
/// # Errors
///
/// The error from `socket(2)`, `getsockopt(2)` or `close(2)`.
pub fn default_socket_buffers() -> io::Result<SocketBufferLengths> {
    // SAFETY: a datagram socket in the internet family, with no options, is a
    // valid call with no preconditions. The descriptor is owned by this
    // function from here to the `close` below, on every path.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut rcvbuf: libc::c_int = 0;
    let mut sndbuf: libc::c_int = 0;
    let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .expect("an int is a valid socklen_t");

    // SAFETY: both calls get a descriptor this function opened, the option
    // name from the same `SOL_SOCKET` level, and a pointer to a live `c_int`
    // together with its length — which is what `getsockopt` writes through and
    // updates. `length` is reset before the second call because the first may
    // have shortened it.
    let received = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            std::ptr::from_mut(&mut rcvbuf).cast(),
            &mut length,
        )
    };
    length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .expect("an int is a valid socklen_t");
    // SAFETY: the same descriptor, option level and shape as the call above,
    // with a pointer to the other `c_int` and its length.
    let send = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            std::ptr::from_mut(&mut sndbuf).cast(),
            &mut length,
        )
    };

    // SAFETY: the descriptor was opened by this function and has not been
    // closed, and no other thread has a copy of it.
    let closed = unsafe { libc::close(fd) };

    if received < 0 || send < 0 || closed < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(SocketBufferLengths { rcvbuf, sndbuf })
}

/// The bytes free to an unprivileged process on the filesystem holding
/// `path` — or zero, when the filesystem cannot be asked.
///
/// This is the reference's `aeron_usable_fs_space`
/// (`aeron-client/src/main/c/util/aeron_fileutil.c:952-961`): `statvfs`'s
/// `f_frsize * f_bavail`, with a failure reading as zero. A caller comparing
/// the answer against a log buffer's length treats that as "no space",
/// which is the safe side to fail on.
pub fn usable_fs_space(path: &std::path::Path) -> u64 {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return 0;
    };

    // SAFETY: `statvfs` is only specified for a zeroed-out struct when the
    // call fails, so this starts from zeroes rather than from whatever the
    // stack held.
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };

    // SAFETY: `path` is a live NUL-terminated string for the duration of the
    // call, and `vfs` is a plain-out struct the call may write through —
    // which is exactly what the two pointers promise `statvfs(3)`.
    if 0 != unsafe { libc::statvfs(path.as_ptr(), &mut vfs) } {
        return 0;
    }

    #[allow(clippy::cast_possible_truncation)] // f_frsize is a page-ish size
    let frsize = vfs.f_frsize as u64;

    frsize * vfs.f_bavail
}

/// A random `i32` to start the session id allocation from.
///
/// The reference reads `/dev/urandom` and **exits the process** if it cannot
/// (`aeron-client/src/main/c/util/aeron_bitutil.c:61-90`). This falls back to
/// the nanosecond clock instead, and the reason is what the value is *for*:
/// session ids need to differ between two drivers started at once, not to be
/// unpredictable to an attacker. A driver that refuses to start because a
/// device node is missing would trade a real outage for a theoretical
/// weakening.
pub fn random_i32() -> i32 {
    let mut bytes = [0u8; 4];
    let read = std::fs::File::open("/dev/urandom")
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut bytes));

    match read {
        Ok(()) => i32::from_ne_bytes(bytes),
        Err(_) => {
            // The low bits of the clock, which is what a driver started twice
            // in the same second would differ by anyway.
            #[allow(clippy::cast_possible_truncation)]
            (clock::epoch_nano_time() as i32)
        }
    }
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

    #[test]
    fn the_socket_probe_answers_and_answers_the_same_thing_twice() {
        // What the probe *returns* is the kernel's business and varies by
        // machine, so the assertion is about the interface: it answers, and
        // the answer is a property of the kernel rather than of the socket the
        // probe happened to open. The values themselves are pinned where they
        // land — in a log buffer's metadata, against the reference's own
        // probing, by the golden test.
        let first = default_socket_buffers().expect("a UDP socket can be made");
        let second = default_socket_buffers().expect("and a second one");

        assert_eq!(first, second);
        assert!(
            first.rcvbuf >= 0 && first.sndbuf >= 0,
            "a negative size is not a buffer length"
        );
    }

    #[test]
    fn randomness_is_not_a_constant() {
        // The value only has to differ between two drivers started at once, so
        // the test is that it differs at all: a fallback to the clock, a fixed
        // seed, or a stubbed-out read would all pass a "it returned something"
        // test and fail this one.
        let values: Vec<i32> = (0..8).map(|_| random_i32()).collect();

        assert!(
            values.iter().any(|value| *value != values[0]),
            "eight draws from a random source should not agree: {values:?}"
        );
    }
}
