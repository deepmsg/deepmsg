//! The kernel seam: the handful of things this driver asks the process's
//! kernel for directly.
//!
//! This is ADR-0002 zone 4, which was written for the media syscall shim
//! (`sendmmsg`, `recvmmsg`) and is amended here to cover the rest of what a
//! driver process needs from the kernel.
//!
//! The things that live here have nothing in common but the reason they cannot
//! be expressed in safe Rust or belong to a protocol module:
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
//! * **The interfaces this host has.** A channel's `interface=` parameter names
//!   one, by address or by name, and the kernel is the only thing that knows
//!   which addresses are local and what their indices are
//!   (`aeron-client/src/main/c/util/aeron_netutil.c:634-787`).
//! * **A thread's name.** `aeron.thread.naming` names the driver's threads, and
//!   the runner threads are named by the `std::thread` that starts them — but
//!   the process's own thread is already running by then, and renaming it means
//!   `pthread_setname_np` (`aeron_thread_set_name`,
//!   `aeron-client/src/main/c/concurrent/aeron_thread.c:141-159`), which is the
//!   one thing here that is neither a syscall nor a signal.
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

/// Datagram sockets and the batching syscalls the data plane moves messages
/// through.
pub mod socket;

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

/// Pin the calling thread to one CPU (`sched_setaffinity`).
///
/// This is how an agent that was given a CPU by `aeron.driver.cpuset.affinity`
/// gets there: the thread sets it on itself as it starts, which is what the
/// reference's `aeron_set_thread_affinity_on_start` does from every runner's
/// `on_start` (`aeronmd.c:137-141`).
///
/// # Errors
///
/// The error from `sched_setaffinity(2)` — `EINVAL` for a CPU that is not on
/// this machine, `EPERM` for one this process may not use.
pub fn set_current_thread_affinity(cpu: i32) -> io::Result<()> {
    // SAFETY: `cpu_set_t` is a plain bit array; zeroing it and setting one bit
    // is what the kernel expects, and `sched_setaffinity` reads only that.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };

    if !(0..=libc::CPU_SETSIZE).contains(&cpu) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a CPU number no mask can hold",
        ));
    }

    // SAFETY: the index was just bounds-checked against the mask's size.
    unsafe { libc::CPU_SET(usize::try_from(cpu).unwrap_or(0), &mut set) };

    // SAFETY: pid 0 is the calling thread, the mask is a live local, and its
    // length is the size of the type it points at.
    let result = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            std::ptr::addr_of!(set),
        )
    };

    if 0 != result {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

/// The CPUs the calling thread may run on (`sched_getaffinity`) — the only way
/// to see what [`set_current_thread_affinity`] did, since the kernel keeps the
/// answer.
///
/// # Errors
///
/// The error from `sched_getaffinity(2)`.
pub fn current_thread_affinity() -> io::Result<Vec<i32>> {
    // SAFETY: as above; this one is written by the kernel.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };

    // SAFETY: the mask is a live local and its length is passed with it.
    let result = unsafe {
        libc::sched_getaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            std::ptr::addr_of_mut!(set),
        )
    };

    if 0 != result {
        return Err(io::Error::last_os_error());
    }

    let mut cpus = Vec::new();
    for cpu in 0..libc::CPU_SETSIZE as usize {
        // SAFETY: the index is inside the mask's size.
        if unsafe { libc::CPU_ISSET(cpu, &set) } {
            cpus.push(i32::try_from(cpu).unwrap_or(i32::MAX));
        }
    }

    Ok(cpus)
}

/// How long the loss report file is: the configured length rounded up to the
/// file page size (`aeron-driver/src/main/c/aeron_driver.c:329-330`).
///
/// The alignment is the reference's and it is why a driver configured with the
/// default megabyte gets exactly that: 1 MiB is already a multiple of every
/// page size it runs with. A configuration that is not gets a file that is one
/// page bigger, which is visible to anyone who looks at the directory.
///
/// # Errors
///
/// [`io::Error`] if the length is not a positive number this build can align.
pub fn loss_report_length(config: &crate::config::DriverConfig) -> io::Result<usize> {
    let length = usize::try_from(config.loss_report_buffer_length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "loss report length"))?;

    if 0 == length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a loss report of no bytes",
        ));
    }

    let page_size = config.layout.page_size;

    if 0 == page_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a file page size of no bytes",
        ));
    }

    Ok(length.div_ceil(page_size) * page_size)
}

/// Give the calling thread a name, the way the reference does
/// (`aeron_thread_set_name`, `concurrent/aeron_thread.c:141-159`).
///
/// Only the process's **own** thread needs this: every runner is a
/// `std::thread` that names itself at birth. The reference renames its own
/// thread to slot 0's role name when `aeron.thread.naming` is `new`
/// (`aeronmd.c:160-163`), and under the classic naming leaves it the name the
/// kernel already gave it — the process's.
///
/// The name is cut to fifteen bytes first, because `pthread_setname_np`
/// **refuses** a longer name rather than truncating it, and two of the
/// reference's classic names are longer than that.
///
/// # Errors
///
/// What `pthread_setname_np` reports: `ERANGE` for a name that is still too
/// long (which [`crate::driver::thread_name`] prevents), or `ENOSYS` on a
/// kernel without the call.
pub fn set_current_thread_name(name: &str) -> io::Result<()> {
    let name = crate::driver::thread_name(name);

    let name = std::ffi::CString::new(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "a thread name cannot hold a zero byte",
        )
    })?;

    // SAFETY: `pthread_self` is this thread's own handle, and `name` is a
    // NUL-terminated string that outlives the call. The call only reads it.
    let result = unsafe { libc::pthread_setname_np(libc::pthread_self(), name.as_ptr()) };

    if 0 == result {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
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

/// Which address family a channel is working in.
///
/// The reference carries `AF_INET`/`AF_INET6` through its lookups; the two
/// cases are all this driver serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressFamily {
    /// `AF_INET`.
    Inet,
    /// `AF_INET6`.
    Inet6,
}

impl AddressFamily {
    /// The family an address is in.
    pub fn of(address: std::net::IpAddr) -> Self {
        match address {
            std::net::IpAddr::V4(_) => Self::Inet,
            std::net::IpAddr::V6(_) => Self::Inet6,
        }
    }
}

/// A local interface address and the kernel's index for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalInterface {
    /// The address this interface holds in the family asked about.
    pub address: std::net::IpAddr,
    /// `if_nametoindex`, which multicast joins and `IP_MULTICAST_IF` take.
    pub index: u32,
}

/// One entry of the kernel's interface list.
struct InterfaceEntry {
    name: String,
    flags: u32,
    address: Option<std::net::IpAddr>,
    netmask: Option<std::net::IpAddr>,
}

/// The kernel's interface list, freed when it goes out of scope.
struct Interfaces {
    head: *mut libc::ifaddrs,
}

impl Interfaces {
    fn open() -> io::Result<Self> {
        let mut head: *mut libc::ifaddrs = std::ptr::null_mut();

        // SAFETY: `getifaddrs` writes an owned list into the pointer it is
        // given and returns zero on success; the list is released by
        // `freeifaddrs` exactly once, in `Drop` below.
        if 0 != unsafe { libc::getifaddrs(&mut head) } {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { head })
    }

    /// Walk the list, newest entries first, as the kernel hands it over.
    fn entries(&self) -> impl Iterator<Item = InterfaceEntry> + '_ {
        let mut cursor = self.head;

        std::iter::from_fn(move || {
            if cursor.is_null() {
                return None;
            }

            // SAFETY: `cursor` starts at the head of a list `getifaddrs`
            // allocated and only ever advances along its `ifa_next` links,
            // which are either valid entries or null — the terminator the
            // loop above tests for.
            let entry = unsafe { &*cursor };
            cursor = entry.ifa_next;

            Some(InterfaceEntry {
                // SAFETY: `ifa_name` is a NUL-terminated string owned by the
                // entry, which outlives this borrow.
                name: unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) }
                    .to_string_lossy()
                    .into_owned(),
                flags: entry.ifa_flags,
                address: sockaddr_to_ip(entry.ifa_addr),
                netmask: sockaddr_to_ip(entry.ifa_netmask),
            })
        })
    }
}

impl Drop for Interfaces {
    fn drop(&mut self) {
        // SAFETY: `head` is the list `getifaddrs` returned, released exactly
        // once — this is the only `freeifaddrs` call on it.
        unsafe { libc::freeifaddrs(self.head) };
    }
}

/// Read the address out of a `sockaddr` the kernel wrote, for the two families
/// this driver serves.
fn sockaddr_to_ip(address: *const libc::sockaddr) -> Option<std::net::IpAddr> {
    if address.is_null() {
        return None;
    }

    // SAFETY: `sa_family` is the first field of every `sockaddr` variant, so
    // reading it from the pointer the kernel wrote is valid whatever the
    // family turns out to be; the two arms below then read the larger struct
    // the family promises is there.
    let family = unsafe { (*address).sa_family } as libc::c_int;
    match family {
        libc::AF_INET => {
            // SAFETY: `AF_INET` means the kernel wrote a `sockaddr_in` at this
            // address.
            let ipv4 = unsafe { &*address.cast::<libc::sockaddr_in>() };
            Some(std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                u32::from_be(ipv4.sin_addr.s_addr),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: `AF_INET6` means the kernel wrote a `sockaddr_in6` at
            // this address.
            let ipv6 = unsafe { &*address.cast::<libc::sockaddr_in6>() };
            Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                ipv6.sin6_addr.s6_addr,
            )))
        }
        _ => None,
    }
}

/// The first address an interface named `name` holds in `family`, as
/// `aeron_ip_lookup_by_name_and_family_func`
/// (`aeron-client/src/main/c/util/aeron_netutil.c:533-568`) picks it: entries
/// that are up, the name matching exactly, the first address in the family.
///
/// # Errors
///
/// The error from `getifaddrs`.
pub fn interface_by_name(family: AddressFamily, name: &str) -> io::Result<Option<LocalInterface>> {
    let interfaces = Interfaces::open()?;

    for entry in interfaces.entries() {
        if 0 == entry.flags & u32::try_from(libc::IFF_UP).unwrap_or(0) || entry.name != name {
            continue;
        }

        let Some(address) = entry.address else {
            continue;
        };

        if AddressFamily::of(address) == family {
            return Ok(Some(LocalInterface {
                address,
                index: interface_index(name),
            }));
        }
    }

    Ok(None)
}

/// The local interface whose address matches `address` under its netmask, as
/// `aeron_ip_lookup_func`
/// (`aeron-client/src/main/c/util/aeron_netutil.c:479-517`) chooses it: a
/// loopback match wins, otherwise the multicast-capable interface with the
/// longest prefix.
///
/// # Errors
///
/// The error from `getifaddrs`.
pub fn interface_for_address(
    family: AddressFamily,
    address: std::net::IpAddr,
    prefix_length: u8,
) -> io::Result<Option<LocalInterface>> {
    let interfaces = Interfaces::open()?;
    let mut loopback = None;
    let mut best: Option<(u8, LocalInterface)> = None;

    for entry in interfaces.entries() {
        if 0 == entry.flags & u32::try_from(libc::IFF_UP).unwrap_or(0) {
            continue;
        }

        let Some(candidate) = entry.address else {
            continue;
        };

        if AddressFamily::of(candidate) != family
            || !address_matches(candidate, address, prefix_length)
        {
            continue;
        }

        let index = interface_index(&entry.name);

        if entry.flags & u32::try_from(libc::IFF_LOOPBACK).unwrap_or(0) != 0 {
            loopback.get_or_insert(LocalInterface {
                address: candidate,
                index,
            });
        } else if entry.flags & u32::try_from(libc::IFF_MULTICAST).unwrap_or(0) != 0 {
            // The prefix length of this interface's own netmask decides which
            // match wins — the longest is the most specific.
            let interface_prefix = entry.netmask.map_or(0, netmask_prefix_length);

            if best
                .as_ref()
                .is_none_or(|(prefix, _)| interface_prefix > *prefix)
            {
                best = Some((
                    interface_prefix,
                    LocalInterface {
                        address: candidate,
                        index,
                    },
                ));
            }
        }
    }

    Ok(loopback.or_else(|| best.map(|(_, interface)| interface)))
}

/// Whether `candidate` falls inside `address`'s net of `prefix_length` bits.
///
/// This is the reference's `aeron_ip_does_prefix_match`
/// (`aeron-client/src/main/c/util/aeron_netutil.c:204-243`), which ANDs the two
/// addresses and compares the leading bits.
///
/// The four-byte address is shifted up into the top of the 128-bit field the
/// comparison happens in, so that a prefix counts from the same end of the
/// address in both families — comparing a v4 address as a small integer would
/// compare its *low* bits against a mask over the *high* ones, which matches
/// everything.
fn address_matches(
    candidate: std::net::IpAddr,
    address: std::net::IpAddr,
    prefix_length: u8,
) -> bool {
    let (candidate, address) = match (candidate, address) {
        (std::net::IpAddr::V4(candidate), std::net::IpAddr::V4(address)) => (
            u128::from(u32::from(candidate)) << 96,
            u128::from(u32::from(address)) << 96,
        ),
        (std::net::IpAddr::V6(candidate), std::net::IpAddr::V6(address)) => {
            (u128::from(candidate), u128::from(address))
        }
        _ => return false,
    };

    let bits = u32::from(prefix_length).min(128);
    let mask = if bits == 0 {
        0
    } else {
        u128::MAX << (128 - bits)
    };

    candidate & mask == address & mask
}

/// How many leading bits a netmask has, as `aeron_ipv4_netmask_to_prefixlen`
/// counts them (`aeron-client/src/main/c/util/aeron_netutil.c:355-358`, a
/// population count).
fn netmask_prefix_length(netmask: std::net::IpAddr) -> u8 {
    match netmask {
        std::net::IpAddr::V4(mask) => u8::try_from(u32::from(mask).count_ones()).unwrap_or(0),
        std::net::IpAddr::V6(mask) => u8::try_from(u128::from(mask).count_ones()).unwrap_or(0),
    }
}

/// `if_nametoindex(name)`, or zero when there is no such interface.
#[allow(clippy::cast_possible_truncation)] // indices are small
fn interface_index(name: &str) -> u32 {
    let Ok(name) = std::ffi::CString::new(name) else {
        return 0;
    };

    // SAFETY: `name` is a live NUL-terminated string for the call, which only
    // reads it and searches the kernel's interface table.
    unsafe { libc::if_nametoindex(name.as_ptr()) }
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
    fn an_address_this_host_holds_finds_its_interface_and_one_it_does_not_finds_nothing() {
        use std::net::{IpAddr, Ipv4Addr};

        // Loopback is on every host this test can run on, and its index is
        // never zero — that is what tells a real lookup from a fallback.
        let loopback =
            interface_for_address(AddressFamily::Inet, IpAddr::V4(Ipv4Addr::LOCALHOST), 32)
                .expect("the kernel answers")
                .expect("every host has a loopback");

        assert_eq!(IpAddr::V4(Ipv4Addr::LOCALHOST), loopback.address);
        assert_ne!(0, loopback.index, "a real interface index is never zero");

        // A prefix match is a *prefix* match: a whole-address lookup of an
        // address this host does not have finds nothing. Comparing the four
        // bytes as a small integer would match everything here, because the
        // mask covers the bits such a comparison never looks at.
        assert_eq!(
            None,
            interface_for_address(
                AddressFamily::Inet,
                IpAddr::V4(Ipv4Addr::new(10, 255, 255, 1)),
                32
            )
            .expect("the kernel answers")
        );
    }

    #[test]
    fn a_named_interface_is_found_by_its_name() {
        // `lo` is the one interface name POSIX promises.
        let loopback = interface_by_name(AddressFamily::Inet, "lo")
            .expect("the kernel answers")
            .expect("every host has a loopback");

        assert_ne!(0, loopback.index);
        assert!(
            interface_by_name(AddressFamily::Inet, "nosuchinterface")
                .expect("the kernel answers")
                .is_none()
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

    /// The pair of syscalls G4-2's affinity is built on, which is also the only
    /// way to see that it did anything: the kernel keeps the mask, so the test
    /// reads it back rather than trusting the return value.
    ///
    /// Pinning the **calling** thread is what an agent does to itself when it
    /// starts, and this test's thread is one.
    #[test]
    fn a_thread_can_be_pinned_and_asked_where_it_is() {
        let allowed = current_thread_affinity().expect("this thread's mask");
        assert!(!allowed.is_empty(), "a thread runs somewhere");

        let cpu = allowed[0];
        set_current_thread_affinity(cpu).expect("a CPU this thread may use");

        assert_eq!(
            vec![cpu],
            current_thread_affinity().expect("the mask again"),
            "the kernel now has this thread on one CPU"
        );
    }
}
