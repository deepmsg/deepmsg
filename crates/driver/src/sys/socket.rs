//! Datagram sockets, with the batching syscalls the data plane sends and
//! receives through.
//!
//! This is the other half of ADR-0002 zone 4, and it exists because the
//! reference's own transport is a thin layer over the same calls
//! (`aeron-driver/src/main/c/media/aeron_udp_channel_transport.c`): `socket`,
//! `bind`, `connect`, `setsockopt`, `send`/`sendmmsg`, `recvmmsg`,
//! `getsockname`. Nothing here has a protocol opinion — a [`DatagramSocket`]
//! knows an address family and whether it is connected, and the layers above
//! decide what the bytes mean.
//!
//! # Why `sendmmsg` rather than a loop of `send`s
//!
//! One datagram per `sendmsg`, batched into one syscall: the reference's
//! `aeron_udp_channel_transport_sendv`
//! (`aeron_udp_channel_transport.c:701-752`) sends up to sixteen datagrams
//! this way and reports how many the kernel took, which is what lets the
//! sender's position advance by exactly what left the machine.
//!
//! # The receive side counts *datagrams*
//!
//! `recvmmsg` returns how many messages were filled, and each one's length.
//! UDP is datagram-atomic, so a short read is a short *datagram*, never a
//! truncated one — which is why the buffer is always an MTU-sized frame and the
//! length is the frame's.

use std::io;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::AddressFamily;

/// The most datagrams one batched call may carry, which is the reference's
/// `AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX` and
/// `AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX` — both sixteen
/// (`aeron-driver/src/main/c/aeron_driver_context.h:55-59`).
pub const MAX_BATCH: usize = 16;

/// Where a received datagram came from and how long it was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Datagram {
    /// The number of bytes the kernel wrote.
    pub length: usize,
    /// The source address; `None` when the socket is connected and the kernel
    /// reported none.
    pub source: Option<SocketAddr>,
    /// When the kernel says the datagram arrived, in nanoseconds since the
    /// epoch — `None` unless the socket was asked for receive timestamps
    /// ([`DatagramSocket::set_receive_timestamps`]).
    ///
    /// This is the only reading of "when did it arrive" that is not the
    /// driver's own clock, and it is why the option exists: the distance from
    /// here to the write into the log is the driver's receive latency, with the
    /// caller's own polling on it.
    pub kernel_ns: Option<i64>,
}

/// The datagrams one [`DatagramSocket::receive_batch`] filled, in order.
///
/// A fixed-size array rather than a `Vec`: this is on the receive hot path,
/// where ADR-0003 keeps allocation out.
#[derive(Clone, Copy, Debug)]
pub struct Datagrams {
    datagrams: [Datagram; MAX_BATCH],
    count: usize,
}

impl Datagrams {
    /// An empty batch.
    pub const fn new() -> Self {
        Self {
            datagrams: [Datagram {
                length: 0,
                source: None,
                kernel_ns: None,
            }; MAX_BATCH],
            count: 0,
        }
    }

    /// The datagrams that were filled, in the order the kernel reported them.
    pub fn as_slice(&self) -> &[Datagram] {
        &self.datagrams[..self.count]
    }

    /// How many datagrams were filled.
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether none were.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Default for Datagrams {
    fn default() -> Self {
        Self::new()
    }
}

/// Room for one control message carrying a `timespec`, rounded up to the
/// alignment `cmsghdr` needs — which is what `CMSG_SPACE` is for, and why the
/// buffer below is `u64`s rather than bytes.
const CONTROL_BYTES: usize =
    // SAFETY: `CMSG_SPACE` is arithmetic on constants — no pointer, no state.
    unsafe { libc::CMSG_SPACE(std::mem::size_of::<libc::timespec>() as libc::c_uint) as usize };

/// The same, in `u64`s, so the buffer's alignment is `cmsghdr`'s.
const CONTROL_WORDS: usize = CONTROL_BYTES.div_ceil(std::mem::size_of::<u64>());

/// The kernel's arrival stamp from a message's control block, if it put one
/// there (`SCM_TIMESTAMPNS`).
///
/// # Safety
///
/// `header` must be a message the kernel just filled, with `msg_control`
/// pointing at a buffer at least `msg_controllen` bytes long and aligned for
/// `cmsghdr`.
unsafe fn kernel_timestamp(header: &libc::msghdr) -> Option<i64> {
    // SAFETY: the caller guarantees `msg_control` bounds and alignment, which
    // is exactly what the `cmsg` macros walk; every read below is inside that
    // buffer.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(header);

        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
            {
                let stamp =
                    std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::timespec>());

                return Some(stamp.tv_sec * 1_000_000_000 + stamp.tv_nsec);
            }

            cmsg = libc::CMSG_NXTHDR(header, cmsg);
        }

        None
    }
}

/// `IPV6_JOIN_GROUP` — one option number with `IPV6_ADD_MEMBERSHIP` on Linux,
/// which is the name `libc` carries
/// (`unix/linux_like/mod.rs:898`).
const IPV6_JOIN_GROUP: libc::c_int = libc::IPV6_ADD_MEMBERSHIP;

/// `IPV6_MULTICAST_ALL` (option 29 on Linux), which `libc` has no name for on
/// glibc: the reference guards it with `#if defined(IPV6_MULTICAST_ALL)` for
/// the same reason (`aeron_udp_channel_transport.c:190-205`) and treats
/// `ENOPROTOOPT` as "this kernel is older than 4.20", which is what
/// [`DatagramSocket::set_multicast_all_disabled`] does.
///
/// The option is what keeps a socket from hearing groups it never joined:
/// without it, delivery goes to every socket bound to the port that has joined
/// *any* group.
const IPV6_MULTICAST_ALL: libc::c_int = 29;

/// A bind that failed, with the two things the reference's message names: the
/// descriptor and the address (`aeron_bind`, `aeron_socket.c:88-95`).
///
/// Carried rather than folded into an `io::Error` because the caller three
/// layers up is the one that composes the entry, and the descriptor number is
/// knowable only here.
#[derive(Debug)]
pub struct BindFailure {
    /// The errno, which is also what the composition's first line carries.
    pub source: io::Error,
    /// The descriptor the bind was attempted on.
    pub fd: i32,
    /// The address it was attempted at.
    pub address: SocketAddr,
}

impl std::fmt::Display for BindFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for BindFailure {}

/// A UDP socket, owned for as long as this value lives.
#[derive(Debug)]
pub struct DatagramSocket {
    fd: libc::c_int,
    family: AddressFamily,
    /// Whether the kernel is asked to stamp arriving datagrams
    /// (`set_receive_timestamps`). Read once per batch, so a plain relaxed load.
    receive_timestamps: std::sync::atomic::AtomicBool,
}

impl DatagramSocket {
    /// A datagram socket in `family`, as
    /// `aeron_udp_channel_transport_init` opens one
    /// (`aeron_udp_channel_transport.c:136-141`).
    ///
    /// # Errors
    ///
    /// The error from `socket(2)`.
    pub fn open(family: AddressFamily) -> io::Result<Self> {
        let domain = match family {
            AddressFamily::Inet => libc::AF_INET,
            AddressFamily::Inet6 => libc::AF_INET6,
        };

        // SAFETY: `socket` with a supported domain, `SOCK_DGRAM` and the
        // default protocol is always valid; the descriptor it returns is owned
        // by this value from here on and released once, in `Drop`.
        let fd = unsafe { libc::socket(domain, libc::SOCK_DGRAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            fd,
            family,
            receive_timestamps: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Bind the socket to a local address
    /// (`aeron_udp_channel_transport.c:147-154`).
    ///
    /// # Errors
    ///
    /// The error from `bind(2)`.
    pub fn bind(&self, address: SocketAddr) -> Result<(), BindFailure> {
        let (storage, length) = to_sockaddr_storage(address);

        // SAFETY: the descriptor is live, and `storage` is a `sockaddr_storage`
        // the length describes — which is what `bind` reads.
        let result = unsafe {
            libc::bind(
                self.fd,
                std::ptr::from_ref(&storage).cast::<libc::sockaddr>(),
                length,
            )
        };

        if result < 0 {
            return Err(BindFailure {
                source: io::Error::last_os_error(),
                fd: self.fd,
                address,
            });
        }

        Ok(())
    }

    /// Connect the socket, so that sends need no address and the kernel
    /// filters what arrives (`:316-324`).
    ///
    /// # Errors
    ///
    /// The error from `connect(2)`.
    pub fn connect(&self, address: SocketAddr) -> io::Result<()> {
        let (storage, length) = to_sockaddr_storage(address);

        // SAFETY: as `bind`: a live descriptor and a `sockaddr` the length
        // describes.
        let result = unsafe {
            libc::connect(
                self.fd,
                std::ptr::from_ref(&storage).cast::<libc::sockaddr>(),
                length,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// `SO_REUSEADDR` (`aeron_udp_channel_transport.c:166-174`).
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_reuse_address(&self) -> io::Result<()> {
        let enabled: libc::c_int = 1;

        // SAFETY: a live descriptor, an option name from `SOL_SOCKET` and a
        // pointer to a live `c_int` of the length given — `setsockopt`'s
        // documented shape for an integer option.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                std::ptr::from_ref(&enabled).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// `SO_RCVBUF` (`:326-334`).
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_receive_buffer(&self, bytes: usize) -> io::Result<()> {
        self.set_byte_option(libc::SO_RCVBUF, bytes)
    }

    /// `SO_SNDBUF` (`:336-344`).
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_send_buffer(&self, bytes: usize) -> io::Result<()> {
        self.set_byte_option(libc::SO_SNDBUF, bytes)
    }

    /// `IP_TTL`, which is what a unicast sender's `ttl=` sets (the reference
    /// sets `IP_MULTICAST_TTL`, `:304-312`, because only multicast honours it —
    /// the unicast option is the same idea for the unicast path).
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_ttl(&self, ttl: u8) -> io::Result<()> {
        let value = libc::c_int::from(ttl);
        let (level, name) = match self.family() {
            AddressFamily::Inet => (libc::IPPROTO_IP, libc::IP_TTL),
            AddressFamily::Inet6 => (libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS),
        };

        // SAFETY: a live descriptor, an option from the level given, and a
        // live `c_int` with its length.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                level,
                name,
                std::ptr::from_ref(&value).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// `SO_REUSEPORT` (`aeron_udp_channel_transport.c:176-182`).
    ///
    /// The reference sets this beside `SO_REUSEADDR` and only on a multicast
    /// socket, where several subscribers to one group share the port —
    /// `SO_REUSEADDR` alone lets a second socket *bind*, and `SO_REUSEPORT` is
    /// what makes the kernel spread the group's datagrams across all of them.
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_reuse_port(&self) -> io::Result<()> {
        self.set_option(libc::SOL_SOCKET, libc::SO_REUSEPORT, &1i32)
    }

    /// Join a multicast group on an interface
    /// (`aeron_udp_channel_transport.c:213-227`, `:275-293`).
    ///
    /// The families name the interface differently, which is the kernel's
    /// doing and not the reference's: IPv4 takes the interface's *address*,
    /// IPv6 takes its *index*.
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`; `EINVAL` for a group and an interface
    /// in different families.
    pub fn join_multicast_group(
        &self,
        group: IpAddr,
        interface: IpAddr,
        interface_index: u32,
    ) -> io::Result<()> {
        match (group, interface) {
            (IpAddr::V4(group), IpAddr::V4(interface)) => {
                let request = libc::ip_mreq {
                    imr_multiaddr: in_addr(group),
                    imr_interface: in_addr(interface),
                };

                self.set_option(libc::IPPROTO_IP, libc::IP_ADD_MEMBERSHIP, &request)
            }

            (IpAddr::V6(group), _) => {
                let request = libc::ipv6_mreq {
                    ipv6mr_multiaddr: libc::in6_addr {
                        s6_addr: group.octets(),
                    },
                    ipv6mr_interface: interface_index,
                };

                self.set_option(libc::IPPROTO_IPV6, IPV6_JOIN_GROUP, &request)
            }

            (IpAddr::V4(_), IpAddr::V6(_)) => Err(io::Error::from_raw_os_error(libc::EINVAL)),
        }
    }

    /// `IP_MULTICAST_IF` / `IPV6_MULTICAST_IF`: the interface this socket's
    /// multicast **sends** leave by (`aeron_udp_channel_transport.c:230-235`,
    /// `:295-302`).
    ///
    /// Both families set it on the sending descriptor rather than on the one
    /// that joins, which is why a multicast transport may hold two.
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_multicast_interface(
        &self,
        interface: IpAddr,
        interface_index: u32,
    ) -> io::Result<()> {
        match interface {
            IpAddr::V4(interface) => {
                self.set_option(libc::IPPROTO_IP, libc::IP_MULTICAST_IF, &in_addr(interface))
            }

            IpAddr::V6(_) => self.set_option(
                libc::IPPROTO_IPV6,
                libc::IPV6_MULTICAST_IF,
                &interface_index,
            ),
        }
    }

    /// `IP_MULTICAST_TTL` / `IPV6_MULTICAST_HOPS`
    /// (`aeron_udp_channel_transport.c:240-248`, `:304-312`).
    ///
    /// One byte, because that is what the reference hands the kernel
    /// (`sizeof(params->ttl)`, `:308`) — the option reads an `int` or a
    /// single octet and the reference chose the octet.
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_multicast_ttl(&self, ttl: u8) -> io::Result<()> {
        match self.family() {
            AddressFamily::Inet => self.set_option(libc::IPPROTO_IP, libc::IP_MULTICAST_TTL, &ttl),

            AddressFamily::Inet6 => {
                self.set_option(libc::IPPROTO_IPV6, libc::IPV6_MULTICAST_HOPS, &ttl)
            }
        }
    }

    /// Turn off delivery of groups this socket did not join
    /// (`aeron_udp_channel_transport.c:190-205`, `:257-272`).
    ///
    /// Not an error when the kernel has never heard of the option: the
    /// reference clears the error and carries on, and so does this.
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`, except `ENOPROTOOPT`.
    pub fn set_multicast_all_disabled(&self) -> io::Result<()> {
        let disabled: libc::c_int = 0;

        let (level, name) = match self.family() {
            AddressFamily::Inet => (libc::IPPROTO_IP, libc::IP_MULTICAST_ALL),
            AddressFamily::Inet6 => (libc::IPPROTO_IPV6, IPV6_MULTICAST_ALL),
        };

        match self.set_option(level, name, &disabled) {
            Err(error) if error.raw_os_error() == Some(libc::ENOPROTOOPT) => Ok(()),
            result => result,
        }
    }

    /// Ask the kernel to stamp arriving datagrams with the time it took them
    /// off the queue (`SO_TIMESTAMPNS`), so that a datagram's own arrival can
    /// be told apart from the moment this process got round to reading it.
    ///
    /// Not the reference's option — it has none — and not free: the kernel
    /// builds a control message for every datagram, which is work on the very
    /// path being timed. It is here for `debug.stage.timing`, and a run whose
    /// numbers are reported leaves it off.
    ///
    /// # Errors
    ///
    /// The error from `setsockopt(2)`.
    pub fn set_receive_timestamps(&self, enabled: bool) -> io::Result<()> {
        let enabled: libc::c_int = i32::from(enabled);
        self.set_option(libc::SOL_SOCKET, libc::SO_TIMESTAMPNS, &enabled)?;
        self.receive_timestamps
            .store(enabled != 0, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Put the socket in non-blocking mode, which is how the data plane polls
    /// (`aeron_udp_channel_transport.c:356-366`).
    ///
    /// # Errors
    ///
    /// The error from `fcntl(2)`.
    pub fn set_nonblocking(&self) -> io::Result<()> {
        // SAFETY: `F_GETFL` reads the descriptor's flags and takes no
        // argument; the descriptor is live.
        let flags = unsafe { libc::fcntl(self.fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: `F_SETFL` sets the flags read above, with `O_NONBLOCK`
        // added; a live descriptor and an integer flag word are its whole
        // contract.
        if unsafe { libc::fcntl(self.fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Send one datagram to a connected socket's peer
    /// (`aeron_udp_channel_transport_send_connected`, `:646-665`).
    ///
    /// # Errors
    ///
    /// The error from `send(2)` — `WouldBlock` when the socket's buffer is
    /// full, which the caller treats as "nothing left this time".
    pub fn send(&self, buffer: &[u8]) -> io::Result<usize> {
        // SAFETY: a live descriptor, a live buffer and its length; no flags.
        let result = unsafe {
            libc::send(
                self.fd,
                buffer.as_ptr().cast(),
                buffer.len(),
                libc::MSG_NOSIGNAL,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        #[allow(clippy::cast_sign_loss)] // non-negative above
        Ok(result as usize)
    }

    /// Send up to [`MAX_BATCH`] datagrams, one per buffer, in one syscall
    /// (`aeron_udp_channel_transport_sendv`, `:701-752`).
    ///
    /// `address` is the destination for an unconnected socket; a connected
    /// socket takes `None` and the kernel uses the address it was connected to.
    ///
    /// Returns how many datagrams were sent — never a partial one, because UDP
    /// datagrams are atomic.
    ///
    /// # Errors
    ///
    /// The error from `sendmmsg(2)`, with `WouldBlock` and `ConnectionRefused`
    /// left for the caller to read as "nothing sent this time" — which is what
    /// the reference does with `EAGAIN`, `EWOULDBLOCK`, `ECONNREFUSED` and
    /// `EINTR` (`:728-731`).
    pub fn send_batch(&self, address: Option<SocketAddr>, buffers: &[&[u8]]) -> io::Result<usize> {
        let count = buffers.len().min(MAX_BATCH);
        if count == 0 {
            return Ok(0);
        }

        let mut messages = [MaybeUninit::<libc::mmsghdr>::uninit(); MAX_BATCH];
        let mut io_vectors = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; MAX_BATCH];

        // The address lives until the call returns, which is what `msg_name`
        // pointing into it requires.
        let (storage, length) = address.map_or((None, 0), |address| {
            let (storage, length) = to_sockaddr_storage(address);
            (Some(storage), length)
        });

        for (index, buffer) in buffers.iter().take(count).enumerate() {
            // One datagram per message, each with its own iovec — the shape the
            // reference builds (`:713-723`).
            io_vectors[index] = libc::iovec {
                iov_base: buffer.as_ptr().cast_mut().cast(),
                iov_len: buffer.len(),
            };

            // SAFETY: `msghdr` is plain data with no invalid bit patterns; the
            // fields that matter are written below, and the ones that do not
            // (control, flags) are documented as ignored when zero.
            let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
            header.msg_name = storage.as_ref().map_or(std::ptr::null_mut(), |storage| {
                std::ptr::from_ref(storage).cast_mut().cast()
            });
            header.msg_namelen = length;
            header.msg_iov = std::ptr::from_mut(&mut io_vectors[index]);
            header.msg_iovlen = 1;

            messages[index].write(libc::mmsghdr {
                msg_hdr: header,
                msg_len: 0,
            });
        }

        // SAFETY: the first `count` entries of `messages` were initialized
        // above, each holding a `msghdr` whose `msg_iov` points at an entry of
        // `io_vectors`, which in turn points at a buffer lent by the caller —
        // all of which outlive the call.
        let sent = unsafe {
            libc::sendmmsg(
                self.fd,
                messages.as_mut_ptr().cast::<libc::mmsghdr>(),
                count as libc::c_uint,
                0,
            )
        };

        if sent < 0 {
            return Err(io::Error::last_os_error());
        }

        #[allow(clippy::cast_sign_loss)] // non-negative above
        Ok(sent as usize)
    }

    /// Receive up to `buffers.len()` datagrams into them, in one syscall
    /// (`aeron_udp_channel_transport_recvmmsg`, `:549-644`).
    ///
    /// The buffer lengths are the maximum datagram size, so a filled buffer is
    /// a whole datagram — UDP does not truncate silently into a smaller
    /// buffer, it truncates the *datagram* and reports the length it was, which
    /// the caller compares against the MTU.
    ///
    /// # Errors
    ///
    /// The error from `recvmmsg(2)`, with `WouldBlock` left for the caller to
    /// read as "nothing arrived this time" (`:584-587`).
    pub fn receive_batch(
        &self,
        buffers: &mut [Vec<u8>],
        datagrams: &mut Datagrams,
    ) -> io::Result<usize> {
        let count = buffers.len().min(MAX_BATCH).min(datagrams.datagrams.len());
        if count == 0 {
            return Ok(0);
        }

        let mut messages = [MaybeUninit::<libc::mmsghdr>::uninit(); MAX_BATCH];
        let mut names = [MaybeUninit::<libc::sockaddr_storage>::uninit(); MAX_BATCH];
        let mut io_vectors = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }; MAX_BATCH];

        // Only when the socket was asked for them: an unused control buffer is
        // a `msg_controllen` of zero, and the kernel then writes no control
        // message at all.
        let stamping = self
            .receive_timestamps
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut control = [[MaybeUninit::<u64>::uninit(); CONTROL_WORDS]; MAX_BATCH];

        for (index, buffer) in buffers.iter_mut().take(count).enumerate() {
            io_vectors[index] = libc::iovec {
                iov_base: buffer.as_mut_ptr().cast(),
                iov_len: buffer.len(),
            };

            // SAFETY: `msghdr` is plain data with no invalid bit patterns; the
            // fields that matter are written below, and the ones that do not
            // (control, flags) are documented as ignored when zero.
            let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
            header.msg_name = names[index].as_mut_ptr().cast();
            header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            header.msg_iov = std::ptr::from_mut(&mut io_vectors[index]);
            header.msg_iovlen = 1;

            if stamping {
                header.msg_control = control[index].as_mut_ptr().cast();
                header.msg_controllen = CONTROL_BYTES as libc::size_t;
            }

            messages[index].write(libc::mmsghdr {
                msg_hdr: header,
                msg_len: 0,
            });
        }

        // No timeout: the socket is non-blocking, so this returns everything
        // that was queued, up to `count`. The reference passes a zero
        // `timespec` here instead (`:560`, `:577`), which the kernel reads as
        // "return after the first datagram" — the same datagrams, one syscall
        // per burst instead of one per datagram.
        //
        // SAFETY: the first `count` messages were initialized above, each
        // pointing at one of this function's `names` entries and at a buffer
        // the caller lent for the duration of the call.
        let received = unsafe {
            libc::recvmmsg(
                self.fd,
                messages.as_mut_ptr().cast::<libc::mmsghdr>(),
                count as libc::c_uint,
                0,
                std::ptr::null_mut(),
            )
        };

        if received < 0 {
            return Err(io::Error::last_os_error());
        }

        #[allow(clippy::cast_sign_loss)] // non-negative above
        let received = received as usize;
        datagrams.count = received;

        for index in 0..received {
            // SAFETY: the kernel filled the first `received` messages, so each
            // one's `msg_len` and `msg_hdr.msg_namelen` are written.
            let message = unsafe { messages[index].assume_init_ref() };

            datagrams.datagrams[index] = Datagram {
                length: message.msg_len as usize,
                source: if message.msg_hdr.msg_namelen == 0 {
                    None
                } else {
                    // SAFETY: the kernel wrote an address of `msg_namelen`
                    // bytes into this entry, and `msg_namelen` says how much
                    // of it is meaningful.
                    unsafe { from_sockaddr(names[index].as_ptr(), message.msg_hdr.msg_namelen) }
                },
                kernel_ns: if stamping {
                    // SAFETY: `stamping` was set with the same control buffer
                    // this message was given, so the kernel wrote its headers
                    // into `control[index]` and `msg_controllen` bounds them.
                    unsafe { kernel_timestamp(&message.msg_hdr) }
                } else {
                    None
                },
            };
        }

        Ok(received)
    }

    /// `getsockname(2)`, which is where a channel's local address comes from
    /// when the kernel chose the port
    /// (`aeron_udp_channel_transport_bind_addr_and_port`, `:917-930`).
    ///
    /// # Errors
    ///
    /// The error from `getsockname(2)`, or [`io::ErrorKind::InvalidData`] for
    /// an address family this driver does not speak.
    pub fn local_address(&self) -> io::Result<SocketAddr> {
        let mut storage = MaybeUninit::<libc::sockaddr_storage>::uninit();
        let mut length = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        // SAFETY: a live descriptor, and a buffer plus its length for the
        // kernel to write a `sockaddr` into — which `getsockname` updates to
        // the length it wrote.
        let result = unsafe {
            libc::getsockname(
                self.fd,
                storage.as_mut_ptr().cast::<libc::sockaddr>(),
                &mut length,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: the call succeeded, so the buffer holds a `sockaddr`.
        unsafe { from_sockaddr(storage.as_ptr(), length) }
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown address family"))
    }

    /// `SO_RCVBUF` as the kernel reports it, which is what a receiver compares
    /// its window against (`aeron_udp_channel_transport_get_so_rcvbuf`,
    /// `:904-915`).
    ///
    /// # Errors
    ///
    /// The error from `getsockopt(2)`.
    pub fn receive_buffer_size(&self) -> io::Result<usize> {
        let mut value: libc::c_int = 0;
        let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;

        // SAFETY: a live descriptor, a `SOL_SOCKET` option, and a live `c_int`
        // with its length for the kernel to write through.
        let result = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                std::ptr::from_mut(&mut value).cast(),
                &mut length,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        #[allow(clippy::cast_sign_loss)] // a buffer length is not negative
        Ok(value as usize)
    }

    /// The address family this socket was opened in.
    ///
    /// Read back with `getsockname` rather than remembered, because the
    /// wildcard address a caller binds does not tell the two families apart
    /// from the outside.
    fn family(&self) -> AddressFamily {
        self.family
    }

    /// A `SOL_SOCKET` option whose value is a byte count.
    fn set_byte_option(&self, name: libc::c_int, bytes: usize) -> io::Result<()> {
        let value = libc::c_int::try_from(bytes).unwrap_or(libc::c_int::MAX);

        self.set_option(libc::SOL_SOCKET, name, &value)
    }

    /// `setsockopt(2)` for an option whose value is some plain-data struct.
    ///
    /// `T`'s size is what the kernel is told, which is how the reference writes
    /// these: it passes a pointer and `sizeof` the thing pointed at, so an
    /// option that wants one byte gets one byte.
    fn set_option<T>(&self, level: libc::c_int, name: libc::c_int, value: &T) -> io::Result<()> {
        // SAFETY: a live descriptor, an option name from the level given, and a
        // live `T` with its own size — `setsockopt`'s documented shape.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                level,
                name,
                std::ptr::from_ref(value).cast(),
                std::mem::size_of::<T>() as libc::socklen_t,
            )
        };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

impl Drop for DatagramSocket {
    fn drop(&mut self) {
        // SAFETY: the descriptor was opened by `open` and is released exactly
        // once, here — no other value holds a copy.
        unsafe { libc::close(self.fd) };
    }
}

/// A `SocketAddr` as the kernel's own address type.
/// An IPv4 address as the kernel wants it in an option's payload: the four
/// octets, which on a little-endian machine is the byte-swapped number
/// (`in_addr.s_addr` is network order).
fn in_addr(address: Ipv4Addr) -> libc::in_addr {
    libc::in_addr {
        s_addr: u32::from(address).to_be(),
    }
}

fn to_sockaddr_storage(address: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: `sockaddr_storage` is plain data and every field is a byte
    // array or an integer; zeroing it is how the reference builds one too
    // (`memset(&endpoint_addr, 0, sizeof(endpoint_addr))`,
    // `aeron_udp_channel.c:286-288`).
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };

    match address {
        SocketAddr::V4(address) => {
            // SAFETY: the storage is at least as large as a `sockaddr_in`, and
            // the family field is written by the cast struct.
            let inet =
                unsafe { &mut *std::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in>() };
            inet.sin_family = libc::AF_INET as libc::sa_family_t;
            inet.sin_port = address.port().to_be();
            inet.sin_addr.s_addr = u32::from(*address.ip()).to_be();

            (
                storage,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(address) => {
            // SAFETY: as above, with the larger of the two address types.
            let inet =
                unsafe { &mut *std::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in6>() };
            inet.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            inet.sin6_port = address.port().to_be();
            inet.sin6_addr.s6_addr = address.ip().octets();
            inet.sin6_scope_id = address.scope_id();

            (
                storage,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}

/// The kernel's address type as a `SocketAddr`, for the two families this
/// driver serves.
///
/// # Safety
///
/// `address` must point at a `sockaddr` whose family is `AF_INET` or
/// `AF_INET6`, with at least `length` bytes valid.
unsafe fn from_sockaddr(
    address: *const libc::sockaddr_storage,
    length: libc::socklen_t,
) -> Option<SocketAddr> {
    if (length as usize) < std::mem::size_of::<libc::sa_family_t>() {
        return None;
    }

    // SAFETY: the caller guarantees at least `length` readable bytes, which is
    // enough for the family field every `sockaddr` starts with.
    let family = unsafe { (*address).ss_family } as libc::c_int;

    match family {
        libc::AF_INET => {
            if (length as usize) < std::mem::size_of::<libc::sockaddr_in>() {
                return None;
            }

            // SAFETY: `AF_INET` means the caller's buffer holds a
            // `sockaddr_in`, and `length` says the whole one is there.
            let inet = unsafe { &*address.cast::<libc::sockaddr_in>() };
            let ip = Ipv4Addr::from(u32::from_be(inet.sin_addr.s_addr));

            Some(SocketAddr::new(IpAddr::V4(ip), u16::from_be(inet.sin_port)))
        }
        libc::AF_INET6 => {
            if (length as usize) < std::mem::size_of::<libc::sockaddr_in6>() {
                return None;
            }

            // SAFETY: as above, with the IPv6 address type.
            let inet = unsafe { &*address.cast::<libc::sockaddr_in6>() };
            let ip = Ipv6Addr::from(inet.sin6_addr.s6_addr);
            let address = SocketAddr::new(IpAddr::V6(ip), u16::from_be(inet.sin6_port));

            // The scope id only means anything for a link-local address, and
            // only when the kernel wrote one.
            match (address, inet.sin6_scope_id) {
                (SocketAddr::V6(mut address), scope) if scope != 0 => {
                    address.set_scope_id(scope);
                    Some(SocketAddr::V6(address))
                }
                (address, _) => Some(address),
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_datagram_goes_from_one_socket_to_another() {
        let receiver = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        receiver
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        receiver.set_nonblocking().expect("non-blocking");

        let local = receiver.local_address().expect("a bound address");
        assert_ne!(0, local.port(), "the kernel picks a port");

        let sender = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        let sent = sender
            .send_batch(Some(local), &[b"one", b"two"])
            .expect("a send");
        assert_eq!(2, sent);

        let mut buffers = vec![vec![0u8; 64], vec![0u8; 64]];
        let mut datagrams = Datagrams::new();

        let received = receiver
            .receive_batch(&mut buffers, &mut datagrams)
            .expect("a receive");

        assert_eq!(2, received);
        assert_eq!(3, datagrams.as_slice()[0].length);
        assert_eq!(3, datagrams.as_slice()[1].length);
        assert_eq!(b"one", &buffers[0][..3]);
        assert_eq!(b"two", &buffers[1][..3]);
        assert_eq!(
            Some(local),
            datagrams.as_slice()[0].source.map(|source| {
                // The sender's port is its own; only the address is known.
                SocketAddr::new(source.ip(), local.port())
            })
        );
    }

    #[test]
    fn a_connected_socket_sends_and_receives_without_an_address() {
        let receiver = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        receiver
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        receiver.set_nonblocking().expect("non-blocking");

        let local = receiver.local_address().expect("a bound address");

        let sender = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        sender.connect(local).expect("a connect");
        sender.set_nonblocking().expect("non-blocking");
        assert_eq!(3, sender.send(b"abc").expect("a send"));

        let mut buffers = vec![vec![0u8; 64]];
        let mut datagrams = Datagrams::new();
        receiver
            .receive_batch(&mut buffers, &mut datagrams)
            .expect("a receive");

        assert_eq!(b"abc", &buffers[0][..3]);
        assert_eq!(
            Some(local),
            datagrams.as_slice()[0]
                .source
                .map(|source| SocketAddr::new(source.ip(), local.port())),
            "the sender's source port is the one the kernel gave it"
        );
    }

    #[test]
    fn a_batch_stops_at_what_the_kernel_took() {
        // A non-blocking socket with a full send buffer reports fewer datagrams
        // than it was asked for — which is the number the position advances by.
        let receiver = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        receiver
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");

        let local = receiver.local_address().expect("a bound address");
        let sender = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        sender.set_nonblocking().expect("non-blocking");

        let payload = vec![0u8; 64 * 1024];
        let buffers: Vec<&[u8]> = (0..4).map(|_| payload.as_slice()).collect();

        // A datagram larger than the path's MTU is refused, and the count says
        // nothing was sent rather than four things were.
        let result = sender.send_batch(Some(local), &buffers);

        assert!(
            result.is_err() || result.expect("a send") <= 4,
            "the kernel decides how many datagrams it took"
        );
    }

    #[test]
    fn a_six_socket_binds_and_sends_to_itself() {
        let socket = DatagramSocket::open(AddressFamily::Inet6).expect("a socket");
        socket
            .bind("[::1]:0".parse().expect("an address"))
            .expect("a bind");
        socket.set_nonblocking().expect("non-blocking");

        let local = socket.local_address().expect("a bound address");
        assert!(matches!(local, SocketAddr::V6(_)));

        let peer = DatagramSocket::open(AddressFamily::Inet6).expect("a socket");
        peer.connect(local).expect("a connect");
        assert_eq!(2, peer.send(b"hi").expect("a send"));

        let mut buffers = vec![vec![0u8; 64]];
        let mut datagrams = Datagrams::new();
        assert_eq!(
            1,
            socket
                .receive_batch(&mut buffers, &mut datagrams)
                .expect("a receive")
        );
        assert_eq!(b"hi", &buffers[0][..2]);
        assert!(
            datagrams.as_slice()[0]
                .source
                .is_some_and(|source| matches!(source, SocketAddr::V6(_))),
            "a v6 datagram has a v6 source"
        );
    }

    #[test]
    fn the_receive_buffer_size_is_the_kernels_answer() {
        let socket = DatagramSocket::open(AddressFamily::Inet).expect("a socket");

        let size = socket.receive_buffer_size().expect("an answer");

        assert!(size > 0, "a kernel hands every socket a buffer");
    }
}

/// A descriptor the poller can watch: a socket, as the kernel names it.
///
/// Opaque on purpose — nothing outside this module has any business with the
/// number, and the reference's poller keeps the same distinction (it holds
/// transports, and asks them for their `recv_fd`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Descriptor(libc::c_int);

impl DatagramSocket {
    /// The descriptor to register with a [`Poller`].
    pub const fn descriptor(&self) -> Descriptor {
        Descriptor(self.fd)
    }
}

/// An `epoll` instance over datagram sockets — the reference's
/// `aeron_udp_transport_poller` for the half that needs readiness rather than a
/// syscall per socket (`media/aeron_udp_transport_poller.c`).
///
/// One call per pass asks the kernel **which** sockets have something, and only
/// those are read. Below the transport-count threshold the reference does not
/// bother — it calls `recvmmsg` on each in turn (`:190-206`) — because at that
/// size the bookkeeping costs more than the syscalls it saves. The choice is the
/// caller's; this type is only the mechanism.
pub struct Poller {
    fd: libc::c_int,
    /// One `epoll_event` per registered descriptor is what the reference
    /// allocates (`:150-160`); here the events buffer is filled by
    /// [`Poller::ready`] into the caller's list.
    _private: (),
}

impl Poller {
    /// A new `epoll` instance.
    ///
    /// # Errors
    ///
    /// The error from `epoll_create1(2)`.
    pub fn new() -> io::Result<Self> {
        // SAFETY: `epoll_create1` takes flags and returns a descriptor or -1;
        // `EPOLL_CLOEXEC` is the only flag this build wants, so a driver that
        // execs something does not leak its poller.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd, _private: () })
    }

    /// Watch `descriptor` for readability.
    ///
    /// # Errors
    ///
    /// The error from `epoll_ctl(EPOLL_CTL_ADD)`.
    pub fn add(&self, descriptor: Descriptor) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: u32::try_from(libc::EPOLLIN).unwrap_or(0),
            u64: u64::try_from(descriptor.0).unwrap_or(0),
        };

        // SAFETY: the descriptor is this poller's, the event is a live local,
        // and `EPOLL_CTL_ADD` is the documented way to register one.
        let result =
            unsafe { libc::epoll_ctl(self.fd, libc::EPOLL_CTL_ADD, descriptor.0, &mut event) };
        if 0 != result {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Stop watching `descriptor`.
    ///
    /// # Errors
    ///
    /// The error from `epoll_ctl(EPOLL_CTL_DEL)`.
    pub fn remove(&self, descriptor: Descriptor) -> io::Result<()> {
        let mut event = libc::epoll_event { events: 0, u64: 0 };

        // SAFETY: as `add`, with the deletion operation.
        let result =
            unsafe { libc::epoll_ctl(self.fd, libc::EPOLL_CTL_DEL, descriptor.0, &mut event) };
        if 0 != result {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// The descriptors that have something to read, appended to `ready`. At
    /// most `max` of them are taken — the caller knows how many transports it
    /// has, and asking for more events than that is asking the kernel to fill
    /// a buffer nobody is waiting for.
    ///
    /// The timeout is **zero** — the reference's own (`:208`) — so a pass never
    /// waits: whatever is there now is what this pass reads, and the next pass
    /// asks again. `EINTR` and `EAGAIN` are "nothing this pass" rather than
    /// errors, which is also the reference's reading of them (`:211-216`).
    ///
    /// # Returns
    ///
    /// How many descriptors were appended.
    ///
    /// # Errors
    ///
    /// The error from `epoll_wait(2)`, other than the two above.
    pub fn ready(&self, max: usize, ready: &mut Vec<Descriptor>) -> io::Result<usize> {
        let capacity = max.max(1);
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; capacity];

        // SAFETY: `events` is a live buffer of exactly `capacity` entries and
        // the count matches it; the poller's descriptor is this type's.
        let result = unsafe {
            libc::epoll_wait(
                self.fd,
                events.as_mut_ptr(),
                i32::try_from(capacity).unwrap_or(i32::MAX),
                0,
            )
        };

        if result < 0 {
            let error = io::Error::last_os_error();
            if io::ErrorKind::Interrupted == error.kind()
                || io::ErrorKind::WouldBlock == error.kind()
            {
                return Ok(0);
            }

            return Err(error);
        }

        let count = usize::try_from(result).unwrap_or(0);
        for event in &events[..count] {
            if 0 != event.events & u32::try_from(libc::EPOLLIN).unwrap_or(0) {
                #[allow(clippy::cast_possible_truncation)] // a descriptor is a c_int
                ready.push(Descriptor(event.u64 as libc::c_int));
            }
        }

        Ok(count)
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        // SAFETY: the descriptor came from `epoll_create1` and is closed once.
        unsafe {
            libc::close(self.fd);
        }
    }
}
