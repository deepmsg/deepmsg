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

/// A UDP socket, owned for as long as this value lives.
#[derive(Debug)]
pub struct DatagramSocket {
    fd: libc::c_int,
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

        Ok(Self { fd })
    }

    /// Bind the socket to a local address
    /// (`aeron_udp_channel_transport.c:147-154`).
    ///
    /// # Errors
    ///
    /// The error from `bind(2)`.
    pub fn bind(&self, address: SocketAddr) -> io::Result<()> {
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
            return Err(io::Error::last_os_error());
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
        match self.local_address() {
            Ok(SocketAddr::V6(_)) => AddressFamily::Inet6,
            _ => AddressFamily::Inet,
        }
    }

    /// A `SOL_SOCKET` option whose value is a byte count.
    fn set_byte_option(&self, name: libc::c_int, bytes: usize) -> io::Result<()> {
        let value = libc::c_int::try_from(bytes).unwrap_or(libc::c_int::MAX);

        // SAFETY: a live descriptor, a `SOL_SOCKET` option name, and a live
        // `c_int` with its length.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
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
}

impl Drop for DatagramSocket {
    fn drop(&mut self) {
        // SAFETY: the descriptor was opened by `open` and is released exactly
        // once, here — no other value holds a copy.
        unsafe { libc::close(self.fd) };
    }
}

/// A `SocketAddr` as the kernel's own address type.
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
