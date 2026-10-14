//! Which implementation the UDP data paths run on, chosen once per driver.
//!
//! The reference's transport bindings are a table of function pointers
//! (`aeron_udp_channel_transport_bindings_t`,
//! `media/aeron_udp_channel_transport_bindings.h:113-134`) that the context
//! loads by name from `AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA`
//! (`aeron_driver_context.c:1258-1263`). Of its twelve slots, [`Transport`]
//! already carries the ones a transport answers for itself; what is here are
//! the two that **make** things — `init_func`, which opens a transport
//! (`:43-50`), and `poller_init_func`, which makes a thread's poller
//! (`:91-94`) — because those are the two places a driver has to know which
//! bindings it is running on.
//!
//! An `enum`, not a `dyn`: the set of bindings is fixed when the driver is
//! compiled, and the data paths are where this build counts its indirect calls
//! (the `lto = "fat"` note in the workspace's `Cargo.toml`). One variant today,
//! the kernel's sockets, which is the only name the configuration serves
//! (`crate::config`, `parse_transport_bindings`).
//!
//! **The conductor's transports do not come through here.** The reference
//! gives the conductor a bindings of its own
//! (`AERON_CONDUCTOR_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA`,
//! `aeron_driver_context.c:1266-1270`) and the name resolver is its only user
//! (`aeron_driver_name_resolver.c:399`); this build serves `default` there
//! and nothing else, so the resolver opens its socket directly
//! (`crate::name_resolver::driver`).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use super::poller::{MediaPoller, TransportPoller};
use super::udp_transport::{OpenError, UdpTransport};
use super::userspace::stack::{Stack, ThreadQueues};
use super::userspace::transport::UserspaceTransport;
use super::{Transport, TransportParams};
use crate::udp_channel::{UdpChannel, UdpChannelError};

/// The bindings a driver's UDP data paths run on.
///
/// `Debug` and `PartialEq` are written out rather than derived, because a
/// stack is neither: two bindings are the same when they are the same
/// *stack*, which is what the port table, the rings and the ports are — an
/// `Arc` and so a pointer, not a value. `Clone` and `Default` still derive,
/// and the driver compares bindings in one place: the configuration's own
/// test, which asks that the only name it serves resolves to `Default`
/// (`crate::config`).
#[derive(Clone, Default)]
pub enum TransportBindings {
    /// `default`: the kernel's sockets
    /// (`aeron_udp_channel_transport_bindings_default`,
    /// `media/aeron_udp_channel_transport_bindings.c:38-57`).
    #[default]
    Default,
    /// A userspace stack: Ethernet frames in and out of a port the caller
    /// owns, with no socket underneath.
    ///
    /// The stack is built by whoever owns the ports and handed to the driver,
    /// because a port is a thing the driver does not have — in the DPDK build
    /// it is a queue pair on an EAL that must be initialised before the driver
    /// exists at all (the design's §4.13).
    Userspace(Arc<Stack>),
}

/// What a media can answer about the addresses it holds.
///
/// The kernel's interface list is the only thing that knows which addresses a
/// machine has — except for a media whose port the driver drives itself
/// (design §4.9, Q1a). A NIC bound to vfio is not in that list at all, and a
/// port opened through `net_af_packet` is one the kernel sees and whose frames
/// this driver's own stack is the one answering for. So a channel that names
/// an `interface=` asks the media first, and what the media does not hold falls
/// through to `getifaddrs`.
///
/// A trait rather than a method on the bindings, and answered by the **stack**
/// rather than by the bindings themselves, which is the D4 plan's P-D4-4: what
/// `udp_channel` wants to know is "does the thing the driver runs on hold this
/// address", and a driver that later has its conductor resolve names through a
/// second media asks the same question of it.
pub trait InterfaceSource {
    /// The interface holding `address` under `prefix_length`, when this media
    /// is the one that holds it.
    fn interface_for(
        &self,
        family: crate::sys::AddressFamily,
        address: std::net::IpAddr,
        prefix_length: u8,
    ) -> Option<crate::sys::LocalInterface>;
}

impl InterfaceSource for TransportBindings {
    fn interface_for(
        &self,
        family: crate::sys::AddressFamily,
        address: std::net::IpAddr,
        prefix_length: u8,
    ) -> Option<crate::sys::LocalInterface> {
        match self {
            // The kernel's sockets hold nothing the kernel does not have: the
            // fallback *is* this media.
            Self::Default => None,
            Self::Userspace(stack) => stack.owns(family, address, prefix_length),
        }
    }
}

impl std::fmt::Debug for TransportBindings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "Default"),
            Self::Userspace(_) => write!(f, "Userspace(<stack>)"),
        }
    }
}

impl PartialEq for TransportBindings {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Default, Self::Default) => true,
            (Self::Userspace(one), Self::Userspace(other)) => Arc::ptr_eq(one, other),
            _ => false,
        }
    }
}

impl Eq for TransportBindings {}

/// Whether the kernel is the thing that holds `address`: loopback, or an
/// address one of its interfaces has.
///
/// A media that drives a port itself cannot reach either. Its frames go out of
/// that port and nowhere else, so a channel whose peer is an address the kernel
/// holds belongs to the kernel even when the channel named no local address of
/// its own (design §4.9, Q1a) — which is every publication in the reference's
/// system tests and this build's interop tests, all of which name `localhost`.
///
/// A lookup that could not be made is **not** an answer that the address is the
/// kernel's: the media is the one the driver was configured for, and a channel
/// the media cannot reach says so there — `ENETUNREACH`, off this link — rather
/// than being moved to a socket the configuration did not ask for.
fn the_kernel_holds(address: Ipv4Addr) -> bool {
    crate::sys::interface_for_address(
        crate::sys::AddressFamily::Inet,
        IpAddr::V4(address),
        // The whole address, so the match is the address and not the net it
        // sits in: `127.0.0.2` is not held by a machine whose loopback is
        // `127.0.0.1`, and a publication to it is not the kernel's to serve.
        32,
    )
    .is_ok_and(|held| held.is_some())
}

/// Which thread a transport or a poller will be used on
/// (`aeron_udp_channel_transport_affinity_t`,
/// `media/aeron_udp_channel_transport_bindings.h:26-32`).
///
/// The reference passes it to `init_func` and `poller_init_func`, and its
/// default bindings read it only to name it in a bind failure
/// (`media/aeron_udp_channel_transport.c:151,209,271`, which is where the
/// `affinity=0` and `affinity=1` in the endpoints' bind reports come from):
/// it is there for bindings that give each thread a queue of its own, which
/// have to know at open time whose queue a transport belongs to. The
/// discriminants are the reference's. The conductor's member, `2`, is left out
/// with the conductor's transports (module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affinity {
    /// `AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER`: a send endpoint's
    /// transport (`media/aeron_send_channel_endpoint.c:140`), and the sender's
    /// poller (`aeron_driver_sender.c:43-44`).
    Sender = 0,
    /// `AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER`: a receive
    /// destination's transport (`media/aeron_receive_destination.c:76`), and
    /// the receiver's poller (`aeron_driver_receiver.c:41-42`).
    Receiver = 1,
}

impl TransportBindings {
    /// Open a transport (`init_func`, `:43-50`), for the thread `affinity`
    /// names.
    ///
    /// **Which media it is opened on is decided here, per channel**, from the
    /// two addresses the caller has already resolved
    /// ([`Self::stack_for`]). A driver configured for the userspace media has
    /// one, and a channel whose traffic that media cannot carry runs on the
    /// kernel's sockets instead — which is what keeps a driver whose data paths
    /// are on a port it drives itself able to serve the channels that never
    /// touch it (design §4.9, Q1).
    ///
    /// # Errors
    ///
    /// [`OpenError`], with the bind failure kept apart because the endpoints
    /// compose the reference's error chain from it.
    pub fn open(
        &self,
        bind: SocketAddr,
        multicast_interface: Option<SocketAddr>,
        connect: Option<SocketAddr>,
        params: &TransportParams,
        affinity: Affinity,
    ) -> Result<Box<dyn Transport>, OpenError> {
        match self.stack_for(bind, connect) {
            Some(stack) => {
                // The transport parameters are the kernel's knobs — socket
                // buffers, the multicast interface, the TTL — and a userspace
                // stack has none of them: what it is sized by is its own
                // configuration, which the caller built the stack with. The
                // design records what each one means here rather than
                // pretending to apply it (§4.11): `so-rcvbuf` and `so-sndbuf`
                // are accepted and do nothing, and the channel is refused for
                // a multicast group before a transport is ever opened for it.
                let _ = (multicast_interface, params);
                let transport = UserspaceTransport::open(stack.clone(), bind, connect, affinity)?;
                Ok(Box::new(transport))
            }
            None => {
                // The kernel's socket, reached both by the kernel's own
                // bindings and by a userspace one whose channel is not its to
                // carry. Its `init_func`
                // (`media/aeron_udp_channel_transport.c:100-107`) reads the
                // affinity only into its bind errors, which the endpoints
                // compose themselves.
                let _ = affinity;
                let transport = UdpTransport::open(bind, multicast_interface, connect, params)?;
                Ok(Box::new(transport))
            }
        }
    }

    /// The userspace stack a channel's data paths run on, or [`None`] for the
    /// kernel's sockets (design §4.9, Q1/Q1a/Q1b).
    ///
    /// **The key is the local side of the channel**, not where its datagrams
    /// are going: the question is which of the two things this driver can open
    /// a socket *for* is the one that holds the address the socket is bound to.
    /// `bind` is therefore what an endpoint is about to bind — a receive
    /// endpoint's is the channel's `endpoint=`, a send endpoint's is its
    /// interface or the wildcard (both, and why, in `crate::udp_channel`'s
    /// module note) — and the answer is the media's own when that address falls
    /// inside the port's net.
    ///
    /// Three cases, and the third is the one the design's own wording leaves
    /// open:
    ///
    /// * **A named local address the port holds** — 10.10.0.2 on a port holding
    ///   10.10.0.1/24 — is the media's, and there is no fallback from it: a
    ///   channel that named the media's address is a channel the media must
    ///   serve, and one it cannot is a failure rather than a silent move to a
    ///   socket that has no such address.
    /// * **A named local address it does not hold** — `127.0.0.1`, another
    ///   interface's — is the **kernel's**, because the kernel is the thing
    ///   that holds it. This is what keeps every channel in the reference's
    ///   system tests and this build's interop tests working on a driver whose
    ///   data paths are on a DPDK port.
    /// * **No local address at all**: a send endpoint with no `interface=` — a
    ///   publication — binds the wildcard, and a wildcard is not an address
    ///   anybody holds. It is the **media's**, because the media is the thing
    ///   that assigns it a port and its own address, **unless the channel is
    ///   talking to an address the kernel holds** (loopback, another local
    ///   interface), which only the kernel can reach. The destination is
    ///   consulted for that one case and no other; the design's "the key is the
    ///   local side, not the peer" holds everywhere the local side was written.
    ///
    /// Multi-destination and multicast channels reach here too and are answered
    /// by the same rule: a group address is refused before a transport is
    /// opened for it ([`Self::refuse_channel`]), and a destination's own
    /// address is what its transport binds.
    fn stack_for(&self, bind: SocketAddr, connect: Option<SocketAddr>) -> Option<&Arc<Stack>> {
        let Self::Userspace(stack) = self else {
            return None;
        };

        let IpAddr::V4(address) = bind.ip() else {
            // IPv6 is refused at the channel, so this is a caller that has
            // built an address the parse would not have let through.
            return None;
        };

        if !address.is_unspecified() {
            let held = stack.owns(
                crate::sys::AddressFamily::Inet,
                IpAddr::V4(address),
                stack.params().prefix_length,
            );

            return held.is_some().then_some(stack);
        }

        match connect.map(|peer| peer.ip()) {
            Some(IpAddr::V4(peer)) if the_kernel_holds(peer) => None,
            _ => Some(stack),
        }
    }

    /// Refuse a channel this media cannot serve, where the channel is read
    /// (design §4.11).
    ///
    /// The kernel's bindings serve every shape a channel can take — multicast
    /// and IPv6 included — so this is the userspace media's list and nothing
    /// else: a **group** address needs IGMP reports and a group filter on the
    /// port, and **IPv6** needs NDP, and the userspace stack has neither. Both
    /// limits are the media's rather than the driver's, which is why the
    /// message names the media the configuration selected.
    ///
    /// **Refused rather than ignored, and that is the whole point of the
    /// list.** A multicast channel this media accepted would have the group
    /// bound as if it were one of the port's own addresses — the bind succeeds
    /// and no frame is ever filed, so a subscription looks connected and
    /// delivers nothing for as long as it is held. An error the client can read
    /// is the honest answer to a channel that cannot work.
    ///
    /// # Errors
    ///
    /// [`UdpChannelError::Unsupported`], which reaches a client as `ON_ERROR`
    /// carrying `ERROR_CODE_NOT_SUPPORTED` and the message as its text — the
    /// same shape the timestamp parameters are refused with
    /// ([`crate::udp_channel::UdpChannel::resolve`]).
    pub fn refuse_channel(&self, channel: &UdpChannel) -> Result<(), UdpChannelError> {
        let Self::Userspace(_) = self else {
            return Ok(());
        };

        if channel.is_multicast {
            return Err(UdpChannelError::Unsupported(format!(
                "the dpdk media does not serve multicast: {} is a group address",
                channel.remote_data
            )));
        }

        for address in [channel.remote_data, channel.local_data] {
            if address.is_ipv6() {
                return Err(UdpChannelError::Unsupported(format!(
                    "the dpdk media does not serve IPv6: {address}"
                )));
            }
        }

        Ok(())
    }

    /// A poller for the thread `affinity` names (`poller_init_func`,
    /// `:91-94`).
    pub fn poller(&self, affinity: Affinity) -> MediaPoller {
        match self {
            Self::Default => {
                let _ = affinity;
                MediaPoller::Kernel(TransportPoller::new())
            }
            // One per agent thread, and the two behave differently: see
            // [`ThreadQueues::pump`]. The reference passes the same affinity to
            // `poller_init_func` for the same reason.
            Self::Userspace(stack) => {
                MediaPoller::Userspace(ThreadQueues::new(stack.clone(), affinity))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::userspace::fake_port::{Side, Wire};
    use crate::media::userspace::stack::{Clock, StackParams};
    use crate::sys::AddressFamily;
    use std::net::{IpAddr, Ipv4Addr};

    /// The bindings answer what the media they name holds — and the kernel's
    /// bindings hold nothing of their own, because the kernel *is* the
    /// fallback.
    ///
    /// This is the D4 plan's P-D4-4: the question is the stack's, and
    /// `TransportBindings` only forwards it, so that a driver asks its media
    /// once and the answer it gets is the media's own.
    #[test]
    fn the_bindings_answer_with_the_medias_own_addresses() {
        let wire = Wire::new();
        let own = Ipv4Addr::new(10, 10, 0, 1);
        let stack = Stack::over_a_fake_wire(
            &wire,
            Side::A,
            StackParams::new(own, 24, [0x02, 0, 0, 0, 0, 0x01]),
            Clock::Monotonic,
        );

        assert_eq!(
            None,
            TransportBindings::Default.interface_for(
                AddressFamily::Inet,
                IpAddr::V4(Ipv4Addr::new(10, 10, 0, 1)),
                24
            ),
            "the kernel's own bindings have nothing the kernel does not have"
        );

        let userspace = TransportBindings::Userspace(stack);
        let held = userspace
            .interface_for(
                AddressFamily::Inet,
                IpAddr::V4(Ipv4Addr::new(10, 10, 0, 9)),
                24,
            )
            .expect("the stack holds its own prefix");
        assert_eq!(IpAddr::V4(own), held.address);

        assert_eq!(
            None,
            userspace.interface_for(
                AddressFamily::Inet,
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
                24
            ),
            "and nothing else, so that the kernel still answers for the rest"
        );
    }

    fn channel(uri: &str) -> UdpChannel {
        let parsed = crate::channel_uri::ChannelUri::parse(uri.as_bytes()).expect("a URI");

        UdpChannel::resolve(uri.as_bytes(), &parsed)
            .unwrap_or_else(|error| panic!("{uri}: {error}"))
    }

    fn userspace() -> TransportBindings {
        let wire = Wire::new();
        TransportBindings::Userspace(Stack::over_a_fake_wire(
            &wire,
            Side::A,
            StackParams::new(Ipv4Addr::new(10, 10, 0, 1), 24, [0x02, 0, 0, 0, 0, 0x01]),
            Clock::Monotonic,
        ))
    }

    fn address(text: &str) -> SocketAddr {
        text.parse().expect("an address")
    }

    /// The media a channel's data paths run on, which is the local side's
    /// answer and not the peer's (design §4.9, Q1a).
    ///
    /// The stack here holds `10.10.0.1/24`, which is the shape a driver whose
    /// port is a DPDK one has: the kernel does not have that address at all, so
    /// nothing but this media can bind it.
    #[test]
    fn a_channels_local_side_decides_which_media_carries_it() {
        let bindings = userspace();
        let serves = |bind: &str, peer: Option<&str>| {
            bindings
                .stack_for(address(bind), peer.map(address))
                .is_some()
        };

        // A receive endpoint binds the endpoint its channel named, and one in
        // the port's net is the media's — there is no fallback from it.
        assert!(serves("10.10.0.2:40500", None), "the port's own net");
        assert!(
            serves("10.10.0.9:40500", None),
            "anywhere in the port's net"
        );

        // A named address the media does not hold belongs to the kernel, which
        // is the thing that does hold it — this is every channel in the
        // system tests that names loopback.
        assert!(!serves("127.0.0.1:40456", None), "loopback is the kernel's");
        assert!(
            !serves("192.168.1.5:40456", None),
            "and so is another interface's address"
        );

        // No local address named: a send endpoint binds the wildcard, which is
        // the media's — the peer decides only when the kernel is the only one
        // that can reach it.
        assert!(
            serves("0.0.0.0:0", Some("10.10.0.2:40500")),
            "a publication to the port's own net is the media's"
        );
        assert!(
            serves("0.0.0.0:0", Some("192.0.2.5:40456")),
            "and so is one to an address nobody holds at all — off this link, \
             which the media is then the one to refuse (`ENETUNREACH`)"
        );
        assert!(
            !serves("0.0.0.0:0", Some("127.0.0.1:40456")),
            "but a publication to loopback is the kernel's, which is what every \
             channel in the system tests names"
        );
        assert!(
            serves("0.0.0.0:0", None),
            "and with no peer named at all there is nothing the kernel holds"
        );
    }

    /// The predicate itself, pinned against the kernel's own answer rather than
    /// by naming an interface: loopback is held by every machine, and an
    /// address from the range reserved for documentation
    /// (`192.0.2.0/24`, RFC 5737) is held by none — which is the pair that
    /// matters, because "another interface's address" is not portable between
    /// machines and is the same lookup answering yes.
    #[test]
    fn the_kernel_holds_loopback_and_not_a_reserved_address() {
        assert!(the_kernel_holds(Ipv4Addr::LOCALHOST));
        assert!(!the_kernel_holds(Ipv4Addr::new(192, 0, 2, 5)));
    }

    /// The kernel's own bindings carry everything, which is what makes the
    /// fallback a fallback rather than a second media.
    #[test]
    fn the_kernels_bindings_carry_every_channel() {
        let bindings = TransportBindings::Default;

        assert!(
            bindings
                .stack_for(address("127.0.0.1:40456"), Some(address("127.0.0.1:40457")))
                .is_none()
        );
        assert!(
            bindings
                .stack_for(address("10.10.0.2:40500"), None)
                .is_none()
        );
    }

    /// A multicast channel is refused by the media that cannot serve one, and
    /// served by the one that can (design §4.11, Q1b).
    ///
    /// The message is asserted and not just the refusal: a client that is told
    /// *which* media refused it can tell a deployment mistake from a channel
    /// this build never serves, and the two would otherwise be one opaque
    /// "not supported".
    #[test]
    fn a_group_address_is_refused_by_the_media_that_has_no_groups() {
        let group = channel("aeron:udp?endpoint=224.0.1.1:40456");

        assert!(
            TransportBindings::Default.refuse_channel(&group).is_ok(),
            "the kernel's sockets serve multicast, and always have"
        );

        let refusal = userspace()
            .refuse_channel(&group)
            .expect_err("the userspace media holds no groups");

        assert_eq!(
            "the dpdk media does not serve multicast: 224.0.1.1:40456 is a group address",
            refusal.to_string()
        );
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_NOT_SUPPORTED,
            refusal.error_code()
        );
    }

    /// The same for an IPv6 channel, which needs NDP.
    #[test]
    fn an_ipv6_channel_is_refused_by_the_media_that_has_no_ndp() {
        let channel = channel("aeron:udp?endpoint=[::1]:40456");

        assert!(
            TransportBindings::Default.refuse_channel(&channel).is_ok(),
            "the kernel's sockets serve IPv6"
        );

        let refusal = userspace()
            .refuse_channel(&channel)
            .expect_err("the userspace media speaks IPv4 only");

        assert_eq!(
            "the dpdk media does not serve IPv6: [::1]:40456",
            refusal.to_string()
        );
    }

    /// And a unicast IPv4 channel is served by both, which is what keeps the
    /// refusals from being a blanket one.
    #[test]
    fn a_unicast_channel_is_served_by_both_media() {
        let channel = channel("aeron:udp?endpoint=10.10.0.2:40500");

        assert!(TransportBindings::Default.refuse_channel(&channel).is_ok());
        assert!(userspace().refuse_channel(&channel).is_ok());
    }
}
