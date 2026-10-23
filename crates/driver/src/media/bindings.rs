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
//! (the `lto = "fat"` note in the workspace's `Cargo.toml`). Two variants: the
//! kernel's sockets, which is what `default` names, and the userspace stack a
//! build with the `dpdk` feature runs `dpdk` on (`crate::config`,
//! `parse_media`). Which of the two a **channel** runs on is decided per
//! channel, in [`TransportBindings::stack_for`].
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
use deepmsg_cnc::layout;
use deepmsg_cnc::{CounterManager, CounterRegions};

use super::{Transport, TransportParams};
use crate::config::IdleStrategySetting;
use crate::idle::Strategy;
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

/// Whether a frame for `address` can **only** be delivered by the kernel:
/// loopback, and nothing else.
///
/// A media that drives a port itself puts its frames on that port and nowhere
/// else, so a channel whose peer is loopback is one the media cannot serve: a
/// datagram for `127.0.0.1` never reaches a wire. That is the whole of the
/// fallback for a channel that named no local address of its own, and it is
/// what keeps every publication in the reference's system tests and this
/// build's interop tests — all of which name `localhost` — working on a driver
/// whose data paths are on a port it drives (design §4.9, Q1a).
///
/// **It is deliberately not "an address one of the machine's interfaces
/// has".** That was the first reading, and it makes the answer depend on what
/// the machine happens to have configured: a peer that is a *routable* address
/// the kernel also holds — the reference driver's own address, when both ends
/// are ports of one machine, which is exactly the shape a cross-media test has
/// — would be handed to the kernel, and the frames a publication puts on a
/// wire would never reach the media at all. A routable address is one the
/// media must carry, because carrying it is what putting frames on a wire
/// means. The kernel is the fallback for what the wire cannot do.
///
/// Nothing is asked of the interface list, so the answer is the same on every
/// machine — and a channel naming an address the media cannot reach says so
/// there, as `ENETUNREACH`, rather than being moved to a socket the
/// configuration did not ask for.
fn only_the_kernel_can_reach(address: Ipv4Addr) -> bool {
    address.is_loopback()
}

/// Whether an agent configured with this strategy stays on its core.
///
/// Two of the reference's five do: `noop` returns at once and `spin` reads the
/// CPU until it is told to stop. The other three (`yield`, `backoff`,
/// `sleep-ns`) hand the core back, which is what a port this driver drives
/// itself cannot afford — there is no interrupt to be woken by.
fn keeps_the_cpu(setting: &IdleStrategySetting) -> bool {
    matches!(
        setting.strategy(),
        Some(Strategy::Noop | Strategy::BusySpinning)
    )
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
        match self.stack_for(bind, connect, params.control) {
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

    /// Refuse a publication whose frames are larger than this media can carry
    /// (design §4.8, §4.11).
    ///
    /// The userspace stack does **not fragment**: a frame the port cannot hold
    /// is one no send will ever complete, so a publication that asks for one is
    /// refused where it is opened rather than silently truncated — a truncated
    /// frame is a message the receiver reassembles into something else.
    ///
    /// The kernel's sockets are not asked. They fragment, their limit is the
    /// route's rather than this driver's, and the reference does not pre-check
    /// it either — so the fallback answers `Ok` and the kernel's own `EMSGSIZE`
    /// is what a publication that overshoots meets there.
    ///
    /// **Asked of the media that would carry the channel and of no other**,
    /// which is why the two addresses that decide that ([`Self::stack_for`])
    /// are arguments: a channel whose local side is not on this port is the
    /// kernel's, and the size its datagrams may have is the kernel's too.
    /// Without that, a publication to a loopback endpoint with an `mtu=` the
    /// port happens to be too small for would be refused with a reason about a
    /// port it was never going to use.
    ///
    /// # Errors
    ///
    /// The message a client reads, in the shape the reference's own MTU
    /// refusals use ([`crate::channel_validation`]).
    pub fn refuse_mtu_over_what_it_can_carry(
        &self,
        bind: SocketAddr,
        peer: Option<SocketAddr>,
        control: Option<SocketAddr>,
        mtu_length: usize,
    ) -> Result<(), String> {
        let Some(stack) = self.stack_for(bind, peer, control) else {
            return Ok(());
        };

        let largest = stack.largest_datagram();

        if mtu_length > largest {
            return Err(format!(
                "mtuLength={mtu_length} > {largest}, the largest frame the dpdk media's port \
                 can carry (its MTU less the 42 bytes of Ethernet, IPv4 and UDP headers): \
                 this stack does not fragment"
            ));
        }

        Ok(())
    }

    /// Refuse a channel whose datagrams this media would carry and cannot
    /// deliver (design §4.11): a destination off the port's own link with no
    /// gateway configured.
    ///
    /// Asked of the media that would carry the channel and of no other, which
    /// is why the two addresses that decide *that* ([`Self::stack_for`]) are
    /// arguments: a channel the kernel serves is the kernel's to reach or not,
    /// and the reference's own answer there is the route's.
    ///
    /// Without it a publication to an address the media cannot reach opens
    /// happily and then fails every send with `ENETUNREACH` — a client polling
    /// a publication that is never going to publish anything.
    ///
    /// # Errors
    ///
    /// The message a client reads. The reference's code for a destination it
    /// cannot reach is the errno it fails the `sendto` with, `ENETUNREACH`,
    /// which composes into the generic code at the client.
    pub fn refuse_a_destination_it_cannot_reach(
        &self,
        bind: SocketAddr,
        peer: Option<SocketAddr>,
        control: Option<SocketAddr>,
        destination: SocketAddr,
    ) -> Result<(), String> {
        let Some(stack) = self.stack_for(bind, peer, control) else {
            return Ok(());
        };

        let IpAddr::V4(address) = destination.ip() else {
            // IPv6 is refused at the channel.
            return Ok(());
        };

        // **An unspecified destination names no address to reach.** It is what
        // an MDC channel has until it learns one — the destination arrives with
        // the stream, and the conversation that finds it goes to the control
        // address — so the media has nothing to say about it here. Measured on
        // the reference's own `ChannelValidationTest` and `MultipathTest`, which
        // publish to `endpoint=0.0.0.0` and were refused with `ENETUNREACH` for
        // a destination nobody was going to send to.
        if address.is_unspecified() {
            return Ok(());
        }

        if stack.next_hop(address).is_some() {
            return Ok(());
        }

        Err(format!(
            "the dpdk media cannot reach {address}: it is not on this port's link \
             ({}/{}) and no gateway is configured (ENETUNREACH)",
            stack.params().address,
            stack.params().prefix_length
        ))
    }

    /// The counter that says what one transport has received (design §4.10,
    /// type 76), or [`layout::NULL_COUNTER_ID`] for a media that publishes none.
    ///
    /// The type is the DPDK media's own — 75 to 95 are
    /// (`aeron-client/src/main/c/aeron_counters.h:155-173`) — and a driver whose
    /// data paths are the kernel's has no such counter, which is why the answer
    /// there is the null id rather than a counter nobody would read: what a
    /// kernel transport has received is already counted by the reference's own
    /// `rcv-local-sockaddr` and the channel status beside it.
    ///
    /// `local` is the address the transport is **bound** to, which is why this
    /// is asked after the bind: a channel that named port zero has no other
    /// place to say which port it got. A transport whose address cannot be read
    /// gets no counter and no complaint — one diagnostic number is not worth
    /// refusing a channel that is otherwise up.
    pub fn transport_counter(
        &self,
        transport: &dyn Transport,
        peer: Option<SocketAddr>,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> i32 {
        let Self::Userspace(_) = self else {
            return layout::NULL_COUNTER_ID;
        };

        let Ok(local) = transport.local_address() else {
            return layout::NULL_COUNTER_ID;
        };

        super::userspace::dpdk_counters::allocate_transport_counter(
            manager, regions, local, peer, now_ms,
        )
        .unwrap_or(layout::NULL_COUNTER_ID)
    }

    /// The line a driver on this media owes an operator when one of its two
    /// network agents is configured to give up the CPU (design §4.12).
    ///
    /// A port this media drives has no interrupt to wake a thread with: a frame
    /// that arrives while the receiver sleeps waits in the receive ring, and a
    /// ring that fills drops what a sleeping thread was not there to take. So
    /// an idle that is neither `noop` nor `spin` is a counter that will climb
    /// for a reason nothing else in the driver reports — worth a line, not a
    /// refusal, because the reference lets a caller choose its own strategy and
    /// an idle is a legitimate thing to want on a quiet driver.
    ///
    /// **The conductor's idle is not asked about**: it drives no port, and the
    /// native resource agent's is the sleeping one by design
    /// ([`IdleStrategySetting::sleeping_default`]) — the two slots whose
    /// defaults would make this fire on every start are the two that never
    /// touch a queue.
    ///
    /// [`None`] for the kernel's bindings, whose sockets are woken by the
    /// kernel, and for two strategies that keep the CPU.
    pub fn idle_warning(
        &self,
        sender: &IdleStrategySetting,
        receiver: &IdleStrategySetting,
    ) -> Option<String> {
        let Self::Userspace(_) = self else {
            return None;
        };

        let sleeping = [("sender", sender), ("receiver", receiver)]
            .into_iter()
            .filter(|(_, setting)| !keeps_the_cpu(setting))
            .map(|(agent, setting)| format!("the {agent}'s (`{}`)", setting.name))
            .collect::<Vec<_>>();

        if sleeping.is_empty() {
            return None;
        }

        Some(format!(
            "{} on the dpdk media: a sleeping agent is not there to take the frames the \
             port's receive ring is holding, and a ring that fills drops them — `noop` or \
             `spin` is what a port this driver drives itself wants (§4.12)",
            sleeping.join(" and ")
        ))
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
    ///   anybody holds. It is the **media's** — the media is the thing that
    ///   assigns it a port and its own address — **when the channel is talking
    ///   to something a frame on a wire can reach**. What it is talking to is
    ///   its destination **and its control address** — an MDC channel has no
    ///   destination yet and does all of its talking to `control=`, so a
    ///   control address of loopback makes the channel the kernel's just as a
    ///   loopback peer does. Those two are consulted for that one case and no
    ///   other; the design's "the key is the local side, not the peer" holds
    ///   everywhere the local side was written ([`only_the_kernel_can_reach`]
    ///   has why the fallback is loopback and not "any address this machine
    ///   has").
    /// * **Neither, which is what the manual control mode is**: a
    ///   `control-mode=manual` channel names **no address at all** — no
    ///   endpoint, no control, no tag — and writes the wildcard into all three
    ///   slots, its destinations arriving later. **An unspecified address is
    ///   not a name**, so this rule has been told nothing, and the channel is
    ///   the **kernel's**.
    ///
    ///   **Why the kernel, and why decided here rather than when a destination
    ///   appears.** Ownership is settled when the channel is opened (P-D4): a
    ///   manual channel's destinations do not exist yet, so a rule that read
    ///   them would not be this rule. And the media is asked about
    ///   *addresses* — a channel that has named none has told it nothing, so
    ///   answering "the media" claims for the port a channel that named no
    ///   address the port could ever be asked to carry. The price is written
    ///   down rather than hidden: **a manual channel whose destinations are the
    ///   port's own runs on the kernel and cannot reach them**, and one that
    ///   wants the media has to name what it is talking to.
    ///
    ///   The reference resolves the other way — it builds no transport at all
    ///   for a manual subscription and creates one per destination as each is
    ///   added (`aeron_driver_conductor.c:2099`, then `:5903`) — and its own
    ///   manual channels talk to `localhost`
    ///   (`aeron-system-tests/src/test/java/io/aeron/MultipathTest.java:41-43`),
    ///   which is the kernel's either way. Following it there would move
    ///   ownership out of the open and into the data path, which is the trade
    ///   this build declined (D5 record, 第 10 笔).
    ///
    /// Multi-destination and multicast channels reach here too and are answered
    /// by the same rule: a group address is refused before a transport is
    /// opened for it ([`Self::refuse_channel`]), and a destination's own
    /// address is what its transport binds.
    fn stack_for(
        &self,
        bind: SocketAddr,
        connect: Option<SocketAddr>,
        control: Option<SocketAddr>,
    ) -> Option<&Arc<Stack>> {
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

        // **An unspecified address is not a name.** A channel that names
        // nothing writes the wildcard where an address would go — the manual
        // control mode does it in every slot — so the two are told apart here,
        // once, rather than by whichever caller happened to fill the slot.
        let named = [connect, control]
            .into_iter()
            .flatten()
            .filter(|address| !address.ip().is_unspecified())
            .collect::<Vec<_>>();

        // **Nothing named is the kernel's** (the third case in this function's
        // note): there is no address here for the media to have been asked
        // about.
        if named.is_empty() {
            return None;
        }

        let unreachable = named.iter().any(
            |address| matches!(address.ip(), IpAddr::V4(peer) if only_the_kernel_can_reach(peer)),
        );

        if unreachable {
            return None;
        }

        Some(stack)
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
                .stack_for(address(bind), peer.map(address), None)
                .is_some()
        };

        // A receive endpoint binds the endpoint its channel named, and one in
        // the port's net is the media's — there is no fallback from it.
        assert!(serves("10.10.0.2:10500", None), "the port's own net");
        assert!(
            serves("10.10.0.9:10500", None),
            "anywhere in the port's net"
        );

        // A named address the media does not hold belongs to the kernel, which
        // is the thing that does hold it — this is every channel in the
        // system tests that names loopback.
        assert!(!serves("127.0.0.1:10456", None), "loopback is the kernel's");
        assert!(
            !serves("192.168.1.5:10456", None),
            "and so is another interface's address"
        );

        // No local address named: a send endpoint binds the wildcard, which is
        // the media's — the peer decides only when a frame on a wire could not
        // reach it at all, which is loopback and nothing else.
        assert!(
            serves("0.0.0.0:0", Some("10.10.0.2:10500")),
            "a publication to the port's own net is the media's"
        );
        assert!(
            serves("0.0.0.0:0", Some("192.0.2.5:10456")),
            "and so is one to an address nobody holds at all — off this link, \
             which the media is then the one to refuse (`ENETUNREACH`)"
        );
        assert!(
            serves("0.0.0.0:0", Some("10.10.1.7:10456")),
            "and one to a routable address this machine may hold itself, which \
             is what a cross-media test's peer is: the answer does not depend \
             on what the machine has configured"
        );
        assert!(
            !serves("0.0.0.0:0", Some("127.0.0.1:10456")),
            "but a publication to loopback is the kernel's, which is what every \
             channel in the system tests names"
        );
        assert!(
            !serves("0.0.0.0:0", None),
            "and a channel that names nothing at all is the kernel's: the media \
             is asked about addresses, ownership is settled at the open, and \
             there is no address here to settle it with"
        );
        assert!(
            !serves("0.0.0.0:0", Some("0.0.0.0:0")),
            "which is exactly what a manual channel looks like to this rule — \
             the wildcard written where an address would go is not a name"
        );
    }

    /// **A `control-mode=manual` channel is the kernel's**, and this is the case
    /// the reference's own system tests drove out.
    ///
    /// It is the one shape that names no address *anywhere*: no `endpoint=`, no
    /// `control=`, no tag, and its destinations arrive later through
    /// `addDestination` — so there is nothing at the open for this rule to read.
    /// The reference answers it at each destination instead
    /// (`aeron_driver_conductor.c:2099`, `:5903`); this build answers it here and
    /// takes the consequence, which the second half of this test writes down: a
    /// manual channel's destinations are reached over the kernel, so the ones
    /// the reference's own tests add (`localhost`) work and the ones on a port
    /// this driver drives do not.
    #[test]
    fn a_channel_that_names_no_address_is_the_kernels() {
        let bindings = userspace();

        // Every slot the wildcard can be written into, named as the parse would
        // leave them for `aeron:udp?control-mode=manual`.
        assert!(
            bindings
                .stack_for(address("0.0.0.0:0"), Some(address("0.0.0.0:0")), None)
                .is_none(),
            "the manual publication's shape: wildcard bind, wildcard control"
        );
        assert!(
            bindings
                .stack_for(address("0.0.0.0:0"), None, Some(address("0.0.0.0:0")))
                .is_none(),
            "and the same channel read through the control slot"
        );

        // What still decides: a channel that named the port's own peer, or a
        // control address a frame can reach. Those are the cases above; this is
        // the one line that says the wildcard itself decides nothing.
        assert!(
            bindings
                .stack_for(address("0.0.0.0:0"), Some(address("10.10.0.2:10500")), None)
                .is_some(),
            "a named destination that is not loopback is still the media's"
        );
    }

    /// An **MDC channel** is decided by the address it will talk to, which is
    /// its control address rather than a destination it has not learned yet.
    ///
    /// This is the case the reference's own system tests found (the D5 record's
    /// 第 6 笔): `control=localhost:7779|control-mode=dynamic` names no local
    /// address and no destination, so the rule above called it the media's —
    /// and a port of this driver's own can never reach loopback, so the channel
    /// never connected to anything. What is asserted here is the pair that
    /// tells the two apart: a **loopback** control address is the kernel's, and
    /// a **routable** one stays the media's, because MDC over a port this
    /// driver drives is a deployment the media is supposed to serve.
    #[test]
    fn a_channel_with_no_destination_is_decided_by_its_control_address() {
        let bindings = userspace();
        let with_control = |control: &str| {
            bindings
                .stack_for(address("0.0.0.0:0"), None, Some(address(control)))
                .is_some()
        };

        assert!(
            !with_control("127.0.0.1:7779"),
            "an MDC channel whose control address is loopback is the kernel's:              nothing a frame on a wire does reaches 127.0.0.1"
        );
        assert!(
            with_control("10.10.0.2:7779"),
            "but one whose control address is routable is the media's, which is              what MDC over this port is for"
        );
        assert!(
            with_control("192.0.2.5:7779"),
            "and an address nobody holds is still the media's to reach or to              refuse — off this link, with `ENETUNREACH`"
        );
    }

    /// A frame larger than the port can carry is refused, because this stack
    /// does not fragment (design §4.8, §4.11).
    ///
    /// The stack here is built with a 1472-byte frame, which is what a
    /// 1500-byte Ethernet port answers once the 42 bytes of headers come off —
    /// and the two numbers either side of it are the ones that matter: the
    /// reference's own default (1408) fits, and a jumbo frame does not.
    #[test]
    fn a_frame_larger_than_the_port_can_carry_is_refused() {
        let mut params =
            StackParams::new(Ipv4Addr::new(10, 10, 0, 1), 24, [0x02, 0, 0, 0, 0, 0x01]);
        params.mtu = 1472;

        let wire = Wire::new();
        let bindings = TransportBindings::Userspace(Stack::over_a_fake_wire(
            &wire,
            Side::A,
            params,
            Clock::Monotonic,
        ));

        // A publication to the port's own net, which is the media's to carry.
        let ours = address("0.0.0.0:0");
        let peer = address("10.10.0.2:10500");

        assert_eq!(
            Ok(()),
            bindings.refuse_mtu_over_what_it_can_carry(ours, Some(peer), None, 1408)
        );
        assert_eq!(
            Ok(()),
            bindings.refuse_mtu_over_what_it_can_carry(ours, Some(peer), None, 1472),
            "the largest one fits exactly"
        );

        let refusal = bindings
            .refuse_mtu_over_what_it_can_carry(ours, Some(peer), None, 1473)
            .expect_err("one byte over is over");
        assert!(
            refusal.contains("mtuLength=1473 > 1472") && refusal.contains("does not fragment"),
            "{refusal}"
        );

        // A publication to loopback is the kernel's, and so is the size its
        // datagrams may be: the port this media drives is not carrying it, and
        // a reason about that port's MTU would be about the wrong thing.
        assert_eq!(
            Ok(()),
            bindings.refuse_mtu_over_what_it_can_carry(
                ours,
                Some(address("127.0.0.1:10456")),
                None,
                8192
            ),
            "the kernel fragments, and this channel never reaches the port"
        );

        assert_eq!(
            Ok(()),
            TransportBindings::Default.refuse_mtu_over_what_it_can_carry(
                ours,
                Some(peer),
                None,
                usize::MAX
            ),
            "and the kernel's bindings are not asked at all"
        );
    }

    /// A destination this media cannot reach is refused where the publication
    /// is opened (design §4.11), and one the kernel is serving is not this
    /// media's to refuse.
    #[test]
    fn a_destination_off_the_link_with_no_gateway_is_refused() {
        let bindings = userspace();
        let ours = address("0.0.0.0:0");
        let peer = address("192.0.2.5:10456");

        let refusal = bindings
            .refuse_a_destination_it_cannot_reach(ours, Some(peer), None, peer)
            .expect_err("off this link and no gateway");
        assert!(
            refusal.contains("cannot reach 192.0.2.5") && refusal.contains("ENETUNREACH"),
            "{refusal}"
        );

        // On the port's own link, and through a gateway, are both reachable.
        assert_eq!(
            Ok(()),
            bindings.refuse_a_destination_it_cannot_reach(
                ours,
                Some(address("10.10.0.2:10500")),
                None,
                address("10.10.0.2:10500")
            )
        );

        // **A destination of `0.0.0.0` is not a destination.** It is what an MDC
        // channel has until the stream tells it otherwise, so there is nothing
        // here for the media to size up — and refusing it was a real defect:
        // measured on the reference's own `ChannelValidationTest` and
        // `MultipathTest`, whose channels publish to `endpoint=0.0.0.0` and were
        // answered `ENETUNREACH` for an address nobody was going to send to
        // (the D5 record's 第 6 笔).
        assert_eq!(
            Ok(()),
            bindings.refuse_a_destination_it_cannot_reach(
                ours,
                Some(peer),
                None,
                address("0.0.0.0:0")
            ),
            "an unspecified destination names no address to reach"
        );

        let mut params =
            StackParams::new(Ipv4Addr::new(10, 10, 0, 1), 24, [0x02, 0, 0, 0, 0, 0x01]);
        params.gateway = Some(Ipv4Addr::new(10, 10, 0, 254));

        let wire = Wire::new();
        let with_gateway = TransportBindings::Userspace(Stack::over_a_fake_wire(
            &wire,
            Side::A,
            params,
            Clock::Monotonic,
        ));
        assert_eq!(
            Ok(()),
            with_gateway.refuse_a_destination_it_cannot_reach(ours, Some(peer), None, peer),
            "a gateway is a next hop for anywhere"
        );

        // The kernel's channel: the media is not carrying it, so the answer is
        // the route's and not this media's.
        assert_eq!(
            Ok(()),
            bindings.refuse_a_destination_it_cannot_reach(
                ours,
                Some(address("127.0.0.1:10456")),
                None,
                address("127.0.0.1:10456")
            ),
            "loopback is the kernel's to reach"
        );
        assert_eq!(
            Ok(()),
            TransportBindings::Default.refuse_a_destination_it_cannot_reach(
                ours,
                Some(peer),
                None,
                peer
            )
        );
    }

    /// The fallback predicate: loopback is the kernel's and nothing else is,
    /// and the test says so without asking any machine anything — which is the
    /// point of the rule rather than a property of this test.
    ///
    /// An address from the range reserved for documentation (`192.0.2.0/24`,
    /// RFC 5737) is the one that says "a routable address is the media's"
    /// without depending on what this machine has configured: whether the
    /// kernel *also* holds it is no longer the question, and the third address
    /// here — a port's own address on a machine that has one — is the case
    /// that made the difference (a cross-media test's peer is the other end of
    /// the same machine).
    #[test]
    fn only_loopback_cannot_be_reached_over_a_wire() {
        assert!(only_the_kernel_can_reach(Ipv4Addr::LOCALHOST));
        assert!(
            only_the_kernel_can_reach(Ipv4Addr::new(127, 0, 0, 2)),
            "the whole of 127/8 is loopback, and a frame for any of it stays \
             inside the machine"
        );

        for routable in [
            // Reserved for documentation: no machine holds it, and it is still
            // the media's — a channel naming it says the media is what could
            // reach it, or `ENETUNREACH` if it cannot.
            Ipv4Addr::new(192, 0, 2, 5),
            // And one a machine may well hold itself, which is what the rule
            // had to stop asking about.
            Ipv4Addr::new(10, 10, 0, 2),
        ] {
            assert!(
                !only_the_kernel_can_reach(routable),
                "{routable} is a frame on a wire's to carry"
            );
        }
    }

    /// The kernel's own bindings carry everything, which is what makes the
    /// fallback a fallback rather than a second media.
    #[test]
    fn the_kernels_bindings_carry_every_channel() {
        let bindings = TransportBindings::Default;

        assert!(
            bindings
                .stack_for(
                    address("127.0.0.1:10456"),
                    Some(address("127.0.0.1:10457")),
                    None,
                )
                .is_none()
        );
        assert!(
            bindings
                .stack_for(address("10.10.0.2:10500"), None, None)
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
        let group = channel("aeron:udp?endpoint=224.0.1.1:10456");

        assert!(
            TransportBindings::Default.refuse_channel(&group).is_ok(),
            "the kernel's sockets serve multicast, and always have"
        );

        let refusal = userspace()
            .refuse_channel(&group)
            .expect_err("the userspace media holds no groups");

        assert_eq!(
            "the dpdk media does not serve multicast: 224.0.1.1:10456 is a group address",
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
        let channel = channel("aeron:udp?endpoint=[::1]:10456");

        assert!(
            TransportBindings::Default.refuse_channel(&channel).is_ok(),
            "the kernel's sockets serve IPv6"
        );

        let refusal = userspace()
            .refuse_channel(&channel)
            .expect_err("the userspace media speaks IPv4 only");

        assert_eq!(
            "the dpdk media does not serve IPv6: [::1]:10456",
            refusal.to_string()
        );
    }

    /// And a unicast IPv4 channel is served by both, which is what keeps the
    /// refusals from being a blanket one.
    #[test]
    fn a_unicast_channel_is_served_by_both_media() {
        let channel = channel("aeron:udp?endpoint=10.10.0.2:10500");

        assert!(TransportBindings::Default.refuse_channel(&channel).is_ok());
        assert!(userspace().refuse_channel(&channel).is_ok());
    }

    fn idle(name: &str) -> IdleStrategySetting {
        IdleStrategySetting {
            name: name.to_owned(),
            init_args: None,
        }
    }

    /// A strategy that gives the CPU back is worth a line on the media that
    /// cannot be woken by an interrupt — and only there.
    ///
    /// The three that sleep are the warning; `noop` and `spin` are not, which
    /// is the half that would be missing if this were written the other way
    /// round (a warning on every start is a warning nobody reads). The
    /// kernel's bindings never warn, because their sockets are the kernel's to
    /// wake.
    #[test]
    fn a_sleeping_agent_is_worth_a_line_on_the_media_that_cannot_wake_it() {
        let userspace = userspace();

        for name in ["yield", "backoff", "sleep-ns"] {
            let warning = userspace
                .idle_warning(&idle(name), &idle(name))
                .unwrap_or_else(|| panic!("{name} hands the core back"));
            assert!(
                warning.contains("sender")
                    && warning.contains("receiver")
                    && warning.contains(name),
                "the line names both agents and both strategies: {warning}"
            );

            assert_eq!(
                None,
                TransportBindings::Default.idle_warning(&idle(name), &idle(name)),
                "the kernel's sockets are woken by the kernel"
            );
        }

        for name in ["noop", "spin"] {
            assert_eq!(
                None,
                userspace.idle_warning(&idle(name), &idle(name)),
                "{name} stays on its core"
            );
        }

        let one = userspace
            .idle_warning(&idle("noop"), &idle("backoff"))
            .expect("the receiver sleeps");
        assert!(
            one.contains("receiver") && !one.contains("sender"),
            "only the agent that sleeps is named: {one}"
        );
    }
}
