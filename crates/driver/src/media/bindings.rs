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

use std::net::SocketAddr;

use super::poller::{MediaPoller, TransportPoller};
use super::udp_transport::{OpenError, UdpTransport};
use super::{Transport, TransportParams};

/// The bindings a driver's UDP data paths run on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TransportBindings {
    /// `default`: the kernel's sockets
    /// (`aeron_udp_channel_transport_bindings_default`,
    /// `media/aeron_udp_channel_transport_bindings.c:38-57`).
    #[default]
    Default,
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
        match self {
            Self::Default => {
                // The kernel's socket is everyone's: the reference's default
                // `init_func` (`media/aeron_udp_channel_transport.c:100-107`)
                // reads the affinity only into its bind errors, which the
                // endpoints compose themselves.
                let _ = affinity;
                let transport = UdpTransport::open(bind, multicast_interface, connect, params)?;
                Ok(Box::new(transport))
            }
        }
    }

    /// A poller for the thread `affinity` names (`poller_init_func`,
    /// `:91-94`).
    pub fn poller(&self, affinity: Affinity) -> MediaPoller {
        match self {
            Self::Default => {
                let _ = affinity;
                MediaPoller::Kernel(TransportPoller::new())
            }
        }
    }
}
