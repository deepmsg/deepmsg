//! deepmsg cluster: the client (P3).
//!
//! Counterpart of `aeron-cluster/src/main/java/io/aeron/cluster/client` — the
//! synchronous connect, `AsyncConnect`, the egress poller, and the session
//! object that follows a leader change.
//!
//! **This crate must not depend on `deepmsg-cluster` or `deepmsg-cluster-service`.**
//! An application that talks to a cluster should not link the consensus code;
//! the plan's crate map (plan §4.2) states it as a rule, and the reason is not
//! only build time: a client that could reach into a consensus module is a
//! client that could be tempted to agree with it, and then LC-1 would stop
//! testing anything.
//!
//! `P3-3a` brought the context, the egress poller and `AsyncConnect` — enough
//! to establish a session against a real cluster. `P3-3b` brings the session
//! itself (sending, polling egress, following a leader change), `P3-3c` the
//! LC-1 suite.
//!
//! # Shaped like the archive's client
//!
//! A `Client` is always the caller's here, and every poll method takes it:
//! `AsyncConnect::poll(&mut Client)`, `AeronCluster::poll_egress(&mut Client)`.
//! Nothing in this crate owns a client, starts a thread, or waits on one — the
//! caller drives the client and the connect a turn at a time, exactly as
//! `crates/archive/src/client` does. That is what lets one `Client` carry
//! several cluster sessions.

#![forbid(unsafe_code)]
#![deny(clippy::disallowed_types)]

pub mod async_connect;
pub(crate) mod client_ops;
pub mod cluster;
pub mod context;
pub mod egress;
pub mod egress_poller;
pub mod error;
pub(crate) mod sbe;
#[cfg(test)]
pub(crate) mod test_client;

pub use async_connect::{AsyncConnect, ConnectState, Polled, connect};
pub use cluster::{AeronCluster, IngressPublication, SessionClaim, SessionState};
pub use context::{
    AeronClusterContext, CLIENT_NAME_PROP_NAME, CredentialsSupplier, EGRESS_CHANNEL_PROP_NAME,
    EGRESS_STREAM_ID_DEFAULT, EGRESS_STREAM_ID_PROP_NAME, INGRESS_CHANNEL_PROP_NAME,
    INGRESS_ENDPOINTS_PROP_NAME, INGRESS_STREAM_ID_DEFAULT, INGRESS_STREAM_ID_PROP_NAME,
    MAX_CLIENT_NAME_LENGTH, MESSAGE_TIMEOUT_DEFAULT_NS, MESSAGE_TIMEOUT_PROP_NAME, NanoClock,
    NullCredentialsSupplier, PROTOCOL_SEMANTIC_VERSION, SystemNanoClock,
};
pub use egress::{
    ControlledEgressAdapter, ControlledEgressListener, ControlledEgressListenerExtension,
    EgressAdapter, EgressListener, EgressListenerExtension, SESSION_HEADER_LENGTH,
};
pub use egress_poller::EgressPoller;
pub use error::ClusterClientError;
