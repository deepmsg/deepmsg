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
//! Nothing is here yet: `P3-3a` brings `AsyncConnect` and the egress poller,
//! `P3-3b` the session and leader change, `P3-3c` the LC-1 suite.

#![forbid(unsafe_code)]
#![deny(clippy::disallowed_types)]
