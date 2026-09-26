//! Client conductor: the client-side agent (M04).
//!
//! P0 scope, mirroring `aeron-client/src/main/c/aeron_client_conductor.c`:
//!
//! - duty cycle: drain driver events (broadcast), drive the liveness
//!   heartbeat, run close/reclaim duties,
//! - async add/remove of publications and subscriptions with the
//!   one-in-flight-per-object rule,
//! - driver-death detection and forced close (reference: ~10s driver
//!   timeout -> error callback, not process exit — see ADR-0003),
//! - termination handshake: send CLIENT_CLOSE / TERMINATE_DRIVER; the
//!   reference C client does not wait for a reply (documented in M15).
