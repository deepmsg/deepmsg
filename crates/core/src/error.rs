//! Error taxonomy shared by the deepmsg crates.
//!
//! P0 will introduce:
//!
//! - protocol-level negative result codes reported through the CnC error log,
//! - typed offer outcomes (backpressure, admin action, not connected, max
//!   position exceeded) which are *states*, not errors — see ADR-0003.
