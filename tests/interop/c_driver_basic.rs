//! Interop: deepmsg client against the reference C driver (Aeron 1.53.2).
//!
//! Run with:
//!
//!     cargo test -p deepmsg-tests --features interop
//!
//! Requires the reference `aeronmd` binary on PATH (see docs/reference.md).
//! This is the P0 acceptance test: pub/sub roundtrip, counters, and CnC
//! 0.2.0 negotiation verified against the reference implementation. Until
//! P0 lands this target intentionally contains no tests.
