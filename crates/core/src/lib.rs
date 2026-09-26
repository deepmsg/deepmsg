//! Protocol-agnostic foundation shared by the deepmsg client and driver.
//!
//! This crate is the counterpart of the shared C sources under
//! `aeron-client/src/main/c` that the reference driver also compiles in:
//! buffers, concurrency, log-buffer framing, counters, URI handling and
//! versioning. It carries no protocol knowledge of its own.
//!
//! Unsafe policy (ADR-0002): `unsafe` is confined to named zones — here,
//! only [`buffer`] — and every other crate in the workspace forbids it
//! outright.

pub mod buffer;
pub mod concurrent;
pub mod counters;
pub mod error;
pub mod logbuffer;
pub mod uri;
pub mod version;
