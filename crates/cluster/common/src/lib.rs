//! The parts every cluster crate needs.
//!
//! Counterpart of the pieces that `aeron-cluster` shares across the consensus
//! module, the service container and the client: the `aeron.cluster.*`
//! properties, the small collections whose layout is observable, and the
//! length guards in front of the generated SBE decoders.
//!
//! Dependency direction (plan §4.2): `consensus -> common`, `service -> common`,
//! `client -> common`. This crate depends on none of them, and on nothing else.
//!
//! `P3-1a` built the property layer, the collections and the length guards;
//! `P3-1b` added the mark file, `P3-1c` the counter table, `P3-1d` the member
//! string and the quorum functions.

#![forbid(unsafe_code)]
// Performance spec §3: the disallowed-types list lives in clippy.toml; this
// enables it.
#![deny(clippy::disallowed_types)]

pub mod collections;
pub mod config;
pub mod counters;
pub mod decode;
pub mod mark;
pub mod member;
