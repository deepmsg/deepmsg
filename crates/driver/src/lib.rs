//! The deepmsg media driver: the process that owns the CnC file, the term
//! buffers and the UDP data plane.
//!
//! Thread model (P1), mirroring the reference C driver (M08):
//!
//! - a conductor thread: command drain and duty cycle — the
//!   single-threaded control-plane core (M09),
//! - a sender and a receiver thread for the data plane (M10 / M11),
//! - name-resolution offload and asynchronous resource reclamation.
//!
//! Unsafe is denied at crate root, and exactly one module carries the
//! documented allow ADR-0002 zone 4 provides for: [`sys`], which holds the
//! kernel seam — signals, the socket probe a log buffer's metadata is written
//! from, randomness — and grows into the media syscall shim (`sendmmsg`,
//! `recvmmsg`) when the data plane arrives.

#![deny(unsafe_code)]

pub mod channel_uri;
pub mod clients;
pub mod conductor;
pub mod config;
pub mod dir;
pub mod flowcontrol;
pub mod idle;
pub mod ipc_publication;
pub mod ipc_publications;
pub mod ipc_subscriptions;
pub mod loss;
pub mod media;
pub mod native_resource_agent;
pub mod position;
pub mod publication_params;
pub mod receiver;
pub mod send_endpoints;
pub mod sender;
pub mod subscribable;
/// The kernel seam: the one `unsafe` in this crate, and the only place the
/// allow appears. ADR-0002 zone 4.
#[allow(unsafe_code)]
pub mod sys;
pub mod system_counters;
pub mod udp_channel;
