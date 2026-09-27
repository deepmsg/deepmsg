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
//! documented allow ADR-0002 zone 4 provides for: [`sys`], which is the
//! process's signal seam today and the media syscall shim (`sendmmsg`,
//! `recvmmsg`) when the data plane arrives.

#![deny(unsafe_code)]

pub mod clients;
pub mod conductor;
pub mod config;
pub mod dir;
pub mod flowcontrol;
pub mod loss;
pub mod media;
pub mod receiver;
pub mod sender;
/// The process's signal seam: the one `unsafe` in this crate, and the only
/// place the allow appears. ADR-0002 zone 4.
#[allow(unsafe_code)]
pub mod sys;
pub mod system_counters;
