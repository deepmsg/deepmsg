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
//! Unsafe is denied at crate root; the future media syscall shim (`sys`,
//! sendmmsg/recvmmsg and friends) will carry an explicit, documented allow
//! per ADR-0002.

#![deny(unsafe_code)]

pub mod conductor;
pub mod config;
pub mod dir;
pub mod flowcontrol;
pub mod loss;
pub mod media;
pub mod receiver;
pub mod sender;
