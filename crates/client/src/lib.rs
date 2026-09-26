//! deepmsg client: the public API for publishers and subscribers.
//!
//! Counterpart of the client side of `aeron-client/src/main/c`. P0 scope:
//! connect to a running driver's CnC file, add/remove publications and
//! subscriptions, move data, read counters, terminate cleanly.
//!
//! Design notes (ADR-0003): offer outcomes are typed states, not errors;
//! unknown driver events are skipped rather than panicked on; the default
//! error handler keeps the process alive.

#![forbid(unsafe_code)]

pub mod conductor;
pub mod counter;
pub mod fragment_assembler;
pub mod image;
pub mod publication;
pub mod subscription;
pub mod terminate;
