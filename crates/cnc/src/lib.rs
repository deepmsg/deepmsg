//! The CnC (command-and-control) file: the shared-memory contract between
//! client and driver processes on one host.
//!
//! Scope (P0):
//!
//! - file layout constants and the metadata block
//!   (`aeron-client/src/main/c/aeron_cnc_file_descriptor.{c,h}`),
//! - table-of-contents traversal (variable-length section discovery),
//! - semantic version negotiation: refuse on major mismatch, tolerate a
//!   minor >= our own (constants in [`deepmsg_core::version`]),
//! - heartbeat/liveness fields and the aeron-directory discipline (M17:
//!   liveness is detected via the CnC heartbeat rather than file locks, a
//!   live driver answers EBUSY, and a dead driver's directory — including
//!   the rescued error log — is reclaimed on next start),
//! - error log section handling.
//!
//! This crate is one of the named unsafe zones (ADR-0002).

#![deny(unsafe_op_in_unsafe_fn)]
