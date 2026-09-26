//! The CnC (command-and-control) file: the shared-memory contract between
//! client and driver processes on one host.
//!
//! Scope (P0) — the read side:
//!
//! - the metadata block and the region layout it describes
//!   (`aeron-client/src/main/c/aeron_cnc_file_descriptor.{c,h}`),
//! - version negotiation, in [`deepmsg_core::version`]: refuse on major
//!   mismatch, refuse a file whose minor is older than ours, retry while the
//!   version is still zero,
//! - driver liveness, read from the to-driver ring's consumer heartbeat
//!   rather than from any file lock,
//! - counter enumeration and the distinct error log.
//!
//! Out of scope: everything that writes to a CnC file — command publication,
//! heartbeats, counter allocation. The to-clients broadcast region is laid out
//! in [`layout`] but has no reader type yet, because the reference's own
//! `aeron_cnc_t` never constructs a broadcast receiver: that belongs to the
//! client conductor, and it arrives with the code that consumes it.
//!
//! # Unsafe
//!
//! This crate is a named unsafe zone (ADR-0002, zone 3), but it currently
//! contains **none**: every byte it reads goes through
//! [`deepmsg_core::pal`] for the mapping and [`deepmsg_core::buffer`] for the
//! accessors, both of which are safe APIs. That is the shape the policy is
//! for — the zone is available, not required.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod command;
pub mod counters;
pub mod error;
pub mod error_log;
pub mod file;
pub mod layout;
pub mod metadata;
pub mod ring;

pub use command::{MAX_TOKEN_LENGTH, TERMINATE_DRIVER_TYPE_ID, TerminateDriver};
pub use counters::{CounterDescriptor, CounterScan, CountersReader};
pub use error::{CncError, Region};
pub use error_log::{ErrorLogEntry, ErrorLogReader, ErrorLogScan};
pub use file::{CNC_FILE_NAME, CncFile, CncOpenError, RETRY_INTERVAL};
pub use metadata::{CncMetadata, RegionLayout};
pub use ring::{ClaimError, ToDriverRing};
