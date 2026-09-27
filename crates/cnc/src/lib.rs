//! The CnC (command-and-control) file: the shared-memory contract between
//! client and driver processes on one host.
//!
//! Scope:
//!
//! - **reading** — the metadata block and the region layout it describes
//!   (`aeron-client/src/main/c/aeron_cnc_file_descriptor.{c,h}`), driver
//!   liveness from the to-driver ring's consumer heartbeat rather than from any
//!   file lock, counter enumeration, and the distinct error log;
//! - **creating** — [`CncFile::create`]: the file, its regions, and the
//!   release store that publishes them. Only a driver does this; the client
//!   half of the life of a CnC file is mapping one that is already there;
//! - version negotiation, in [`deepmsg_core::version`]: refuse on major
//!   mismatch, refuse a file whose minor is older than ours, retry while the
//!   version is still zero.
//!
//! Not here yet: counter *allocation* and the to-clients broadcast *writer*.
//! Both belong to the driver, and both arrive with the driver code that needs
//! them rather than ahead of it.
//!
//! # Unsafe
//!
//! This crate is a named unsafe zone (ADR-0002, zone 3), but it currently
//! contains **none**: every byte it reads or writes goes through
//! [`deepmsg_core::pal`] for the mapping and [`deepmsg_core::buffer`] for the
//! accessors, both of which are safe APIs. That is the shape the policy is
//! for — the zone is available, not required.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod broadcast;
pub mod command;
pub mod counter_manager;
pub mod counters;
pub mod create;
pub mod error;
pub mod error_log;
pub mod file;
pub mod layout;
pub mod metadata;
pub mod ring;

pub use broadcast::{Received, ToClientsReceiver};
pub use command::{MAX_TOKEN_LENGTH, TERMINATE_DRIVER_TYPE_ID, TerminateDriver};
pub use counter_manager::CounterManager;
pub use counters::{CounterDescriptor, CounterScan, CountersReader};
pub use create::{CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT, CncCreateError, CncIdentity, CncLayout};
pub use error::{CncError, Region};
pub use error_log::{ErrorLogEntry, ErrorLogReader, ErrorLogScan};
pub use file::{CNC_FILE_NAME, CncFile, CncOpenError, RETRY_INTERVAL};
pub use metadata::{CncMetadata, RegionLayout};
pub use ring::{ClaimError, ToDriverRing, ToDriverRingConsumer};
