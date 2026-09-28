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
//! - **writing** — [`CounterManager`] allocates and reclaims counters,
//!   [`ToClientsTransmitter`] publishes driver→client events, and
//!   [`DistinctErrorLog`] is the distinct error log's writer, composition
//!   included. All are the driver's half of contracts whose reader half is
//!   also here, which is why they live in this crate rather than in the
//!   driver;
//! - version negotiation, in [`deepmsg_core::version`]: refuse on major
//!   mismatch, refuse a file whose minor is older than ours, retry while the
//!   version is still zero.
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

pub use broadcast::{Received, ToClientsReceiver, ToClientsTransmitter, TransmitError};
pub use command::{
    ADD_COUNTER_TYPE_ID, AddCounter, CLIENT_TIMEOUT_LENGTH, CORRELATED_COMMAND_LENGTH,
    COUNTER_UPDATE_LENGTH, Correlated, ERROR_CODE_GENERIC_ERROR, ERROR_CODE_UNKNOWN_COUNTER,
    ERROR_RESPONSE_HEADER_LENGTH, MAX_TOKEN_LENGTH, ON_CLIENT_TIMEOUT_TYPE_ID,
    ON_COUNTER_READY_TYPE_ID, ON_OPERATION_SUCCEEDED_TYPE_ID, ON_UNAVAILABLE_COUNTER_TYPE_ID,
    OPERATION_SUCCEEDED_LENGTH, REMOVE_COUNTER_TYPE_ID, RemoveCounter, TERMINATE_DRIVER_TYPE_ID,
    TerminateDriver, decode_add_counter, decode_correlated, decode_remove_counter,
    encode_client_timeout, encode_counter_update, encode_error, encode_operation_succeeded,
};
pub use counter_manager::{CounterManager, CounterRegions};
pub use counters::{CounterDescriptor, CounterScan, CountersReader};
pub use create::{CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT, CncCreateError, CncIdentity, CncLayout};
pub use error::{CncError, Region};
pub use error_log::{
    DistinctErrorLog, ErrorLogEntry, ErrorLogReader, ErrorLogRegion, ErrorLogScan,
};
pub use file::{CNC_FILE_NAME, CncFile, CncOpenError, Liveness, RETRY_INTERVAL};
pub use metadata::{CncMetadata, RegionLayout};
pub use ring::{ClaimError, ToDriverRing, ToDriverRingConsumer};
