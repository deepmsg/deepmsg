//! The businesses that ship with this build.
//!
//! The reference selects a business by Java class name
//! (`aeron.cluster.service.class.name`) and refuses anything it cannot load;
//! this build selects one from a compiled-in registry (spec §6) and refuses a
//! class name by name (`config.rs`). What lives here are the entries of that
//! registry: P3-2c adds the binary that selects between them, P3-2b adds the
//! first one that has state worth snapshotting.

pub mod basic_auction;

pub use basic_auction::BasicAuction;
