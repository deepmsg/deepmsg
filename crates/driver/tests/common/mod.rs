//! Pieces the driver's DPDK test files share.
//!
//! A directory rather than a `common.rs`: Cargo compiles every `tests/*.rs` as
//! a test target of its own and does not look inside a directory unless it has
//! a `main.rs`, so nothing here becomes a test binary that runs zero tests.
//!
//! It is reached only from the files that are `#[cfg(feature = "dpdk")]`, and
//! so is only compiled when the media is: none of this needs DPDK itself.

pub mod pcap;
