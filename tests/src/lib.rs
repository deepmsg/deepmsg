//! Shared harness for the integration and interop suites.
//!
//! [`driver`] spawns the reference C media driver into a temporary aeron
//! directory, waits for its CnC file, and tears both down. [`archiving_driver`]
//! does the same for this workspace's own archiving media driver, whose
//! readiness is a mark file's version rather than a file's existence. Both are
//! compiled unconditionally, so CI lints them even though the tests that call
//! [`driver`] are behind the `interop` feature. [`archive`] is the fourth: a
//! client for our archive, written by hand because this workspace has no archive
//! client crate yet. [`standalone_archive`] is the fifth and the newest: the
//! archive that attaches to a driver somebody else runs, which is the same pair
//! of binaries again with the difference that the aeron directory is borrowed
//! rather than owned. [`java_driver`] is the sixth: the reference's **Java** media
//! driver with no archive beside it, which is the one pairing a mixed gate needs.
//! [`aeron_tests`] is the seventh and is not a process at all: it is where the C
//! and Java this suite runs are looked for, which is a fork of the reference
//! rather than anything in this tree.

#![forbid(unsafe_code)]

pub mod aeron_tests;
pub mod archive;
pub mod archiving_driver;
pub mod cluster;
pub mod cluster_bytes;
pub mod cluster_client;
pub mod cluster_harness;
pub mod cluster_mixed;
pub mod cluster_tool;
pub mod driver;
pub mod java;
pub mod java_archiving_driver;
pub mod java_driver;
pub mod samples;
pub mod standalone_archive;
pub mod synthetic;
pub mod temp;
