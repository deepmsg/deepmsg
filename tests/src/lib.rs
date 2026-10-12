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
//! rather than owned.

#![forbid(unsafe_code)]

pub mod archive;
pub mod archiving_driver;
pub mod driver;
pub mod java;
pub mod java_archiving_driver;
pub mod samples;
pub mod standalone_archive;
pub mod synthetic;
pub mod temp;
