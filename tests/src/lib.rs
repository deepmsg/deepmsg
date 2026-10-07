//! Shared harness for the integration and interop suites.
//!
//! [`driver`] spawns the reference C media driver into a temporary aeron
//! directory, waits for its CnC file, and tears both down. [`archiving_driver`]
//! does the same for this workspace's own archiving media driver, whose
//! readiness is a mark file's version rather than a file's existence. Both are
//! compiled unconditionally, so CI lints them even though the tests that call
//! [`driver`] are behind the `interop` feature.

#![forbid(unsafe_code)]

pub mod archiving_driver;
pub mod driver;
pub mod java;
pub mod samples;
pub mod synthetic;
pub mod temp;
