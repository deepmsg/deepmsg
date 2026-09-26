//! Shared harness for the integration and interop suites.
//!
//! [`driver`] spawns the reference C media driver into a temporary aeron
//! directory, waits for its CnC file, and tears both down. It is compiled
//! unconditionally, so CI lints it even though the tests that call it are
//! behind the `interop` feature.

#![forbid(unsafe_code)]

pub mod driver;
pub mod synthetic;
