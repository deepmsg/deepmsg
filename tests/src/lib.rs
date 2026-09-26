//! Shared harness for the integration and interop suites: spawn a driver
//! into a temporary aeron directory, wait for the CnC file, tear down.
//! Lands with P0.

#![forbid(unsafe_code)]
