//! Micro-benchmarks and the latency harness.
//!
//! Planned (P0):
//!
//! - criterion micro-benchmarks: ring buffers, term scanning, buffer
//!   accessors,
//! - a ping-pong latency histogram (hdrhistogram) over a live driver — the
//!   project's headline gate; baseline comparisons run against the
//!   reference C driver.
#![forbid(unsafe_code)]
