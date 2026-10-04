//! The reference's load test rig, rebuilt here so this project's client can be
//! measured against the same workload the reference measures its own with.
//!
//! The rig being copied is `real-logic/benchmarks`' `LoadTestRig` — not a
//! micro-benchmark but a rate-controlled end-to-end harness: it drives a
//! transceiver by a target message rate for a fixed number of one-second
//! iterations, records the round trip of every reply, and reports what it
//! managed. `deepmsg-bench`'s own `benches/latency.rs` answers a different
//! question — one round trip at a time, with no pacing — and the two numbers
//! are not the same quantity: the reference's clock starts when a message was
//! *meant* to go out, so queueing counts as latency, and measuring from the
//! moment of enqueue would report a smaller number for a different thing.
//!
//! # What this is a port of, and how faithful it is
//!
//! Every value that ends up in a report, and every rule that decides one, is
//! the reference's: the send cadence and its integer arithmetic, what a
//! timestamp means, the payload's layout, the OK/FAIL verdict and its warning
//! text, the property names and their defaults, and the interval log the
//! results are written to. Where the reference's *implementation* exists only
//! to work around the JVM — interface dispatch, cache-line padding, a
//! background reporting thread — this does the simpler Rust thing, because
//! those are not part of the measurement.

pub mod config;
pub mod format;
pub mod in_memory;
pub mod progress;
pub mod recorder;
pub mod result;
pub mod rig;
pub mod transceiver;
pub mod transport;
