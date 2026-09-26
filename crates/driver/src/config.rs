//! Driver configuration.
//!
//! P1 scope, mirroring the reference driver's configuration surface
//! (M08 appendix A: ~110 environment variables in 10 groups) plus the URI
//! driver params (M06):
//!
//! - a typed config struct with reference-compatible defaults,
//! - the aeron-directory discipline: liveness via CnC heartbeat (EBUSY for
//!   a live driver), dead-driver reclaim including error-log rescue (M17),
//! - the capacity formula: fixed overhead plus per-stream 3x term length,
//!   sparse files by default with the low-latency opt-in (M17).
