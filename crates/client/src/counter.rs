//! Client-side counters (M04, M08).
//!
//! Allocation via the driver, keyed reads by type id, free-form labels in
//! the metadata buffer. Counters never leave the host: cross-process
//! references pass the 4-byte registration id and the reader opens the
//! counter on its own CnC (M20).
//!
//! Reference: `aeron-client/src/main/c/aeron_counter.{c,h}` and
//! `aeron-client/src/main/c/concurrent/aeron_counters_manager.{c,h}`.
