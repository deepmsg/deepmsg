//! Lock-free structures used across the CnC boundary.
//!
//! Planned for P0, each as a dedicated module, mirroring the reference
//! sources under `aeron-client/src/main/c/concurrent/`:
//!
//! - `one_to_one` — SPSC ring buffer, reference `aeron_spsc_rb.{c,h}`
//! - `many_to_one` — MPSC ring buffer (client -> driver command queue),
//!   reference `aeron_mpsc_rb.{c,h}`
//! - `broadcast` — broadcast transmitter/receiver (driver -> client events),
//!   reference `aeron_broadcast_transmitter.{c,h}` / `aeron_broadcast_receiver.{c,h}`
//! - `counter` — atomic counter slots in the CnC counters buffers,
//!   reference `aeron_dist/counter.{c,h}`
//!
//! These structures get loom tests (feature-gated) once they exist.
