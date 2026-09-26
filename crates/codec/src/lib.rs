//! SBE codecs generated from the schemas under `schemas/`.
//!
//! The wire protocol, archive and cluster control protocols, mark files and
//! the node-state file all share SBE encoding (M19); the only deliberately
//! non-SBE on-disk format in the system is the archive `recording.log`
//! (hand-rolled inside `deepmsg-archive`).
//!
//! This is a placeholder until ADR-0004 selects the generator (sbe-rs vs a
//! hand-written one). Generated sources will live in `src/generated/`, be
//! checked in, and be guarded by golden byte tests against fixture bytes
//! captured from the reference Java encoder.

#![forbid(unsafe_code)]
