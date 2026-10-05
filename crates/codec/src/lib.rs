//! SBE codecs generated from the schemas under `schemas/`.
//!
//! SBE covers the archive and cluster control protocols, the mark files and the
//! node-state file (M19). It does not cover the UDP wire protocol — those frames
//! are hand-written structs, not generated (`docs/protocol/wire-frames.md`) —
//! nor the archive `recording.log`, hand-rolled inside `deepmsg-archive`.
//!
//! This is a placeholder until ADR-0004 selects the generator (sbe-rs vs a
//! hand-written one). Generated sources will live in `src/generated/`, be
//! checked in, and be guarded by golden byte tests against fixture bytes
//! captured from the reference Java encoder.

#![forbid(unsafe_code)]
