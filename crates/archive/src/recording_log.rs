//! The archive `recording.log`: the one deliberately non-SBE on-disk format
//! in the system (M19).
//!
//! Byte contract to replicate exactly:
//!
//! - 48-byte entries, 64-byte alignment, no file header, no version field,
//! - bit 31 of the stop position doubles as the INVALID flag,
//! - appends advance from the 64-byte-aligned tail; restore rewrites
//!   entries in place.
//!
//! Reference: `aeron-archive/src/main/java/io/aeron/archive/RecordingLog.java`.
