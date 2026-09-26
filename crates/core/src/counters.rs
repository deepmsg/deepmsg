//! Counter type-id registry for the counters exposed through the CnC file.
//!
//! Scope (P0): turn the reference driver's counter catalogue (M08 appendix
//! B, 46 types) into an exhaustive `const` registry, together with the
//! free-form label encoding written into the counters metadata buffer.
//!
//! Counters never leave the host: cross-process references pass the 4-byte
//! registration id, and the reader opens the counter on its own CnC (M20).

/// First counter type id reserved by the reference implementation for
/// cluster standby / transition bookkeeping (inclusive).
pub const CLUSTER_RESERVED_TYPE_ID_FIRST: i32 = 220;

/// Last reserved cluster standby / transition counter type id (inclusive).
///
/// deepmsg must not repurpose this range so a future cluster track can
/// adopt the reference ids unchanged.
pub const CLUSTER_RESERVED_TYPE_ID_LAST: i32 = 232;
