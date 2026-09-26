//! Memory views over shared-memory mappings — the unsafe boundary of this
//! crate (ADR-0002).
//!
//! The CnC file and the term buffers are shared between processes without
//! locks; correctness depends on strict acquire/release discipline and on
//! seqlock-style read patterns (term append CAS loops, counter reads).
//!
//! P0 will provide, mirroring `aeron-client/src/main/c/concurrent/aeron_atomics.{c,h}`:
//!
//! - a bounds-checked byte-slice view with atomic accessors
//!   (`get_volatile` / `set_ordered` / `compare_and_set` and friends),
//! - 32-bit and 64-bit variants on naturally aligned offsets.
//!
//! Every `unsafe` block here must carry a `// SAFETY:` comment stating the
//! invariant being upheld; clippy `undocumented_unsafe_blocks` is denied
//! workspace-wide.
