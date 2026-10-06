//! Archive catalog (M13 / M16 / M19).
//!
//! The catalog indexes recordings (descriptors, stop positions, the recording
//! log's place in the file): an append-only, variable-length set of records
//! behind a small header, with its own capacity and its own recovery from a
//! crash. P2-2b.
//!
//! The mark file — the other file an archive directory holds — is not here: it
//! is [`crate::mark_file`], over the generic [`crate::mark`], because the two
//! share nothing but a directory. This module's doc used to describe both,
//! which is the kind of claim that goes stale without saying so.
//!
//! P2 acceptance: the reference Java `ArchiveTool` must read a deepmsg-built
//! catalog and segment files byte-identically.
