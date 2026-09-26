//! Archive catalog and archive mark file (M13 / M16 / M19).
//!
//! The catalog indexes recordings (descriptors, stop positions); the mark
//! file uses SBE schema 100 (`schemas/aeron-archive-mark-codecs.xml`) and
//! carries process liveness plus the error log.
//!
//! P2 acceptance: the reference Java `ArchiveTool` must read a deepmsg-built
//! catalog and segment files byte-identically.
