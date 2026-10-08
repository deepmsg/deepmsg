//! deepmsg archive: recording, replay, replication and catalog.
//!
//! Implemented in P2 (see `docs/roadmap.md`). The reference server is
//! Java-only (the C tree ships a client); deepmsg provides both sides,
//! byte-compatible with the reference on-disk formats and the SBE control
//! protocol (schema 101, `schemas/aeron-archive-codecs.xml`).

#![forbid(unsafe_code)]

pub mod catalog;
pub mod checksum;
pub mod client;
pub mod mark;
pub mod mark_file;
pub mod recording_log;
pub mod recording_writer;
pub mod segment;
pub mod server;
