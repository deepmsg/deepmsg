//! What a cluster node keeps on disk.
//!
//! `recording.log` (`P3-1e`) and `node-state.dat` are the two files the
//! consensus module owns; the mark files live in `common::mark` because the
//! container and the backup write them too.

pub mod node_state;
pub mod recording_log;
