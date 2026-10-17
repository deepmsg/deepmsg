//! Consensus-module snapshots: writing them ([`taker`]) and reading them back
//! ([`adapter`]) (P3-4d).

pub mod adapter;
pub mod taker;

pub use adapter::{ConsensusModuleSnapshotAdapter, SnapshotLoadHandler, SnapshotSession};
pub use taker::{ConsensusModuleSnapshotTaker, SNAPSHOT_TYPE_ID, SnapshotSend};
