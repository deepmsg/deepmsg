//! Termination and the two operator toggles.
//!
//! Counterparts, all under `aeron-cluster/src/main/java/io/aeron/cluster`:
//!
//! | Rust | Reference |
//! |---|---|
//! | [`cluster_control`] | `ClusterControl.java` (counter 202) |
//! | [`node_control`] | `NodeControl.java` (counter 233) |
//! | [`termination`] | `ClusterTermination.java` |
//!
//! The counters are allocated by the agent (`ClusterCounters`); this module
//! owns their values and the transition rules, and the lookup an operator
//! process (`ClusterControl.main`, later `ClusterTool`) uses. The agent owns
//! the reading side: `checkClusterControlToggle` / `checkNodeControlToggle`
//! (`ConsensusModuleAgent.java:2546-2708`) and the termination paths
//! (`:3290-3372`).

pub mod cluster_control;
pub mod node_control;
pub mod termination;
