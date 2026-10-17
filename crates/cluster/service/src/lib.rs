//! deepmsg cluster: the service container (P3).
//!
//! Counterpart of `aeron-cluster/src/main/java/io/aeron/cluster/service` — the
//! agent that replays the log and calls the business, plus the business API
//! itself (`ClusteredService`, `Cluster<'_>`, `ClientSession<'_>`).
//!
//! It talks to the consensus module only through the S2 seam: IPC streams 104
//! and 105, the commit-position and recovery-state counters, and the service
//! mark file. That is why it does not depend on `deepmsg-cluster` — the
//! container must be able to run against a *Java* consensus module (LC-4a),
//! and the dependency direction is what keeps that true by construction.
//!
//! Nothing is here yet: `P3-2a` brings the skeleton, `P3-2b` snapshots and the
//! business twin, `P3-2c` the binary and the LC-4a suite.

#![forbid(unsafe_code)]
#![deny(clippy::disallowed_types)]
