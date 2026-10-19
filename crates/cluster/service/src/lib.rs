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
//! # What P3-2a is, and is not
//!
//! P3-2a is the container **without snapshots**: lifecycle, the service stream,
//! the log, sessions and egress, timers and service messages, role changes and
//! termination. A recovery that finds a snapshot recording id, and a
//! `ClusterActionRequest(SNAPSHOT)`, end the container with an error naming
//! `P3-2b` rather than pretending the service has no state to save.
//!
//! # The frozen field table (public DoD 13, plan §3.1)
//!
//! The container owns everything; nothing here is `Rc<RefCell<_>>` and nothing
//! points back at the agent. A business callback receives
//! [`Cluster<'_>`](cluster::Cluster), a value assembled from **eight disjoint
//! field borrows**:
//!
//! | Field | Borrowed for |
//! |---|---|
//! | `client` | `idle` polls the conductor; publications are read by registration id |
//! | `sessions` | `session()` looks one up; `close_client_session` marks it |
//! | `cm_proxy` | the five sends to stream 105, and the sticky session header |
//! | `context` | idle strategy, clocks, commit counter, mark file, lifecycle gate |
//! | `time` | `time()`/`time_unit()`: copied from the current log event |
//! | `log_position` | copied from the current log event |
//! | `role` | `role()` |
//! | `member_id` | `member_id()` |
//!
//! Why it is sound: the callback runs `service.on_*(&mut cluster, …)` while the
//! agent holds `&mut self.service`; `Cluster` borrowing the **other** fields is
//! exactly the disjoint-borrow rule, and the log poll takes its own fields so
//! the handler can add or remove an egress publication while the image is being
//! read. This table is frozen at P3-2a; a ninth field is a new borrowing
//! conflict and needs a new justification.
//!
//! # Deviations from the reference (detail at each site)
//!
//! 1. `Cluster::idle` does not run `doBackgroundWork`: the callback already
//!    holds `&mut service`.
//! 2. Sessions are ids and views, not objects handed across a callback.
//! 3. Egress and service messages are one `offer` of an encoded buffer, not a
//!    two-vector offer; there is no vectored offer yet (`P3-0d`).
//! 4. Egress publication add/remove is a blocking client command with a 1 s
//!    timeout, not an asynchronous registration.
//! 5. An unfragmented log message is copied once into the adapter's buffer
//!    before it is decoded: the SBE decoders read a `&[u8]`, and this build's
//!    client hands out windows over a term another process is writing.

#![forbid(unsafe_code)]
// Performance spec §3: the disallowed-types list lives in clippy.toml; this
// enables it.
#![deny(clippy::disallowed_types)]

pub mod adapter;
pub mod agent;
pub mod cluster;
pub mod config;
pub mod counters;
pub mod error;
pub mod log_adapter;
pub mod proxy;
pub mod role;
pub mod service;
pub mod services;
pub mod session;
pub mod snapshot;

pub use agent::{ClusteredServiceAgent, ShutdownHandle};
pub use cluster::{ClientSession, Cluster, SessionInfo};
pub use config::ServiceContainerConfig;
pub use error::ContainerError;
pub use role::{Role, TimeUnit};
pub use service::{ClusteredService, LogHeader, SessionId};
pub use services::BasicAuction;
