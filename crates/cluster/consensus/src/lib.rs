//! deepmsg cluster: the consensus module (P3).
//!
//! This crate is the counterpart of `aeron-cluster/src/main/java/io/aeron/cluster`
//! — the consensus module, its election state machine, and the on-disk state
//! (`recording.log`, `node-state.dat`). The service container and the cluster
//! client are separate crates; this one must not depend on them.
//!
//! Design notes (plan `analysis/cluster/deepmsg-p3-plan-cluster.md`):
//!
//! - **Transcribed, not redesigned.** Every method here has a reference
//!   counterpart named in its doc comment. Where this build departs, the
//!   departure is written down at the departure.
//! - **Waiting keeps the reference's blocking semantics.** Any wait is a loop
//!   whose body is `idle()`, and `idle()` drives this agent's own client
//!   (plan §4.4). A wait that returns to the duty cycle instead is a behaviour
//!   change, not an optimisation.
//! - **`ClusterEvent` and `AgentTermination` are control flow.** Every function
//!   they can pass through returns `Result` and propagates with `?`; swallowing
//!   one lets the duty cycle do work the reference would have skipped
//!   (plan §4.4, rule 5).
//!
//! # What P3-5a-α froze
//!
//! Frozen: the two election seams — `election::ElectionHost` and
//! `election::ConsensusSender` — and the `election::ElectionState` code space.
//! Their method sets are pinned by `tests/election_surface.rs` against the
//! reference's own call surface, captured in
//! `tests/fixtures/election-surface.tsv`.
//!
//! Not frozen yet: every method body, the contents of `election::Election`
//! (the field *grouping* is fixed; where the real type belongs to a later
//! slice, a placeholder stands in), and everything outside `election`.

#![forbid(unsafe_code)]
// Performance spec §3: the disallowed-types list lives in clippy.toml; this
// enables it.
#![deny(clippy::disallowed_types)]

pub mod election;
pub mod error;
pub mod handles;
pub mod state;

pub use error::ClusterError;
pub use state::{ModuleState, Role};
