//! The four error kinds the consensus module distinguishes.
//!
//! The reference signals all of these with unchecked exceptions, so in Java
//! "can this call fail?" is not in the signature. Here it is: every function
//! these can pass through returns `Result`, and it propagates with `?` — never
//! `if let Ok(..)` or `let _ =`. A `ClusterEvent` that gets swallowed is a duty
//! cycle that keeps doing work the reference would have abandoned, and no test
//! that only checks counters will see it (plan §4.4, rule 5).

/// The reference's four exception kinds (`ConsensusModuleAgent.java:439-466`),
/// named after what the caller does with them rather than after the Java class.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterError {
    /// The agent is finished. Count it, run the termination hook, leave the
    /// thread. Counterpart of `AgentTerminationException`.
    AgentTermination(String),

    /// A recoverable event during an election. `Election::handle_error` takes
    /// it and the state machine returns to `INIT`. Counterpart of
    /// `ClusterEventException`.
    ClusterEvent(String),

    /// An invariant is broken. Counterpart of `ClusterException`.
    Cluster(String),

    /// A deadline passed. During an election this is a WARN, not a failure.
    /// Counterpart of `TimeoutException`.
    Timeout(String),
}

impl ClusterError {
    /// The reference's three-way split at the top of `doWork`
    /// (`ConsensusModuleAgent.java:439-466`): is this the agent ending, or can
    /// the election retry?
    pub fn is_termination(&self) -> bool {
        matches!(self, Self::AgentTermination(_))
    }

    /// Whether an in-progress election should absorb this and retry.
    pub fn is_election_recoverable(&self) -> bool {
        matches!(self, Self::ClusterEvent(_) | Self::Timeout(_))
    }
}

impl core::fmt::Display for ClusterError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AgentTermination(msg) => write!(f, "agent termination: {msg}"),
            Self::ClusterEvent(msg) => write!(f, "cluster event: {msg}"),
            Self::Cluster(msg) => write!(f, "cluster: {msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
        }
    }
}

impl core::error::Error for ClusterError {}
