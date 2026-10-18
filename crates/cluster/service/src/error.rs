//! The error vocabulary of the service container.
//!
//! The reference's container throws the same unchecked exceptions its
//! consensus module does, but the two halves of S2 are separate processes and
//! separate crates here: `deepmsg-cluster-service` cannot depend on
//! `deepmsg-cluster` (LC-4a runs this container against a *Java* consensus
//! module, and the dependency direction is what keeps that true by
//! construction). So this is a second, same-shaped enum rather than a shared
//! one — the four kinds the container distinguishes are the four it can act on.
//!
//! The names follow what the caller does with them, as in
//! `deepmsg-cluster`'s error: `AgentTermination` ends the thread,
//! `ClusterEvent` is reported and work continues, `Cluster` is a broken
//! invariant that ends the container, and `Timeout` is a deadline that passed.

use deepmsg_client::client::CommandError;
use deepmsg_cluster_common::counters::CounterError;
use deepmsg_cluster_common::decode::DecodeError;
use deepmsg_cluster_common::mark::ClusterMarkError;
use deepmsg_cluster_common::member::MemberError;

/// What can go wrong in the container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContainerError {
    /// The agent is finished. Counterpart of `AgentTerminationException`.
    AgentTermination(String),

    /// A recoverable event: the reference reports it through the counted error
    /// handler and carries on. Counterpart of `ClusterEvent`.
    ClusterEvent(String),

    /// A broken invariant, or a send the reference throws from.
    /// Counterpart of `ClusterException`.
    Cluster(String),

    /// A deadline passed. Counterpart of `TimeoutException`.
    Timeout(String),
}

impl ContainerError {
    /// Whether this error ends the duty cycle (as opposed to being reported).
    pub fn is_termination(&self) -> bool {
        matches!(self, Self::AgentTermination(_))
    }
}

impl core::fmt::Display for ContainerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AgentTermination(message) => write!(f, "agent termination: {message}"),
            Self::ClusterEvent(message) => write!(f, "cluster event: {message}"),
            Self::Cluster(message) => write!(f, "cluster error: {message}"),
            Self::Timeout(message) => write!(f, "timeout: {message}"),
        }
    }
}

impl std::error::Error for ContainerError {}

impl From<CommandError> for ContainerError {
    fn from(error: CommandError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<deepmsg_client::client::ConnectError> for ContainerError {
    fn from(error: deepmsg_client::client::ConnectError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<std::io::Error> for ContainerError {
    fn from(error: std::io::Error) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<CounterError> for ContainerError {
    fn from(error: CounterError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<MemberError> for ContainerError {
    fn from(error: MemberError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<ClusterMarkError> for ContainerError {
    fn from(error: ClusterMarkError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<DecodeError> for ContainerError {
    fn from(error: DecodeError) -> Self {
        Self::Cluster(format!(
            "{}: needed {} bytes, {} available",
            error.what, error.needed, error.available
        ))
    }
}
