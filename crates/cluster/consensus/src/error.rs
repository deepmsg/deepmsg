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

    /// The archive refused a request with one of its own codes
    /// (`ArchiveException`). Kept as a variant rather than folded into
    /// [`ClusterError::Cluster`] because `doWork` classifies one of them:
    /// storage space (`ArchiveException.STORAGE_SPACE`, code 11) ends the
    /// agent (`ConsensusModuleAgent.java:446-450`).
    ArchiveRefused {
        /// The archive's own code, `0..=16`.
        code: i64,
        /// Its reason, as it sent it.
        message: String,
    },

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

    /// The reference's `AeronException.Category`, as far as these variants go
    /// (`AeronException.java:76-80` is the default, ERROR).
    ///
    /// * [`Self::ClusterEvent`] is the reference's `ClusterEvent`, which is
    ///   **WARN** by construction (`ClusterEvent.java:35` — "does not require
    ///   a stack trace, so it can be lighter-weight").
    /// * [`Self::Timeout`] is the WARN-category `TimeoutException` the
    ///   reference throws at every site this build has one for
    ///   (`RecordingReplication.java:101`, `:945`).
    /// * [`Self::Cluster`] and [`Self::ArchiveRefused`] are `ClusterException`
    ///   and `ArchiveException`, both of which take the default: **ERROR**.
    /// * [`Self::AgentTermination`] ends the duty cycle, so no handler decides
    ///   anything about it.
    ///
    /// It matters because the reference's counted-error handler is the
    /// deployment's (`TestCluster` passes `ClusterTests.errorHandler`,
    /// `ClusterTest.java:565`) and that handler fails a test on an ERROR while
    /// a WARN is only printed (`ClusterTests.java:77-98`).
    #[must_use]
    pub const fn is_warning(&self) -> bool {
        matches!(self, Self::ClusterEvent(_) | Self::Timeout(_))
    }
}

impl From<deepmsg_cluster_common::counters::CounterError> for ClusterError {
    fn from(error: deepmsg_cluster_common::counters::CounterError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<deepmsg_cluster_common::member::MemberError> for ClusterError {
    fn from(error: deepmsg_cluster_common::member::MemberError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl From<deepmsg_cluster_common::decode::DecodeError> for ClusterError {
    fn from(error: deepmsg_cluster_common::decode::DecodeError) -> Self {
        Self::Cluster(format!(
            "{}: needed {} bytes, {} available",
            error.what, error.needed, error.available
        ))
    }
}

impl From<deepmsg_archive::client::archive::ArchiveError> for ClusterError {
    fn from(error: deepmsg_archive::client::archive::ArchiveError) -> Self {
        match error {
            deepmsg_archive::client::archive::ArchiveError::Refused {
                archive_error_code,
                message,
                ..
            } => Self::ArchiveRefused {
                code: archive_error_code,
                message,
            },
            other => Self::Cluster(other.to_string()),
        }
    }
}

impl From<deepmsg_client::client::CommandError> for ClusterError {
    fn from(error: deepmsg_client::client::CommandError) -> Self {
        Self::Cluster(error.to_string())
    }
}

impl core::fmt::Display for ClusterError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AgentTermination(msg) => write!(f, "agent termination: {msg}"),
            Self::ClusterEvent(msg) => write!(f, "cluster event: {msg}"),
            Self::Cluster(msg) => write!(f, "cluster: {msg}"),
            Self::ArchiveRefused { code, message } => {
                write!(f, "archive refused (code {code}): {message}")
            }
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
        }
    }
}

impl core::error::Error for ClusterError {}
