//! Why a cluster client call did not do what it was asked.
//!
//! The reference's failures are four exceptions — `ConfigurationException`,
//! `TimeoutException`, `ClusterException` and `AuthenticationException` — and
//! all four extend `AeronException`, whose constructor is what puts the
//! `ERROR - ` in front of every one of their messages
//! (`aeron-client/src/main/java/io/aeron/AeronException.java:76-80`). The
//! reference's own tests assert `getMessage()`, prefix and all
//! (`AeronClusterContextTest`), so the prefix belongs in [`Display`] here
//! rather than in every message a caller builds.
//!
//! The fifth arm — [`ClusterClientError::Client`] — has no reference
//! counterpart: the driver refused an add, or the client stopped being able to
//! work. It is where this build's `deepmsg-client` failures land, and it does
//! not pretend to be one of the reference's four.

use core::fmt;

/// What went wrong.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterClientError {
    /// The context is not one a session can be opened with. The message is the
    /// reference's, word for word (`AeronCluster.Context.conclude`).
    Configuration(String),
    /// The connect ran out of time in a step. The message is the reference's
    /// own text (`AeronCluster.AsyncConnect.checkDeadline`): the state, the
    /// timeout, and which resource had not arrived.
    Timeout(String),
    /// The cluster refused the connect with a reason of its own, or the
    /// connection was lost while it was being made.
    Cluster(String),
    /// The cluster's authenticator rejected the credentials.
    Authentication(String),
    /// This build's client could not carry out the operation — the driver
    /// refused an add, the command ring was full, or the client terminated.
    Client(String),
}

impl fmt::Display for ClusterClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // `AeronException`'s prefix, applied where the construction of every
            // one of the reference's messages applies it.
            Self::Configuration(message)
            | Self::Timeout(message)
            | Self::Cluster(message)
            | Self::Authentication(message) => write!(f, "ERROR - {message}"),
            Self::Client(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ClusterClientError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four that come from the reference carry its prefix; a client
    /// failure does not pretend to be one of them.
    #[test]
    fn the_references_four_carry_its_prefix() {
        for error in [
            ClusterClientError::Configuration("ingressChannel must be specified".to_owned()),
            ClusterClientError::Timeout("cluster connect timeout: state=DONE".to_owned()),
            ClusterClientError::Cluster("unexpected loss of connection to cluster".to_owned()),
            ClusterClientError::Authentication("no".to_owned()),
        ] {
            assert!(
                error.to_string().starts_with("ERROR - "),
                "{error} should carry the reference's prefix"
            );
        }

        assert_eq!(
            "the driver refused it",
            ClusterClientError::Client("the driver refused it".to_owned()).to_string()
        );
    }
}
