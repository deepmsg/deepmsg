//! The two categories a counted error can be in, and which of the reference's
//! exceptions each of this build's variants is.
//!
//! It is a test rather than a comment because the category is a **behaviour**:
//! the reference's counted-error handler is the deployment's, and `TestCluster`
//! passes one that fails a test on `ERROR` and only prints `WARN`
//! (`ClusterTest.java:565`, `ClusterTests.java:77-98`). The harness in
//! `tests/src/cluster_harness.rs` is that handler's other half, so a variant
//! that moves between the two changes what fails a case.

use deepmsg_cluster::ClusterError;

#[test]
fn the_replications_own_two_sentences_are_not_the_same_kind_of_thing() {
    // `RecordingReplication.java:101` throws this one with
    // `AeronException.Category.WARN`.
    let not_progressed = ClusterError::Timeout("log replication has not progressed".to_owned());
    assert!(not_progressed.is_warning());

    // `:105` throws this one as a plain `ClusterException`, which is the
    // default category — ERROR.
    let failed_to_stop = ClusterError::Cluster("log replication failed to stop".to_owned());
    assert!(
        !failed_to_stop.is_warning(),
        "the reference's `ClusterException` is an error, not a warning"
    );
}

#[test]
fn events_are_warnings_and_refusals_are_errors() {
    // `ClusterEvent` is WARN by construction (`ClusterEvent.java:35`).
    assert!(ClusterError::ClusterEvent("truncating cluster log".to_owned()).is_warning());

    // `ArchiveException` takes the default — ERROR (`AeronException.java:76-80`).
    let refused = ClusterError::ArchiveRefused {
        code: 5,
        message: "unknown src recording id 1000".to_owned(),
    };
    assert!(!refused.is_warning());
}
