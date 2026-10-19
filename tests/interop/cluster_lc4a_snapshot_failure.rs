//! LC-4a's snapshot-failure scenario: **a service whose snapshot throws ends,
//! and the cluster carries on**.
//!
//! The reference's `MultiClusteredServicesTest.shouldContinueFromLogIfSnapshotThrowsException`
//! (`MultiClusteredServicesTest.java:88-135`) makes one service fail the next
//! snapshot, takes one, and then asserts three things: the snapshot counter did
//! not advance, the other services kept counting messages, and a service
//! restarted afterwards catches up from the log.
//!
//! What the reference relies on is that a business exception in `onTakeSnapshot`
//! propagates out of the agent (`ClusteredServiceAgent.java:988-1019` catches
//! only `ArchiveException`), after the action has been acked with a null
//! recording id (`:1057-1092`). That is the behaviour asserted here: the failing
//! container **ends**, the module's snapshot count stays put, its neighbour
//! keeps applying the log, and the restarted container replays what it missed.
//!
//! In-process only: the failure is injected through the test business, and an
//! external container has no way to be told to fail. Recorded as
//! `n/a:external-injection` in `records/P3-2c.md`.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID;
use deepmsg_tests::cluster::{ContainerExit, MemberSpec, Options, TestCluster};

const CLUSTER_ID: i32 = 12;
const SERVICE_COUNT: i32 = 2;

static SERIAL: Mutex<()> = Mutex::new(());

fn start(test_name: &str) -> Option<TestCluster> {
    // `ClusterAuctionBidClientProbe` is used only for its SNAPSHOT toggle: with
    // zero bids it connects, turns the control counter, and prints
    // `SNAPSHOT_REQUESTED`.
    let options = Options {
        cluster_id: CLUSTER_ID,
        probes: vec![
            "ClusterEchoClientProbe.java",
            "ClusterAuctionBidClientProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterEchoTimerService.java",
        ],
        ..Options::default()
    };
    TestCluster::start(
        test_name,
        &[
            MemberSpec::test_container(0, "test-service").with_service_id(0),
            MemberSpec::test_container(0, "test-service").with_service_id(1),
        ],
        options,
    )
}

fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

fn echo(cluster: &TestCluster, aeron_dir: &str, expected: usize) -> String {
    cluster.run_probe(
        "ClusterEchoClientProbe",
        &[aeron_dir, "1", "20", &expected.to_string()],
    )
}

#[test]
fn a_snapshot_that_fails_ends_that_service_and_the_cluster_carries_on() {
    let _serial = SERIAL.lock().expect("the serial lock");
    let Some(mut cluster) = start("lc4a-snapshot-failure") else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let first = cluster.test_service(0, 0).expect("service 0's state");
    let second = cluster.test_service(0, 1).expect("service 1's state");

    // One message reaches both services.
    let reading = echo(&cluster, &aeron_dir, SERVICE_COUNT as usize);
    assert!(
        reading.contains(&format!("RECEIVED {SERVICE_COUNT}")),
        "both services answered, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );
    wait_for("both services to count the message", || {
        first.messages() >= 1 && second.messages() >= 1
    });

    // Service 0 will fail the next snapshot.
    first.fail_next_snapshot();

    let reading = cluster.run_probe(
        "ClusterAuctionBidClientProbe",
        &[
            aeron_dir.as_str(),
            &CLUSTER_ID.to_string(),
            "0",
            "0",
            "0",
            "true",
            "0",
        ],
    );
    assert!(
        reading.contains("SNAPSHOT_REQUESTED"),
        "the SNAPSHOT toggle turned, client said: {reading}"
    );

    // The failing service's duty cycle ends; the module acked its null
    // recording id first, so the action is not left outstanding.
    let exit = cluster.await_container_exit(0, 0);
    assert!(
        matches!(exit, ContainerExit::Failed(_)),
        "the service whose snapshot threw ended with an error, got {exit:?}"
    );

    // Its neighbour is untouched, and no snapshot was counted.
    assert!(
        cluster.container_is_running(0, 1),
        "the other service is still running"
    );
    assert_eq!(
        cluster
            .counter(CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)
            .unwrap_or(0),
        0,
        "a failed snapshot does not advance the module's snapshot counter"
    );

    // The cluster still takes messages, and the live service still applies
    // them.
    let reading = echo(&cluster, &aeron_dir, 1);
    assert!(
        reading.contains("RECEIVED 1"),
        "the surviving service answered, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );
    wait_for("the surviving service to count the second message", || {
        second.messages() >= 2
    });

    // The reference's own recovery shape (`MultiClusteredServicesTest.java:120-135`):
    // stop the cluster and start it again over the same directories, rather
    // than putting one container back into a module that has already seen a
    // service leave. A fresh business, a fresh state, and the replayed messages
    // in it.
    cluster.restart_node(0);
    cluster.await_active();

    let restarted = cluster.test_service(0, 0).expect("service 0's new state");
    wait_for("the restarted service to replay the log", || {
        restarted.messages() >= 2
    });

    // And from here both services answer again.
    let reading = echo(&cluster, &aeron_dir, SERVICE_COUNT as usize);
    assert!(
        reading.contains(&format!("RECEIVED {SERVICE_COUNT}")),
        "both services answered after the restart, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );

    cluster.shutdown();
}
