//! LC-4a's multi-service scenario: **two containers on one node, both answer**.
//!
//! The reference's `MultiClusteredServicesTest.shouldSupportMultipleServicesPerNode`
//! (`MultiClusteredServicesTest.java:66-86`) runs three nodes with two services
//! each and asserts, for three client messages, that **each** service counted
//! three. The message counts are a stub-service assertion; the client-visible
//! half — two answers per message, one per service — is what all three arms can
//! see, and it is the half that catches a container whose egress is missing or
//! doubled.
//!
//! This file is the single-node form. The three-node form (leadership, and the
//! `service.count` a follower carries) is the same fixture with a members string
//! and UDP channels, and belongs to the failover scenarios; the container side —
//! two services on one node, both applying one log — is what is asserted here.
//!
//! Three arms, as everywhere in LC-4a: our container in process (where the
//! service counters are readable, and the reference's assertion is ported
//! whole), our container as two binaries, and the reference's container running
//! the Java twin twice.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster};

const CLUSTER_ID: i32 = 10;
const MESSAGE_COUNT: usize = 3;
const SERVICE_COUNT: usize = 2;

static SERIAL: Mutex<()> = Mutex::new(());

fn start(test_name: &str, members: [MemberSpec; SERVICE_COUNT]) -> Option<TestCluster> {
    let options = Options {
        cluster_id: CLUSTER_ID,
        probes: vec![
            "ClusterEchoClientProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterEchoTimerService.java",
        ],
        ..Options::default()
    };
    TestCluster::start(test_name, &members, options)
}

/// Two containers on node 0, with service ids 0 and 1.
fn pair(service: impl Fn(i32) -> MemberSpec) -> [MemberSpec; SERVICE_COUNT] {
    [service(0).with_service_id(0), service(1).with_service_id(1)]
}

/// Send `MESSAGE_COUNT` messages and expect one answer per message per service.
fn two_services_answer_every_message(test_name: &str, members: [MemberSpec; SERVICE_COUNT]) {
    let Some(mut cluster) = start(test_name, members) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let expected = MESSAGE_COUNT * SERVICE_COUNT;
    let reading = cluster.run_probe(
        "ClusterEchoClientProbe",
        &[
            aeron_dir.as_str(),
            &MESSAGE_COUNT.to_string(),
            "30",
            &expected.to_string(),
        ],
    );
    assert!(
        reading.contains(&format!("RECEIVED {expected}")),
        "both services answered every message, client said: {reading}\nnode log:\n{}\n{}",
        cluster.node_log(0),
        cluster.container_logs()
    );

    cluster.shutdown();
}

#[test]
fn two_services_on_one_node_answer_every_message_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");

    let Some(mut cluster) = start(
        "lc4a-multi-service-in-process",
        pair(|_| MemberSpec::test_container(0, "test-service")),
    ) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let expected = MESSAGE_COUNT * SERVICE_COUNT;
    let reading = cluster.run_probe(
        "ClusterEchoClientProbe",
        &[
            aeron_dir.as_str(),
            &MESSAGE_COUNT.to_string(),
            "30",
            &expected.to_string(),
        ],
    );
    assert!(
        reading.contains(&format!("RECEIVED {expected}")),
        "both services answered every message, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );

    // The reference's own assertion: each service counted every message.
    let deadline = Instant::now() + Duration::from_secs(10);
    for service_id in 0..SERVICE_COUNT as i32 {
        let state = cluster
            .test_service(0, service_id)
            .expect("the in-process arm has a test business");
        while state.messages() < MESSAGE_COUNT && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            state.messages(),
            MESSAGE_COUNT,
            "service {service_id} counted every message"
        );
    }

    cluster.shutdown();
}

#[test]
fn two_services_on_one_node_answer_every_message_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    two_services_answer_every_message(
        "lc4a-multi-service-external",
        pair(|_| MemberSpec::our_container(0, "echo", ProcessMode::External)),
    );
}

#[test]
fn the_reference_containers_answer_the_same_way() {
    let _serial = SERIAL.lock().expect("the serial lock");
    two_services_answer_every_message(
        "lc4a-multi-service-java",
        pair(|_| MemberSpec::java_container(0, "ClusterEchoTimerService")),
    );
}
