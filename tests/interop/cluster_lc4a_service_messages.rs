//! LC-4a's service-message scenario: **a client message becomes a service
//! message and comes back to the client, once**.
//!
//! The reference's `shouldEchoServiceIpcMessages`
//! (`ServiceIpcIngressMessageTest.java:54-73`) sends the one message
//! `TestNode.TestService` treats specially (`TestNode.java:669-690`,
//! `ClusterTests.java:40`): the service offers it back **into** the cluster,
//! the log carries it to every service, and the service message that comes out
//! has no client session — so the service echoes it to every session.
//!
//! What the reference asserts is two counts: the client's answers
//! (`awaitResponseMessageCount`) and the services' own message counters
//! (`awaitServicesMessageCount`). The second is a stub-service assertion — it
//! reads a business's field inside the test JVM — and this build owes it only
//! in the in-process arm (plan §4.5: the external and all-Java arms record the
//! loss instead of pretending to it). The first is the one all three arms can
//! make, and it is the one that catches a duplicate: the client has to see
//! exactly one answer per message, not one per service and not two.
//!
//! The three arms are the plan's: our container in process, our container as
//! the binary, and the reference's container running the Java twin. The last is
//! the fixture's self-check.

use std::sync::Mutex;

use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster};

const CLUSTER_ID: i32 = 9;
const MESSAGE_COUNT: usize = 10;

static SERIAL: Mutex<()> = Mutex::new(());

fn start(test_name: &str, member: MemberSpec) -> Option<TestCluster> {
    let options = Options {
        cluster_id: CLUSTER_ID,
        probes: vec![
            "ClusterServiceMessageClientProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterEchoTimerService.java",
        ],
        ..Options::default()
    };
    TestCluster::start(test_name, &[member], options)
}

fn arms() -> [(&'static str, MemberSpec); 3] {
    [
        (
            "in-process",
            MemberSpec::our_container(0, "echo", ProcessMode::InProcess),
        ),
        (
            "external",
            MemberSpec::our_container(0, "echo", ProcessMode::External),
        ),
        (
            "java",
            MemberSpec::java_container(0, "ClusterEchoTimerService"),
        ),
    ]
}

fn service_messages(test_name: &str, member: MemberSpec) {
    let Some(mut cluster) = start(test_name, member) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let reading = cluster.run_probe(
        "ClusterServiceMessageClientProbe",
        &[aeron_dir.as_str(), &MESSAGE_COUNT.to_string(), "30"],
    );

    assert!(
        reading.contains(&format!("RECEIVED {MESSAGE_COUNT}")),
        "each service message came back exactly once, client said: {reading}\nnode log:\n{}\n{}",
        cluster.node_log(0),
        cluster.container_logs()
    );

    cluster.shutdown();
}

#[test]
fn a_service_message_round_trips_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages("lc4a-service-message-in-process", arms()[0].1);
}

#[test]
fn a_service_message_round_trips_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages("lc4a-service-message-external", arms()[1].1);
}

#[test]
fn the_reference_container_round_trips_a_service_message_the_same_way() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages("lc4a-service-message-java", arms()[2].1);
}
