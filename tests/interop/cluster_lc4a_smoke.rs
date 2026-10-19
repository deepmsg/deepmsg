//! P3-2a/P3-2c's LC-4a smoke: **the reference's driver, archive and consensus
//! module, deepmsg's service container, the reference's client**.
//!
//! Everything else in this workspace's cluster suite either puts our own fake
//! service on streams 104/105 or our own consensus module on the other side. A
//! container that only ever met our own module would be two halves of one
//! implementation agreeing; here the far side of S2 is the reference's own
//! `ConsensusModuleAgent` inside `ClusteredMediaDriver`, and the client is the
//! reference's own `AeronCluster`.
//!
//! # Three arms
//!
//! Each scenario runs with the container started three ways (plan §4.5, Q4):
//!
//! * **in process** — on a thread of this test, the mode that can read a
//!   business's own state (the reference's harness reads `messageCount` out of
//!   its JVM the same way);
//! * **external** — the `deepmsg-clustered-service-container` binary, which is
//!   the path a deployment and LC-3 take;
//! * **all Java** — the reference's own container running
//!   `ClusterEchoTimerService`, the fixture's self-check: if it is red, the
//!   fixture reads the scenario wrong and the other two arms would be testing
//!   the wrong thing.
//!
//! # One at a time
//!
//! Each test owns a whole cluster on `/dev/shm` names and a Java process, and
//! the tests in this binary share a process; [`SERIAL`] keeps them from
//! meeting.

use std::sync::Mutex;

use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

/// A single-member cluster whose one container is `member`.
fn start(test_name: &str, member: MemberSpec) -> Option<TestCluster> {
    let options = Options {
        cluster_id: 7,
        probes: vec![
            "ClusterEchoClientProbe.java",
            "ClusterTimerClientProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterEchoTimerService.java",
        ],
        ..Options::default()
    };
    TestCluster::start(test_name, &[member], options)
}

/// The three ways to run the container.
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

/// One echo, then ten thousand, through one session.
fn echoes(test_name: &str, member: MemberSpec) {
    let Some(mut cluster) = start(test_name, member) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let reading = cluster.run_probe("ClusterEchoClientProbe", &[aeron_dir.as_str(), "1", "20"]);
    assert!(
        reading.contains("RECEIVED 1"),
        "one echo came back, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );

    let reading = cluster.run_probe(
        "ClusterEchoClientProbe",
        &[aeron_dir.as_str(), "10000", "60"],
    );
    assert!(
        reading.contains("RECEIVED 10000"),
        "ten thousand echoes came back, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );

    cluster.shutdown();
}

/// A timer fires, a cancelled timer does not, and a third one fires.
fn timers(test_name: &str, member: MemberSpec) {
    let Some(mut cluster) = start(test_name, member) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let reading = cluster.run_probe("ClusterTimerClientProbe", &[aeron_dir.as_str(), "30"]);
    assert!(
        reading.contains("TIMER 1"),
        "the scheduled timer fired, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );
    assert!(
        reading.contains("CANCELLED 2"),
        "the cancelled timer did not fire, client said: {reading}"
    );
    assert!(
        reading.contains("TIMER 3"),
        "the third timer fired, client said: {reading}"
    );

    cluster.shutdown();
}

#[test]
fn a_java_cluster_echoes_through_our_container_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    echoes("lc4a-echo-in-process", arms()[0].1);
}

#[test]
fn a_java_cluster_echoes_through_our_container_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    echoes("lc4a-echo-external", arms()[1].1);
}

#[test]
fn the_reference_container_echoes_the_same_way() {
    let _serial = SERIAL.lock().expect("the serial lock");
    echoes("lc4a-echo-java", arms()[2].1);
}

#[test]
fn timers_schedule_cancel_and_expire_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    timers("lc4a-timers-in-process", arms()[0].1);
}

#[test]
fn timers_schedule_cancel_and_expire_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    timers("lc4a-timers-external", arms()[1].1);
}

#[test]
fn the_reference_container_schedules_and_cancels_the_same_way() {
    let _serial = SERIAL.lock().expect("the serial lock");
    timers("lc4a-timers-java", arms()[2].1);
}
