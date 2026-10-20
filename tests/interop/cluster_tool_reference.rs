//! The reference's own `ClusterTool` against the directories our nodes write.
//!
//! This is LC-5's other half and the card's "use the reference's tool" arm:
//! `java -cp aeron-all.jar io.aeron.cluster.ClusterTool <dir> describe` and
//! friends must read our mark files, our `recording.log` and our CnC file the
//! same way they read a reference deployment's. `ClusterTool.java:239-270`
//! takes the directory first, the command second.
//!
//! The cluster is the three-node harness (P3-6a/6b); the tool is the
//! reference's, run as a process.

use std::sync::mpsc;

use deepmsg_tests::cluster_harness::{
    ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_LEADER, announce_skip,
    await_launched, members, wait_until,
};
use deepmsg_tests::driver;
use deepmsg_tests::java;

#[test]
fn the_reference_tool_reads_what_our_cluster_wrote() {
    let Some(jar) = driver::locate_aeron_all() else {
        eprintln!("SKIPPED: the reference's aeron-all jar");
        return;
    };

    let ports = Ports::new();
    let member_string = members(&ports);
    let faults = Faults::default();
    let (launched_tx, launched_rx) = mpsc::channel();
    let mut nodes = Vec::new();
    for id in 0..MEMBER_COUNT {
        let Some(node) = Node::start(id, &ports, &member_string, &faults, &launched_tx) else {
            announce_skip();
            return;
        };
        nodes.push(node);
    }
    await_launched(&faults, &launched_rx, MEMBER_COUNT);

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();
    wait_until("every member's election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });
    let leader = counters
        .iter()
        .position(|node| node.role() == Some(ROLE_LEADER))
        .expect("one leader");
    let cluster_dir = nodes[leader].cluster_dir();

    // ---- describe: the mark file's header, in the SBE generated format ------
    let described = java::cluster_tool(&jar, cluster_dir, &["describe"]);
    assert!(
        described.contains("Type: CONSENSUS_MODULE "),
        "describe said: {described}"
    );
    assert!(
        described.contains("[MarkFileHeader](sbeTemplateId=200"),
        "describe said: {described}"
    );
    assert!(
        described.contains("componentType=CONSENSUS_MODULE"),
        "describe said: {described}"
    );

    // ---- pid: the process that wrote the mark file --------------------------
    let pid = java::cluster_tool(&jar, cluster_dir, &["pid"]);
    assert_eq!(
        std::process::id().to_string(),
        pid.trim(),
        "the nodes run in this process"
    );

    // ---- recording-log ------------------------------------------------------
    let log = java::cluster_tool(&jar, cluster_dir, &["recording-log"]);
    assert!(log.starts_with("RecordingLog{entries=["), "said: {log}");
    assert!(log.contains("type=TERM"), "said: {log}");

    // ---- is-leader: zero on the leader --------------------------------------
    let leader_answer = java::cluster_tool(&jar, cluster_dir, &["is-leader"]);
    assert!(
        leader_answer.is_empty(),
        "is-leader prints nothing: {leader_answer}"
    );

    for mut node in nodes {
        node.stop();
    }
}
