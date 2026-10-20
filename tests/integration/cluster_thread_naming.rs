//! `ClusterThreadNamingTest`'s first scenario, on this build's own threads
//! (`ClusterThreadNamingTest.java:62-123`).
//!
//! The reference reads `ThreadMXBean` names in the JVM and gets the full
//! `consensus-module-0-0`; the kernel keeps **15 bytes**, so this test asserts
//! the name reaches the operating system and the configuration test in
//! `deepmsg_cluster::config` asserts the full spelling for both naming modes.
//! The backup arm (`cluster-backup` / `aeron-cl-bu`) is P3-9's.

use std::sync::mpsc;

use deepmsg_tests::cluster_harness::{
    ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, announce_skip,
    await_launched, members, wait_until,
};

/// Every live thread's kernel name (`/proc/self/task/<tid>/comm`).
fn task_names() -> Vec<String> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    tasks
        .filter_map(Result::ok)
        .filter_map(|task| std::fs::read_to_string(task.path().join("comm")).ok())
        .map(|name| name.trim_end_matches('\n').to_owned())
        .collect()
}

#[test]
fn every_node_thread_carries_the_consensus_module_name() {
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

    // `consensus-module-<clusterId>-<memberId>` cut at the kernel's 15 bytes.
    let names = task_names();
    let named = names
        .iter()
        .filter(|name| name.as_str() == "consensus-modul")
        .count();
    assert!(
        named >= MEMBER_COUNT as usize,
        "each node's thread is named: {names:?}"
    );

    for mut node in nodes {
        node.stop();
    }
}
