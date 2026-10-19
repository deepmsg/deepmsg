//! P3-8's live half: the operator's tool against three pure-Rust nodes.
//!
//! The scenarios are `ClusterToolTest`'s (`ClusterToolTest.java:70-221`,
//! `:370-398`) run on this build's own cluster: `is-leader`,
//! `shouldHandleSnapshotOnLeaderOnly`, `shouldNotSnapshotWhenSuspendedOnly`,
//! `shouldSuspendAndResume`, plus `describe`, `pid`, `recording-log` and
//! `list-members`. One test rather than several, because the three nodes share
//! one port allocation and this binary's tests run as parallel threads.

use std::sync::mpsc;

use deepmsg_client::client::Client;
use deepmsg_cluster::state::ModuleState;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
};
use deepmsg_cluster_common::mark::{self, ClusterMarkFile, ERROR_BUFFER_MIN_LENGTH};
use deepmsg_cluster_tool::ClusterTool;
use deepmsg_cluster_tool::operator::FAILURE;
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_tests::cluster_harness::{
    ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_LEADER, announce_skip,
    await_launched, members, wait_until,
};

/// Turn a tool's output into text.
fn text(out: &[u8]) -> String {
    String::from_utf8(out.to_vec()).expect("the tool writes UTF-8")
}

#[test]
fn the_tool_operates_a_three_node_cluster() {
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
    let follower = (0..MEMBER_COUNT as usize)
        .find(|id| *id != leader)
        .expect("a follower");
    let tool = ClusterTool::default();

    // ---- is-leader: zero on the leader, one elsewhere ----------------------
    for (id, node) in nodes.iter().enumerate() {
        let mut out = Vec::new();
        let status = tool.is_leader(node.cluster_dir(), &mut out);
        if id == leader {
            assert_eq!(0, status, "the leader: {}", text(&out));
        } else {
            assert_eq!(1, status, "a follower: {}", text(&out));
        }
        assert!(out.is_empty(), "`is-leader` prints nothing");
    }

    // ---- describe / pid / recording-log on a real component ---------------
    let mut out = Vec::new();
    assert_eq!(0, tool.describe(nodes[leader].cluster_dir(), &mut out));
    let described = text(&out);
    assert!(
        described.contains("[MarkFileHeader](sbeTemplateId=200"),
        "{described}"
    );
    assert!(
        described.contains("componentType=CONSENSUS_MODULE"),
        "{described}"
    );
    assert!(described.contains("clusterId="), "{described}");

    let mut out = Vec::new();
    assert_eq!(0, tool.pid(nodes[leader].cluster_dir(), &mut out));
    assert_eq!(
        std::process::id().to_string(),
        text(&out).trim(),
        "the module runs in this process, so its mark file names this pid"
    );

    let mut out = Vec::new();
    assert_eq!(0, tool.recording_log(nodes[leader].cluster_dir(), &mut out));
    let recording_log = text(&out);
    assert!(
        recording_log.starts_with("RecordingLog{entries=[Entry{"),
        "{recording_log}"
    );
    assert!(recording_log.contains("type=TERM"), "{recording_log}");

    // ---- list-members ------------------------------------------------------
    let mut out = Vec::new();
    assert_eq!(0, tool.list_members(nodes[leader].cluster_dir(), &mut out));
    let members_text = text(&out);
    assert!(
        members_text.contains(&format!("leaderMemberId={leader}")),
        "{members_text}"
    );
    assert!(
        members_text.contains(&format!("memberId={leader}")),
        "{members_text}"
    );
    let listed = members_text.matches("ClusterMember{id=").count();
    assert_eq!(MEMBER_COUNT as usize, listed, "{members_text}");

    // ---- snapshot: the leader only ----------------------------------------
    let mut out = Vec::new();
    assert_eq!(
        FAILURE,
        tool.toggle(nodes[follower].cluster_dir(), &mut out, "snapshot")
    );
    let refusal = text(&out);
    assert!(
        refusal.contains(&format!(
            "Current node is not the leader (leaderMemberId = {leader}), unable to SNAPSHOT"
        )),
        "{refusal}"
    );

    let before = counters[leader].snapshot_count().unwrap_or(0);
    let mut out = Vec::new();
    assert_eq!(
        0,
        tool.toggle(nodes[leader].cluster_dir(), &mut out, "snapshot")
    );
    assert!(
        text(&out).contains("SNAPSHOT applied successfully"),
        "{}",
        text(&out)
    );
    wait_until("the snapshot counter to move", &faults, || {
        counters[leader].snapshot_count().unwrap_or(0) > before
    });

    // ---- describe-latest-cm-snapshot --------------------------------------
    let mut out = Vec::new();
    assert_eq!(
        0,
        tool.describe_latest_cm_snapshot(nodes[leader].cluster_dir(), &mut out)
    );
    let snapshot = text(&out);
    assert!(
        snapshot.contains("Snapshot: appVersion=1 timeUnit=MILLISECONDS"),
        "{snapshot}"
    );
    assert!(
        snapshot.contains(&format!(
            "Consensus Module Snapshot End: memberId={leader} recordingId="
        )),
        "{snapshot}"
    );

    // ---- suspend, and a snapshot that must be refused ---------------------
    let mut out = Vec::new();
    assert_eq!(
        0,
        tool.toggle(nodes[leader].cluster_dir(), &mut out, "suspend")
    );
    assert!(
        text(&out).contains("SUSPEND applied successfully"),
        "{}",
        text(&out)
    );
    wait_until("the module to report SUSPENDED", &faults, || {
        counters[leader].value(CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)
            == Some(i64::from(ModuleState::Suspended.code()))
    });

    // The reference's `shouldCheckForLeaderInAnyStateAfterElectionWasClosed`:
    // a suspended leader is still the leader.
    let mut out = Vec::new();
    assert_eq!(0, tool.is_leader(nodes[leader].cluster_dir(), &mut out));
    let mut out = Vec::new();
    assert_eq!(1, tool.is_leader(nodes[follower].cluster_dir(), &mut out));

    let mut out = Vec::new();
    assert_eq!(
        FAILURE,
        tool.toggle(nodes[leader].cluster_dir(), &mut out, "snapshot")
    );
    let refused = text(&out);
    assert!(
        refused.contains(
            "Unable to SNAPSHOT as the state of the consensus module is SUSPENDED, \
             but needs to be ACTIVE"
        ),
        "{refused}"
    );
    assert_eq!(
        before + 1,
        counters[leader].snapshot_count().unwrap_or(0),
        "a refused snapshot takes none"
    );

    let mut out = Vec::new();
    assert_eq!(
        0,
        tool.toggle(nodes[leader].cluster_dir(), &mut out, "resume")
    );
    assert!(
        text(&out).contains("RESUME applied successfully"),
        "{}",
        text(&out)
    );
    wait_until("the module to report ACTIVE", &faults, || {
        counters[leader].value(CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)
            == Some(i64::from(ModuleState::Active.code()))
    });

    // ---- backup-query on a BACKUP mark file --------------------------------
    let backup_dir = std::env::temp_dir().join(format!(
        "deepmsg-cluster-tool-backup-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&backup_dir);
    std::fs::create_dir_all(&backup_dir).unwrap();
    let now_ms = deepmsg_core::clock::epoch_millis();
    let backup_mark = ClusterMarkFile::create(
        &backup_dir.join(mark::FILENAME),
        ClusterComponentType::BACKUP,
        ERROR_BUFFER_MIN_LENGTH,
        4096,
        now_ms,
        i64::from(std::process::id()),
    )
    .unwrap();
    backup_mark.set_cluster_id(7);
    backup_mark
        .write_var_data(&[
            &nodes[leader].aeron_dir().to_string_lossy(),
            "aeron:ipc?term-length=128k",
            "",
            "",
            "",
            "",
        ])
        .unwrap();
    backup_mark.signal_ready(now_ms).unwrap();

    let mut operator = Client::connect(nodes[leader].aeron_dir()).expect("the operator connects");
    let spec = counters::cluster_counter("ClusterBackup next query deadline in ms", 7)
        .expect("the counter spec");
    let counter = operator
        .add_counter(
            CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID,
            spec.key(),
            spec.label(),
            std::time::Duration::from_secs(5),
        )
        .expect("the deadline counter is allocated");
    operator
        .counters_writable()
        .expect("writable counters")
        .set_value(counter.counter_id(), 1_700_000_000_000)
        .unwrap();

    // `listMembersShouldReturnFalseIfQueryTimesOut`: the BACKUP mark file
    // names a live driver, but no consensus module answers on its control
    // channel.
    let mut out = Vec::new();
    assert_eq!(FAILURE, tool.list_members(&backup_dir, &mut out));
    assert_eq!(
        "timeout waiting for response from node\n",
        text(&out),
        "the query times out"
    );

    let mut out = Vec::new();
    assert_eq!(0, tool.backup_query(&backup_dir, &mut out, None));
    assert!(text(&out).contains("next: "), "{}", text(&out));

    let mut out = Vec::new();
    assert_eq!(0, tool.backup_query(&backup_dir, &mut out, Some(5_000)));
    assert!(text(&out).contains("setting next: "), "{}", text(&out));

    for mut node in nodes {
        node.stop();
    }
}
