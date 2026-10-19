//! The tool's file-reading commands, against files this build writes.
//!
//! These are the scenarios of `ClusterToolTest` that never start a cluster:
//! `shouldFailIfMarkFileUnavailable`, the `sortRecordingLog*` and
//! `seedRecordingLogFromSnapshot*` families (their own test file), and the
//! mark-file shape `describe` and `errors` print.

use std::path::{Path, PathBuf};

use deepmsg_cluster::persist::recording_log::{CONSENSUS_MODULE_SERVICE_ID, RecordingLog};
use deepmsg_cluster_common::mark::{self, ClusterMarkFile, ERROR_BUFFER_MIN_LENGTH};
use deepmsg_cluster_tool::ToolProperties;
use deepmsg_cluster_tool::command;
use deepmsg_cluster_tool::operator::{ClusterTool, FAILURE};
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-cluster-tool-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn now_ms() -> i64 {
    1_700_000_000_000
}

/// A ready consensus-module mark file, in `directory`, with the six var-data
/// strings the reference writes (`ConsensusModule.java:4564-4590`).
fn consensus_module_mark_file(directory: &Path, aeron_dir: &str) -> ClusterMarkFile {
    let path = directory.join(mark::FILENAME);
    let file = ClusterMarkFile::create(
        &path,
        ClusterComponentType::CONSENSUS_MODULE,
        ERROR_BUFFER_MIN_LENGTH,
        4096,
        now_ms(),
        4242,
    )
    .unwrap();
    file.set_cluster_id(7);
    file.set_member_id(1);
    file.set_stream_ids(1, 104, 105, 101);
    file.write_var_data(&[
        aeron_dir,
        "aeron:ipc?term-length=128k",
        "aeron:udp?endpoint=localhost:20111",
        "",
        "io.aeron.security.DefaultAuthenticatorSupplier",
        &directory.to_string_lossy(),
    ])
    .unwrap();
    file.signal_ready(now_ms()).unwrap();
    file
}

fn service_mark_file(directory: &Path, service_id: i32) -> ClusterMarkFile {
    let path = directory.join(mark::service_filename(service_id));
    let file = ClusterMarkFile::create(
        &path,
        ClusterComponentType::CONTAINER,
        ERROR_BUFFER_MIN_LENGTH,
        4096,
        now_ms(),
        5252,
    )
    .unwrap();
    file.set_cluster_id(7);
    file.set_service_id(service_id);
    file.signal_ready(now_ms()).unwrap();
    file
}

#[test]
fn describe_prints_the_header_and_the_service_mark_files() {
    let dir = scratch("describe");
    let _mark_file = consensus_module_mark_file(&dir, "/tmp/aeron-dir");
    let _service_file = service_mark_file(&dir, 0);

    let mut out = Vec::new();
    let status = ClusterTool::default().describe(&dir, &mut out);
    let text = String::from_utf8(out).unwrap();

    assert_eq!(status, 0);
    assert!(text.contains("Type: CONSENSUS_MODULE "), "{text}");
    assert!(
        text.contains("[MarkFileHeader](sbeTemplateId=200"),
        "{text}"
    );
    assert!(text.contains("componentType=CONSENSUS_MODULE"), "{text}");
    assert!(text.contains("pid=4242"), "{text}");
    assert!(text.contains("aeronDirectory='/tmp/aeron-dir'"), "{text}");
    // The service mark file the services directory names.
    assert!(text.contains("Type: CONTAINER "), "{text}");
    assert!(text.contains("serviceId=0"), "{text}");
    assert!(text.contains("pid=5252"), "{text}");
}

#[test]
fn describe_without_a_mark_file_prints_the_reference_message_and_succeeds() {
    let dir = scratch("describe-empty");
    let mut out = Vec::new();
    let status = ClusterTool::default().describe(&dir, &mut out);

    assert_eq!(status, 0, "`describe` is wrapped in `ignoreFailures`");
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "cluster-mark.dat does not exist.\n"
    );
}

#[test]
fn pid_prints_the_pid_the_mark_file_carries() {
    let dir = scratch("pid");
    let _mark_file = consensus_module_mark_file(&dir, "/tmp/aeron-dir");

    let mut out = Vec::new();
    assert_eq!(ClusterTool::default().pid(&dir, &mut out), 0);
    assert_eq!(String::from_utf8(out).unwrap(), "4242\n");
}

#[test]
fn pid_without_a_mark_file_fails() {
    let dir = scratch("pid-empty");
    let mut out = Vec::new();
    assert_eq!(ClusterTool::default().pid(&dir, &mut out), FAILURE);
    assert!(out.is_empty());
}

#[test]
fn a_link_file_points_the_tool_at_the_real_directory() {
    let cluster_dir = scratch("link-cluster");
    let mark_dir = scratch("link-mark");
    let _mark_file = consensus_module_mark_file(&mark_dir, "/tmp/aeron-dir");
    std::fs::write(
        cluster_dir.join(mark::LINK_FILENAME),
        mark_dir.to_string_lossy().as_bytes(),
    )
    .unwrap();

    let mut out = Vec::new();
    assert_eq!(ClusterTool::default().pid(&cluster_dir, &mut out), 0);
    assert_eq!(String::from_utf8(out).unwrap(), "4242\n");
}

#[test]
fn recording_log_prints_the_sorted_view_with_the_disk_index() {
    let dir = scratch("recording-log");
    let path = dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
    {
        let mut log = RecordingLog::open(&path).unwrap();
        log.append_term(1, 0, 100, 1, 10, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(1, 0, 100, 2, 11, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
    }

    let mut out = Vec::new();
    assert_eq!(ClusterTool::default().recording_log(&dir, &mut out), 0);
    let text = String::from_utf8(out).unwrap();
    assert!(text.starts_with("RecordingLog{entries=[Entry{"), "{text}");
    assert!(text.contains("type=TERM"), "{text}");
    assert!(text.contains("type=SNAPSHOT"), "{text}");
    assert!(text.contains("entryIndex=0"), "{text}");
    assert!(text.contains("entryIndex=1"), "{text}");
    assert!(text.contains("cacheIndex={1=0}"), "{text}");
}

#[test]
fn invalidate_latest_snapshot_prints_the_reference_line() {
    let dir = scratch("invalidate");
    let path = dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
    {
        let mut log = RecordingLog::open(&path).unwrap();
        log.append_term(1, 0, 100, 1, 10, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(1, 0, 100, 2, 11, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
    }

    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().invalidate_latest_snapshot(&dir, &mut out),
        0
    );
    assert_eq!(
        String::from_utf8(out).unwrap(),
        " invalidate latest snapshot: true\n"
    );

    let reopened = RecordingLog::open(&path).unwrap();
    assert!(!reopened.entries()[1].is_valid);
}

#[test]
fn errors_without_a_mark_file_prints_the_reference_message_and_succeeds() {
    let dir = scratch("errors-empty");
    let mut out = Vec::new();
    assert_eq!(ClusterTool::default().errors(&dir, &mut out), 0);
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "cluster-mark.dat does not exist.\n"
    );
}

#[test]
fn a_command_line_without_a_cluster_directory_prints_the_help() {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let status = command::run(&["x".to_owned()], &mut out, &mut err);
    assert_eq!(status, FAILURE);
    assert!(
        String::from_utf8(out)
            .unwrap()
            .starts_with(command::HELP_PREFIX)
    );
}

#[test]
fn a_missing_cluster_directory_is_reported_on_stderr() {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let status = command::run(
        &["/definitely/not/a/cluster".to_owned(), "pid".to_owned()],
        &mut out,
        &mut err,
    );
    assert_eq!(status, FAILURE);
    assert!(
        String::from_utf8(err)
            .unwrap()
            .starts_with("ERR: cluster directory not found: ")
    );
}

#[test]
fn an_unknown_command_is_refused_by_name() {
    let dir = scratch("unknown");
    let mut out = Vec::new();
    let mut err = Vec::new();
    let status = command::run(
        &[
            dir.to_string_lossy().into_owned(),
            "standby-snapshot".to_owned(),
        ],
        &mut out,
        &mut err,
    );
    assert_eq!(status, FAILURE);
    assert!(
        String::from_utf8(out)
            .unwrap()
            .starts_with("Unknown command: standby-snapshot")
    );
}

#[test]
fn the_usage_lists_every_registered_command() {
    let usage = command::usage();
    for spec in command::COMMANDS {
        assert!(usage.contains(spec.name), "{} missing", spec.name);
    }
    assert_eq!(command::COMMANDS.len(), 18);
}

#[test]
fn the_tool_properties_are_the_references_defaults() {
    let properties = ToolProperties::default();
    assert_eq!(properties.timeout_ms, 0);
    assert_eq!(properties.delay_ms, 0);
    assert_eq!(properties.replay_channel, "aeron:ipc");
    assert_eq!(properties.replay_stream_id, 103);
}

#[test]
fn backup_query_without_a_mark_file_fails() {
    let dir = scratch("backup-empty");
    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().backup_query(&dir, &mut out, None),
        FAILURE
    );
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "cluster-mark.dat does not exist.\n"
    );
}

#[test]
fn backup_query_on_a_consensus_module_is_not_a_backup_node() {
    let dir = scratch("backup-not-a-backup");
    let _mark_file = consensus_module_mark_file(&dir, "/tmp/aeron-dir");
    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().backup_query(&dir, &mut out, None),
        FAILURE
    );
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "not a cluster backup node\n"
    );
}

#[test]
fn describe_latest_cm_snapshot_without_a_recording_log_says_snapshot_not_found() {
    let dir = scratch("describe-snapshot-missing");
    let _mark_file = consensus_module_mark_file(&dir, "/tmp/aeron-dir");
    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().describe_latest_cm_snapshot(&dir, &mut out),
        FAILURE
    );
    assert_eq!(String::from_utf8(out).unwrap(), "Snapshot not found\n");
}
