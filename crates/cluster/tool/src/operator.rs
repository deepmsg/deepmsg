//! The operator's commands, the counterpart of `ClusterToolOperator.java`.
//!
//! One method per subcommand, each returning the reference's exit status
//! (`SUCCESS = 0`, `FAILURE = -1`; `ClusterToolOperator.java:85-87`) and writing
//! what the reference writes. The methods take `&mut dyn Write` rather than a
//! `PrintStream` so a test can capture the output in-process, which is what the
//! reference's `CapturingPrintStream` does for `ClusterToolTest`.

use std::io::Write;
use std::path::{Path, PathBuf};

use deepmsg_archive::client::context::ArchiveContext;
use deepmsg_archive::client::descriptor_poller::RecordingDescriptor;
use deepmsg_archive::client::{Archive, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_cluster::persist::recording_log::{Entry, RecordingExtent, RecordingLog, RecoveryPlan};

use crate::mark_files;
use crate::properties::ToolProperties;

/// `ClusterToolOperator.SUCCESS`.
pub const SUCCESS: i32 = 0;
/// `ClusterToolOperator.FAILURE`.
pub const FAILURE: i32 = -1;

/// The name the reference gives its archive client (`ClusterToolOperator.java:147`).
const TOOL_CLIENT_NAME: &str = "cluster-tool";

/// One operator, with the tool's four settings resolved.
#[derive(Clone, Debug)]
pub struct ClusterTool {
    properties: ToolProperties,
    /// Where the archive is, for the two commands that talk to one. The
    /// reference's `AeronArchive.Context` takes it from `aeron.dir`
    /// (`recovery-plan` and `describe-latest-cm-snapshot` are the two); the
    /// cluster mark file's `aeronDirectory` is used where the command reads a
    /// mark file first and the reference does the same.
    aeron_dir: String,
}

impl Default for ClusterTool {
    fn default() -> Self {
        Self::new(ToolProperties::default())
    }
}

impl ClusterTool {
    /// With the tool's settings, and the default Aeron directory.
    #[must_use]
    pub fn new(properties: ToolProperties) -> Self {
        Self {
            properties,
            aeron_dir: deepmsg_archive::client::context::default_aeron_dir(),
        }
    }

    /// With an explicit `aeron.dir`, the way `-Daeron.dir=…` supplies one.
    #[must_use]
    pub fn with_aeron_dir(mut self, aeron_dir: impl Into<String>) -> Self {
        self.aeron_dir = aeron_dir.into();
        self
    }

    /// The tool's settings.
    #[must_use]
    pub fn properties(&self) -> &ToolProperties {
        &self.properties
    }

    // -- describe -----------------------------------------------------------

    /// `describeClusterMarkFile` (`ClusterToolOperator.java:1132-1157`), wrapped
    /// in `ignoreFailures` by the command table (`ClusterTool.java:135-137`).
    ///
    /// Always `SUCCESS`: the command's exit status is `ignoreFailures`'s, which
    /// is the reference's own choice for `describe`.
    pub fn describe(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        let _ = self.describe_inner(cluster_dir, out);
        SUCCESS
    }

    fn describe_inner(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<(), String> {
        let service_directory = if mark_files::mark_file_exists(cluster_dir)
            || self.properties.timeout_ms > 0
        {
            let mark_file = mark_files::open_mark_file(cluster_dir, self.properties.timeout_ms)?;
            mark_files::print_type_and_activity(out, &mark_file)
                .map_err(|error| error.to_string())?;
            writeln!(out, "{}", mark_files::header_line(&mark_file))
                .map_err(|error| error.to_string())?;
            self.services_directory(cluster_dir, &mark_file)
        } else {
            writeln!(out, "cluster-mark.dat does not exist.").map_err(|error| error.to_string())?;
            return Ok(());
        };

        let service_files = mark_files::open_service_mark_files(
            &service_directory,
            self.properties.timeout_ms,
            out,
        );
        for service_file in &service_files {
            let _ = mark_files::print_type_and_activity(out, service_file);
            let _ = writeln!(out, "{}", mark_files::header_line(service_file));
        }
        Ok(())
    }

    /// `resolveClusterServicesDir` (`ClusterToolOperator.java:1162-1187`): the
    /// mark file's `servicesClusterDir`, or the cluster directory when it is
    /// empty.
    fn services_directory(
        &self,
        cluster_dir: &Path,
        mark_file: &deepmsg_cluster_common::mark::ClusterMarkFile,
    ) -> PathBuf {
        match mark_file.var_data(5) {
            Some(directory) if !directory.trim().is_empty() => PathBuf::from(directory.trim()),
            _ => cluster_dir.to_path_buf(),
        }
    }

    // -- pid ----------------------------------------------------------------

    /// `pid` (`ClusterToolOperator.java:117-131`).
    pub fn pid(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        if !mark_files::mark_file_exists(cluster_dir) && self.properties.timeout_ms <= 0 {
            return FAILURE;
        }
        let Ok(mark_file) = mark_files::open_mark_file(cluster_dir, self.properties.timeout_ms)
        else {
            return FAILURE;
        };
        let _ = writeln!(out, "{}", mark_file.pid().unwrap_or(0));
        SUCCESS
    }

    // -- recording-log ------------------------------------------------------

    /// `recordingLog` (`ClusterToolOperator.java:158-165`): the in-memory
    /// (sorted) view, with each entry's index on disk.
    pub fn recording_log(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        let path = cluster_dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
        if !path.exists() {
            let _ = writeln!(out, "recording.log does not exist");
            return FAILURE;
        }
        let Ok(log) = RecordingLog::open(&path) else {
            return FAILURE;
        };
        let _ = writeln!(out, "{}", recording_log_text(&log));
        SUCCESS
    }

    // -- recovery-plan ------------------------------------------------------

    /// `recoveryPlan` (`ClusterToolOperator.java:141-149`).
    pub fn recovery_plan(
        &self,
        cluster_dir: &Path,
        out: &mut dyn Write,
        service_count: i32,
    ) -> i32 {
        let path = cluster_dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
        if !path.exists() {
            let _ = writeln!(out, "recording.log does not exist");
            return FAILURE;
        }
        let Ok(log) = RecordingLog::open(&path) else {
            return FAILURE;
        };
        match self.create_recovery_plan(&log, service_count) {
            Ok(plan) => {
                let _ = writeln!(out, "{}", recovery_plan_text(&plan));
                SUCCESS
            }
            Err(message) => {
                let _ = writeln!(out, "{message}");
                FAILURE
            }
        }
    }

    fn create_recovery_plan(
        &self,
        log: &RecordingLog,
        service_count: i32,
    ) -> Result<RecoveryPlan, String> {
        let mut client =
            Client::connect(Path::new(&self.aeron_dir)).map_err(|error| error.to_string())?;
        let context = self.archive_context();
        let mut archive = Archive::connect(
            &context,
            &mut client,
            &mut NoCredentials,
            Handlers::default(),
        )
        .map_err(|error| error.to_string())?;
        let mut list = |recording_id: i64| list_recording(&mut archive, &mut client, recording_id);
        log.create_recovery_plan(&mut list, service_count, -1)
            .map_err(|error| error.to_string())
    }

    /// The archive context the reference's `AeronArchive.Context` defaults to:
    /// the driver in `aeron.dir`, control over `aeron:ipc`.
    fn archive_context(&self) -> ArchiveContext {
        let mut context = ArchiveContext::new();
        context.aeron_dir = self.aeron_dir.clone();
        context.client_name = TOOL_CLIENT_NAME.to_owned();
        context.control_request_channel = Some("aeron:ipc".to_owned());
        context.control_response_channel = Some("aeron:ipc".to_owned());
        context
    }

    // -- invalidate-latest-snapshot ----------------------------------------

    /// `invalidateLatestSnapshot` (`ClusterToolOperator.java:738-746`).
    pub fn invalidate_latest_snapshot(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        let path = cluster_dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
        if !path.exists() {
            let _ = writeln!(out, "recording.log does not exist");
            return FAILURE;
        }
        let Ok(mut log) = RecordingLog::open(&path) else {
            return FAILURE;
        };
        match log.invalidate_latest_snapshot() {
            Ok(result) => {
                let _ = writeln!(out, " invalidate latest snapshot: {}", result.is_some());
                if result.is_some() { SUCCESS } else { FAILURE }
            }
            Err(_) => FAILURE,
        }
    }

    // -- errors -------------------------------------------------------------

    /// `errors` (`ClusterToolOperator.java:278-308`). The reference always
    /// returns `SUCCESS`, but an unreadable mark file or driver error log
    /// throws out of it; this reports that as `FAILURE`.
    pub fn errors(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        let mut service_directory = cluster_dir.to_path_buf();
        if mark_files::mark_file_exists(cluster_dir) || self.properties.timeout_ms > 0 {
            let mark_file =
                match mark_files::open_mark_file(cluster_dir, self.properties.timeout_ms) {
                    Ok(mark_file) => mark_file,
                    Err(message) => {
                        let _ = writeln!(out, "{message}");
                        return FAILURE;
                    }
                };
            let _ = mark_files::print_type_and_activity(out, &mark_file);
            let _ = mark_files::print_mark_file_errors(out, &mark_file);
            let aeron_directory = mark_file.var_data(0).unwrap_or_default();
            let _ = writeln!(out);
            if let Err(message) =
                mark_files::print_driver_errors(out, &aeron_directory, self.properties.timeout_ms)
            {
                let _ = writeln!(out, "{message}");
                return FAILURE;
            }
            service_directory = self.services_directory(cluster_dir, &mark_file);
        } else {
            let _ = writeln!(out, "cluster-mark.dat does not exist.");
        }

        let service_files = mark_files::open_service_mark_files(
            &service_directory,
            self.properties.timeout_ms,
            out,
        );
        for service_file in &service_files {
            let _ = mark_files::print_type_and_activity(out, service_file);
            let _ = mark_files::print_mark_file_errors(out, service_file);
        }
        SUCCESS
    }

    // -- commands the later commits of this card fill in -------------------

    /// A command this build has not wired yet. Never reached once P3-8 closes.
    fn not_yet(&self, out: &mut dyn Write, name: &str) -> i32 {
        let _ = writeln!(out, "{name}: not implemented yet");
        FAILURE
    }

    /// `sortRecordingLog` (`ClusterToolOperator.java:173-192`).
    pub fn sort_recording_log(&self, _cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        self.not_yet(out, "sort-recording-log")
    }

    /// `seedRecordingLogFromSnapshot` (`ClusterToolOperator.java:201-269`).
    pub fn seed_recording_log_from_snapshot(
        &self,
        _cluster_dir: &Path,
        out: &mut dyn Write,
    ) -> i32 {
        self.not_yet(out, "seed-recording-log-from-snapshot")
    }

    /// `listMembers` (`ClusterToolOperator.java:317-347`).
    pub fn list_members(&self, _cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        self.not_yet(out, "list-members")
    }

    /// `isLeader` (`ClusterToolOperator.java:444-488`).
    pub fn is_leader(&self, _cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        self.not_yet(out, "is-leader")
    }

    /// The five switches and `validate-recording-log`.
    pub fn toggle(&self, _cluster_dir: &Path, out: &mut dyn Write, name: &str) -> i32 {
        self.not_yet(out, name)
    }

    /// `describeLatestConsensusModuleSnapshot`
    /// (`ClusterToolOperator.java:773-843`).
    pub fn describe_latest_cm_snapshot(&self, _cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        self.not_yet(out, "describe-latest-cm-snapshot")
    }

    /// `printNextBackupQuery` / `nextBackupQuery`
    /// (`ClusterToolOperator.java:356-418`).
    pub fn backup_query(
        &self,
        _cluster_dir: &Path,
        out: &mut dyn Write,
        _delay_ms: Option<i64>,
    ) -> i32 {
        self.not_yet(out, "backup-query")
    }
}

/// `listRecording`: one extent, or `None` when the archive does not know it.
fn list_recording(
    archive: &mut Archive,
    client: &mut Client,
    recording_id: i64,
) -> Option<RecordingExtent> {
    let mut extent = None;
    let _ = archive.list_recording(
        client,
        recording_id,
        &mut |descriptor: &RecordingDescriptor| {
            extent = Some(RecordingExtent {
                recording_id: descriptor.recording_id,
                start_position: descriptor.start_position,
                stop_position: descriptor.stop_position,
                initial_term_id: descriptor.initial_term_id,
                term_buffer_length: descriptor.term_buffer_length,
                mtu_length: descriptor.mtu_length,
                session_id: descriptor.session_id,
            });
        },
    );
    extent
}

// -- the reference's `toString`s -------------------------------------------

/// `RecordingLog.toString` (`RecordingLog.java:1277-1283`).
#[must_use]
pub fn recording_log_text(log: &RecordingLog) -> String {
    let entries = log.entries();
    let list = entries
        .iter()
        .map(entry_text)
        .collect::<Vec<_>>()
        .join(", ");
    let mut cache = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.is_term() {
            cache.push(format!("{}={index}", entry.leadership_term_id));
        }
    }
    format!(
        "RecordingLog{{entries=[{list}], cacheIndex={{{}}}}}",
        cache.join(", ")
    )
}

/// `RecordingLog.Entry.toString` (`RecordingLog.java:337-354`).
#[must_use]
pub fn entry_text(entry: &Entry) -> String {
    let archive_endpoint = if entry.entry_type
        == deepmsg_cluster::persist::recording_log::ENTRY_TYPE_STANDBY_SNAPSHOT
    {
        format!(
            ", archiveEndpoint={}",
            entry.archive_endpoint.as_deref().unwrap_or("")
        )
    } else {
        String::new()
    };
    format!(
        "Entry{{recordingId={}, leadershipTermId={}, termBaseLogPosition={}, logPosition={}, \
         timestamp={}, serviceId={}, type={}, isValid={}, entryIndex={}{archive_endpoint}}}",
        entry.recording_id,
        entry.leadership_term_id,
        entry.term_base_log_position,
        entry.log_position,
        entry.timestamp,
        entry.service_id,
        type_as_string(entry.entry_type),
        entry.is_valid,
        entry.entry_index,
    )
}

fn type_as_string(entry_type: i32) -> &'static str {
    match entry_type {
        deepmsg_cluster::persist::recording_log::ENTRY_TYPE_TERM => "TERM",
        deepmsg_cluster::persist::recording_log::ENTRY_TYPE_SNAPSHOT => "SNAPSHOT",
        deepmsg_cluster::persist::recording_log::ENTRY_TYPE_STANDBY_SNAPSHOT => "STANDBY_SNAPSHOT",
        _ => "UNKNOWN",
    }
}

/// `RecordingLog.RecoveryPlan`'s record `toString` (`RecordingLog.java:419-427`).
#[must_use]
pub fn recovery_plan_text(plan: &RecoveryPlan) -> String {
    let snapshots = plan
        .snapshots
        .iter()
        .map(|snapshot| {
            format!(
                "Snapshot[recordingId={}, leadershipTermId={}, termBaseLogPosition={}, \
                 logPosition={}, timestamp={}, serviceId={}]",
                snapshot.recording_id,
                snapshot.leadership_term_id,
                snapshot.term_base_log_position,
                snapshot.log_position,
                snapshot.timestamp,
                snapshot.service_id,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let log = match &plan.log {
        Some(log) => format!(
            "Log[recordingId={}, leadershipTermId={}, termBaseLogPosition={}, logPosition={}, \
             startPosition={}, stopPosition={}, initialTermId={}, termBufferLength={}, \
             mtuLength={}, sessionId={}]",
            log.recording_id,
            log.leadership_term_id,
            log.term_base_log_position,
            log.log_position,
            log.start_position,
            log.stop_position,
            log.initial_term_id,
            log.term_buffer_length,
            log.mtu_length,
            log.session_id,
        ),
        None => "null".to_owned(),
    };
    format!(
        "RecoveryPlan[lastLeadershipTermId={}, lastTermBaseLogPosition={}, \
         appendedLogPosition={}, committedLogPosition={}, snapshots=[{snapshots}], log={log}]",
        plan.last_leadership_term_id,
        plan.last_term_base_log_position,
        plan.appended_log_position,
        plan.committed_log_position,
    )
}
