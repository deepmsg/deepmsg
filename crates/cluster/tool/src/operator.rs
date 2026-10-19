//! The operator's commands, the counterpart of `ClusterToolOperator.java`.
//!
//! One method per subcommand, each returning the reference's exit status
//! (`SUCCESS = 0`, `FAILURE = -1`; `ClusterToolOperator.java:85-87`) and writing
//! what the reference writes. The methods take `&mut dyn Write` rather than a
//! `PrintStream` so a test can capture the output in-process, which is what the
//! reference's `CapturingPrintStream` does for `ClusterToolTest`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_archive::client::context::ArchiveContext;
use deepmsg_archive::client::descriptor_poller::RecordingDescriptor;
use deepmsg_archive::client::{Archive, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_cluster::control::{cluster_control, node_control};
use deepmsg_cluster::election::state::ElectionState;
use deepmsg_cluster::persist::recording_log::{
    CONSENSUS_MODULE_SERVICE_ID, Entry, RecordingExtent, RecordingLog, RecoveryPlan,
};
use deepmsg_cluster::state::ModuleState;
use deepmsg_cluster_common::counters::{
    CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_ELECTION_STATE_TYPE_ID,
    CLUSTER_NODE_ROLE_TYPE_ID, find_cluster_counter,
};
use deepmsg_cluster_service::proxy::ConsensusModuleProxy;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_cnc::file::CncFile;
use deepmsg_codec_cluster::ReadBuf;
use deepmsg_codec_cluster::cluster_members_extended_response_codec::{
    ClusterMembersExtendedResponseDecoder, SBE_TEMPLATE_ID as EXTENDED_MEMBERS_RESPONSE_TEMPLATE_ID,
};
use deepmsg_codec_cluster::cluster_members_response_codec::{
    ClusterMembersResponseDecoder, SBE_TEMPLATE_ID as MEMBERS_RESPONSE_TEMPLATE_ID,
};
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_core::buffer::ReadWrite;

use crate::mark_files;
use crate::properties::ToolProperties;

/// How long a command-level wait polls between looks.
const WAIT_STEP: Duration = Duration::from_millis(1);

/// The timeout every add-publication/add-subscription uses; the cluster's own
/// `COMMAND_TIMEOUT` shape.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

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

    /// `sortRecordingLog` (`ClusterToolOperator.java:173-192`). A file already
    /// in the view's order, an unreadable one, or none at all is `FAILURE`.
    pub fn sort_recording_log(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        let _ = out;
        let path = cluster_dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
        if !path.exists() {
            return FAILURE;
        }
        let Ok(log) = RecordingLog::open(&path) else {
            return FAILURE;
        };
        if log.is_sorted() {
            return FAILURE;
        }
        let entries = log.entries().to_vec();
        drop(log);
        match deepmsg_cluster::persist::recording_log::write_entries(&path, &entries) {
            Ok(()) => SUCCESS,
            Err(_) => FAILURE,
        }
    }

    /// `seedRecordingLogFromSnapshot` (`ClusterToolOperator.java:201-269`): back
    /// the log up, then keep only the run of snapshots that starts at the latest
    /// valid consensus-module snapshot, with the term base and log position
    /// zeroed. No such snapshot ⇒ the log is deleted (the backup remains).
    pub fn seed_recording_log_from_snapshot(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        let path = cluster_dir.join(deepmsg_cluster::persist::recording_log::FILENAME);
        if !path.exists() {
            let _ = writeln!(out, "recording.log does not exist");
            return FAILURE;
        }
        let Ok(log) = RecordingLog::open(&path) else {
            return FAILURE;
        };
        let entries = log.entries().to_vec();
        drop(log);

        let snapshot_index = entries.iter().rposition(|entry| {
            entry.is_valid
                && entry.entry_type == deepmsg_cluster::persist::recording_log::ENTRY_TYPE_SNAPSHOT
                && entry.service_id == CONSENSUS_MODULE_SERVICE_ID
        });

        if let Err(message) = backup_recording_log(&path) {
            let _ = writeln!(out, "{message}");
            return FAILURE;
        }

        let truncated = match snapshot_index {
            None => Vec::new(),
            Some(index) => {
                let mut truncated = Vec::new();
                let mut service_id = CONSENSUS_MODULE_SERVICE_ID;
                for entry in entries[..=index].iter().rev() {
                    if entry.is_valid
                        && entry.entry_type
                            == deepmsg_cluster::persist::recording_log::ENTRY_TYPE_SNAPSHOT
                        && entry.service_id == service_id
                    {
                        truncated.push(Entry {
                            position: 0,
                            recording_id: entry.recording_id,
                            leadership_term_id: entry.leadership_term_id,
                            term_base_log_position: 0,
                            log_position: 0,
                            timestamp: entry.timestamp,
                            service_id: entry.service_id,
                            entry_type: entry.entry_type,
                            is_valid: entry.is_valid,
                            archive_endpoint: None,
                            entry_index: 0,
                        });
                        service_id += 1;
                    } else {
                        break;
                    }
                }
                truncated.reverse();
                truncated
            }
        };

        match deepmsg_cluster::persist::recording_log::write_entries(&path, &truncated) {
            Ok(()) => SUCCESS,
            Err(message) => {
                let _ = writeln!(out, "{message}");
                FAILURE
            }
        }
    }

    /// `listMembers` (`ClusterToolOperator.java:317-347`).
    pub fn list_members(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        if !mark_files::mark_file_exists(cluster_dir) && self.properties.timeout_ms <= 0 {
            let _ = writeln!(out, "cluster-mark.dat does not exist.");
            return FAILURE;
        }
        let mark_file = match mark_files::open_mark_file(cluster_dir, self.properties.timeout_ms) {
            Ok(mark_file) => mark_file,
            Err(message) => {
                let _ = writeln!(out, "{message}");
                return FAILURE;
            }
        };
        let Ok(control) = control_properties(&mark_file) else {
            let _ = writeln!(out, "cluster-mark.dat does not describe a driver");
            return FAILURE;
        };
        // `Math.max(TimeUnit.SECONDS.toMillis(1), timeoutMs)` (`:326`).
        let query_timeout_ms = self.properties.timeout_ms.max(1_000);
        match self.query_cluster_members(&control, query_timeout_ms) {
            Ok(membership) => {
                let _ = writeln!(
                    out,
                    "currentTimeNs={}, leaderMemberId={}, memberId={}, activeMembers={}",
                    membership.current_time_ns,
                    membership.leader_member_id,
                    membership.member_id,
                    membership.active_members_text(),
                );
                SUCCESS
            }
            Err(_) => {
                let _ = writeln!(out, "timeout waiting for response from node");
                FAILURE
            }
        }
    }

    /// `isLeader` (`ClusterToolOperator.java:444-488`): `0` when this node's
    /// role is `LEADER` **and** the election has closed, `1` otherwise, `-1`
    /// when there is no mark file.
    pub fn is_leader(&self, cluster_dir: &Path, out: &mut dyn Write) -> i32 {
        if !mark_files::mark_file_exists(cluster_dir) && self.properties.timeout_ms <= 0 {
            let _ = writeln!(out, "cluster-mark.dat does not exist.");
            return FAILURE;
        }
        let mark_file = match mark_files::open_mark_file(cluster_dir, self.properties.timeout_ms) {
            Ok(mark_file) => mark_file,
            Err(message) => {
                let _ = writeln!(out, "{message}");
                return FAILURE;
            }
        };
        let Ok(control) = control_properties(&mark_file) else {
            return FAILURE;
        };
        let cluster_id = mark_file.cluster_id().unwrap_or(0);
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(
                u64::try_from(self.properties.timeout_ms.max(0)).unwrap_or(0),
            );
        loop {
            if let Ok(cnc) = CncFile::open(
                Path::new(&control.aeron_dir),
                std::time::Duration::from_millis(100),
            ) {
                if let Some(counters) = cnc.counters() {
                    let role =
                        find_cluster_counter(&counters, CLUSTER_NODE_ROLE_TYPE_ID, cluster_id)
                            .and_then(|id| counters.value(id));
                    let election =
                        find_cluster_counter(&counters, CLUSTER_ELECTION_STATE_TYPE_ID, cluster_id)
                            .and_then(|id| counters.value(id));
                    // `Cluster.Role.LEADER` is 2; `ElectionState.CLOSED` is 17
                    // (`ElectionState.java`), which the CM mirrors.
                    let is_leader = role == Some(2)
                        && election == Some(i64::from(ElectionState::Closed.code()));
                    return if is_leader { 0 } else { 1 };
                }
            }
            if std::time::Instant::now() >= deadline {
                return FAILURE;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// The five switches and `validate-recording-log`
    /// (`ClusterToolOperator.java:845-967`, `:995-1115`).
    pub fn toggle(&self, cluster_dir: &Path, out: &mut dyn Write, name: &str) -> i32 {
        let Some(command) = ToggleCommand::named(name) else {
            let _ = writeln!(out, "Unknown command: {name}");
            return FAILURE;
        };
        if self.toggle_state(cluster_dir, out, command) {
            SUCCESS
        } else {
            FAILURE
        }
    }

    fn toggle_state(
        &self,
        cluster_dir: &Path,
        out: &mut dyn Write,
        command: ToggleCommand,
    ) -> bool {
        if !mark_files::mark_file_exists(cluster_dir) && self.properties.timeout_ms <= 0 {
            let _ = writeln!(out, "cluster-mark.dat does not exist.");
            return false;
        }
        let mark_file = match mark_files::open_mark_file(cluster_dir, self.properties.timeout_ms) {
            Ok(mark_file) => mark_file,
            Err(message) => {
                let _ = writeln!(out, "{message}");
                return false;
            }
        };
        let cluster_id = mark_file.cluster_id().unwrap_or(0);
        let Ok(control) = control_properties(&mark_file) else {
            let _ = writeln!(out, "cluster-mark.dat does not describe a driver");
            return false;
        };

        let query_timeout_ms = self.properties.timeout_ms.max(1_000);
        let membership = match self.query_cluster_members(&control, query_timeout_ms) {
            Ok(membership) => membership,
            Err(_) => {
                let _ = writeln!(out, "Timed out querying cluster.");
                return false;
            }
        };
        let prefix = format!("Member [{}]: ", membership.member_id);

        if command.leader_required && membership.leader_member_id != membership.member_id {
            let _ = writeln!(
                out,
                "{prefix}Current node is not the leader (leaderMemberId = {}), unable to {}",
                membership.leader_member_id,
                command.target.name(),
            );
            return false;
        }

        let cnc_path = Path::new(&control.aeron_dir).join("cnc.dat");
        if !cnc_path.exists() {
            let absolute = std::path::absolute(&cnc_path).unwrap_or(cnc_path);
            let _ = writeln!(
                out,
                "{prefix}Unable to locate media driver. C`n`C file [{}] does not exist.",
                absolute.display()
            );
            return false;
        }

        let Ok(cnc) = CncFile::open_writable(
            Path::new(&control.aeron_dir),
            std::time::Duration::from_millis(u64::try_from(query_timeout_ms).unwrap_or(1_000)),
        ) else {
            let _ = writeln!(out, "{prefix}Unable to locate media driver.");
            return false;
        };
        let Some(counters) = cnc.counters_writable() else {
            let _ = writeln!(out, "{prefix}Unable to resolve state of consensus module.");
            return false;
        };

        let state = find_cluster_counter(
            &counters,
            CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
            cluster_id,
        )
        .and_then(|id| counters.value(id))
        .and_then(|value| i32::try_from(value).ok())
        .and_then(ModuleState::from_code);
        let Some(state) = state else {
            let _ = writeln!(out, "{prefix}Unable to resolve state of consensus module.");
            return false;
        };

        if state != command.expected {
            let _ = writeln!(
                out,
                "{prefix}Unable to {} as the state of the consensus module is {}, but needs to be {}",
                command.target.name(),
                state.name(),
                command.expected.name(),
            );
            return false;
        }

        let Some(toggle_id) = find_cluster_counter(&counters, command.target.type_id(), cluster_id)
        else {
            let _ = writeln!(out, "{prefix}Failed to find control toggle");
            return false;
        };

        if !command.target.toggle(&counters, toggle_id) {
            let _ = writeln!(
                out,
                "{prefix}Failed to apply {}, current toggle value = {}",
                command.target.name(),
                command.target.name_at(&counters, toggle_id),
            );
            return false;
        }

        if command.wait_for_completion {
            let timeout_ms = command.default_timeout_ms.max(self.properties.timeout_ms);
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
            let mut is_neutral;
            loop {
                is_neutral = command.target.is_neutral(&counters, toggle_id);
                if is_neutral || std::time::Instant::now() > deadline {
                    break;
                }
                std::thread::yield_now();
            }
            if !is_neutral {
                let _ = writeln!(
                    out,
                    "{prefix}Timed out after {timeout_ms}ms waiting for {} to complete.",
                    command.target.name()
                );
                return false;
            }
        }

        let _ = writeln!(
            out,
            "{prefix}{} applied successfully",
            command.target.name()
        );
        true
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

/// `ClusterNodeControlProperties` (`ClusterNodeControlProperties.java`): what
/// the mark file says about the driver this component is talking to.
struct ControlProperties {
    aeron_dir: String,
    control_channel: String,
    service_stream_id: i32,
    consensus_module_stream_id: i32,
}

/// `ClusterMarkFile.loadControlProperties` (`ClusterMarkFile.java:595-609`).
fn control_properties(
    mark_file: &deepmsg_cluster_common::mark::ClusterMarkFile,
) -> Result<ControlProperties, ()> {
    let (_, service_stream_id, consensus_module_stream_id, _) = mark_file.stream_ids().ok_or(())?;
    Ok(ControlProperties {
        aeron_dir: mark_file.var_data(0).ok_or(())?,
        control_channel: mark_file.var_data(1).ok_or(())?,
        service_stream_id,
        consensus_module_stream_id,
    })
}

/// The tool's copy of `ClusterMembership` (`ClusterToolOperator.ClusterMembership`).
struct ClusterMembership {
    current_time_ns: i64,
    leader_member_id: i32,
    member_id: i32,
    /// The extended response's member list; `None` for the plain response,
    /// which carries no list for a Java `List.toString` to print.
    active_members: Option<Vec<MembershipMember>>,
}

impl ClusterMembership {
    /// What `", activeMembers=" + clusterMembership.activeMembers` prints: a
    /// Java `List.toString` for the extended response, `null` otherwise.
    fn active_members_text(&self) -> String {
        match &self.active_members {
            Some(members) => format!(
                "[{}]",
                members
                    .iter()
                    .map(MembershipMember::text)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => "null".to_owned(),
        }
    }
}

/// One member of the extended response, as `ClusterMember.toString` prints it.
struct MembershipMember {
    leadership_term_id: i64,
    log_position: i64,
    time_of_last_append_ns: i64,
    member_id: i32,
    ingress_endpoint: String,
    consensus_endpoint: String,
    log_endpoint: String,
    catchup_endpoint: String,
    archive_endpoint: String,
}

impl MembershipMember {
    /// `ClusterMember.toString` (`ClusterMember.java:1309-1330`). The fields
    /// the response does not carry keep the constructor's defaults, which is
    /// what the reference's own objects have after the same decode.
    fn text(&self) -> String {
        let endpoints = format!(
            "{},{},{},{},{},{}",
            self.member_id,
            self.ingress_endpoint,
            self.consensus_endpoint,
            self.log_endpoint,
            self.catchup_endpoint,
            self.archive_endpoint,
        );
        format!(
            "ClusterMember{{id={}, isBallotSent=false, isLeader=false, leadershipTermId={}, \
             logPosition={}, candidateTermId=-1, catchupReplaySessionId=-1, correlationId=-1, \
             timeOfLastAppendPositionNs={}, ingressEndpoint='{}', consensusEndpoint='{}', \
             logEndpoint='{}', catchupEndpoint='{}', archiveEndpoint='{}', endpoints='{}', \
             publication=null, vote=null}}",
            self.member_id,
            self.leadership_term_id,
            self.log_position,
            self.time_of_last_append_ns,
            self.ingress_endpoint,
            self.consensus_endpoint,
            self.log_endpoint,
            self.catchup_endpoint,
            self.archive_endpoint,
            endpoints,
        )
    }
}

/// One switch from the command table, with the reference's four parameters
/// (`ClusterToolOperator.java:845-967`).
#[derive(Clone, Copy)]
struct ToggleCommand {
    target: ToggleTarget,
    expected: ModuleState,
    leader_required: bool,
    wait_for_completion: bool,
    default_timeout_ms: i64,
}

impl ToggleCommand {
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "snapshot" => Self {
                target: ToggleTarget::Cluster(cluster_control::ToggleState::Snapshot),
                expected: ModuleState::Active,
                leader_required: true,
                wait_for_completion: true,
                default_timeout_ms: 30_000,
            },
            "suspend" => Self {
                target: ToggleTarget::Cluster(cluster_control::ToggleState::Suspend),
                expected: ModuleState::Active,
                leader_required: true,
                wait_for_completion: false,
                default_timeout_ms: 1_000,
            },
            "resume" => Self {
                target: ToggleTarget::Cluster(cluster_control::ToggleState::Resume),
                expected: ModuleState::Suspended,
                leader_required: true,
                wait_for_completion: true,
                default_timeout_ms: 1_000,
            },
            "shutdown" => Self {
                target: ToggleTarget::Cluster(cluster_control::ToggleState::Shutdown),
                expected: ModuleState::Active,
                leader_required: true,
                wait_for_completion: false,
                default_timeout_ms: 1_000,
            },
            "abort" => Self {
                target: ToggleTarget::Cluster(cluster_control::ToggleState::Abort),
                expected: ModuleState::Active,
                leader_required: true,
                wait_for_completion: false,
                default_timeout_ms: 1_000,
            },
            "validate-recording-log" => Self {
                target: ToggleTarget::Node(node_control::ToggleState::ValidateRecordingLog),
                expected: ModuleState::Active,
                leader_required: false,
                wait_for_completion: true,
                default_timeout_ms: 30_000,
            },
            _ => return None,
        })
    }
}

/// Which of the two operator toggles a command writes.
#[derive(Clone, Copy)]
enum ToggleTarget {
    Cluster(cluster_control::ToggleState),
    Node(node_control::ToggleState),
}

impl ToggleTarget {
    const fn name(self) -> &'static str {
        match self {
            Self::Cluster(toggle) => toggle.name(),
            Self::Node(toggle) => toggle.name(),
        }
    }

    const fn type_id(self) -> i32 {
        match self {
            Self::Cluster(_) => cluster_control::CONTROL_TOGGLE_TYPE_ID,
            Self::Node(_) => node_control::CONTROL_TOGGLE_TYPE_ID,
        }
    }

    fn toggle(self, counters: &CountersReader<'_, ReadWrite>, counter_id: i32) -> bool {
        match self {
            Self::Cluster(toggle) => toggle.toggle(counters, counter_id),
            Self::Node(toggle) => toggle.toggle(counters, counter_id),
        }
    }

    fn is_neutral(self, counters: &CountersReader<'_, ReadWrite>, counter_id: i32) -> bool {
        match self {
            Self::Cluster(_) => {
                counters.value(counter_id)
                    == Some(i64::from(cluster_control::ToggleState::Neutral.code()))
            }
            Self::Node(_) => {
                counters.value(counter_id)
                    == Some(i64::from(node_control::ToggleState::Neutral.code()))
            }
        }
    }

    /// `ToggleState.get(counter)`: the name the failure message prints.
    fn name_at(self, counters: &CountersReader<'_, ReadWrite>, counter_id: i32) -> String {
        let Some(value) = counters.value(counter_id) else {
            return "null".to_owned();
        };
        let code = i32::try_from(value).unwrap_or(i32::MAX);
        match self {
            Self::Cluster(_) => cluster_control::ToggleState::from_code(value)
                .map_or_else(|_| code.to_string(), |toggle| toggle.name().to_owned()),
            Self::Node(_) => node_control::ToggleState::from_code(value)
                .map_or_else(|_| code.to_string(), |toggle| toggle.name().to_owned()),
        }
    }
}

/// `ConsensusModuleProxy.clusterMembersQuery` round trip
/// (`ClusterToolOperator.java:566-631`): a publication on the consensus
/// module's stream, a subscription on the services' stream, and one response.
impl ClusterTool {
    fn query_cluster_members(
        &self,
        control: &ControlProperties,
        timeout_ms: i64,
    ) -> Result<ClusterMembership, String> {
        let mut client =
            Client::connect(Path::new(&control.aeron_dir)).map_err(|error| error.to_string())?;
        let subscription = client
            .add_subscription(
                &control.control_channel,
                control.service_stream_id,
                COMMAND_TIMEOUT,
            )
            .map_err(|error| error.to_string())?;
        let publication = client
            .add_publication(
                &control.control_channel,
                control.consensus_module_stream_id,
                COMMAND_TIMEOUT,
            )
            .map_err(|error| error.to_string())?;

        let mut proxy = ConsensusModuleProxy::new(0);
        proxy.set_publication(publication);
        let correlation_id = client
            .next_correlation_id()
            .map_err(|error| error.to_string())?;
        let timeout = Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
        let deadline = Instant::now() + timeout;

        loop {
            client.poll();
            let bound = client
                .subscription(subscription)
                .is_some_and(|subscription| subscription.is_connected());
            let connected = client
                .publication(publication)
                .and_then(|publication| publication.is_connected())
                .unwrap_or(false);
            if bound && connected {
                break;
            }
            if Instant::now() > deadline {
                return Err("Timed out querying cluster.".to_owned());
            }
            std::thread::sleep(WAIT_STEP);
        }

        loop {
            if proxy
                .cluster_members_query(&client, correlation_id)
                .map_err(|error| error.to_string())?
            {
                break;
            }
            client.poll();
            if Instant::now() > deadline {
                return Err("Timed out querying cluster.".to_owned());
            }
            std::thread::sleep(WAIT_STEP);
        }

        loop {
            client.poll();
            let mut found = None;
            client.poll_subscription(subscription, 10, |message| {
                if let Some(membership) = decode_members_response(message.payload, correlation_id) {
                    found = Some(membership);
                }
            });
            if let Some(membership) = found {
                return Ok(membership);
            }
            if Instant::now() > deadline {
                return Err("Timed out querying cluster.".to_owned());
            }
            std::thread::sleep(WAIT_STEP);
        }
    }
}

/// One message from the services' stream, if it is the answer to
/// `correlation_id` (`ClusterControlAdapter.Listener`).
fn decode_members_response(payload: &[u8], correlation_id: i64) -> Option<ClusterMembership> {
    if payload.len() < MESSAGE_HEADER_LENGTH {
        return None;
    }
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    if header.template_id() == EXTENDED_MEMBERS_RESPONSE_TEMPLATE_ID {
        let decoder = ClusterMembersExtendedResponseDecoder::default().header(header, 0);
        if decoder.correlation_id() != correlation_id {
            return None;
        }
        let current_time_ns = decoder.current_time_ns();
        let leader_member_id = decoder.leader_member_id();
        let member_id = decoder.member_id();

        let mut members = Vec::new();
        let mut group = decoder.active_members_decoder();
        while group.advance().ok().flatten().is_some() {
            let leadership_term_id = group.leadership_term_id();
            let log_position = group.log_position();
            let time_of_last_append_ns = group.time_of_last_append_ns();
            let id = group.member_id();
            let ingress_endpoint = {
                let coordinates = group.ingress_endpoint_decoder();
                String::from_utf8_lossy(group.ingress_endpoint_slice(coordinates)).into_owned()
            };
            let consensus_endpoint = {
                let coordinates = group.consensus_endpoint_decoder();
                String::from_utf8_lossy(group.consensus_endpoint_slice(coordinates)).into_owned()
            };
            let log_endpoint = {
                let coordinates = group.log_endpoint_decoder();
                String::from_utf8_lossy(group.log_endpoint_slice(coordinates)).into_owned()
            };
            let catchup_endpoint = {
                let coordinates = group.catchup_endpoint_decoder();
                String::from_utf8_lossy(group.catchup_endpoint_slice(coordinates)).into_owned()
            };
            let archive_endpoint = {
                let coordinates = group.archive_endpoint_decoder();
                String::from_utf8_lossy(group.archive_endpoint_slice(coordinates)).into_owned()
            };
            members.push(MembershipMember {
                leadership_term_id,
                log_position,
                time_of_last_append_ns,
                member_id: id,
                ingress_endpoint,
                consensus_endpoint,
                log_endpoint,
                catchup_endpoint,
                archive_endpoint,
            });
        }

        return Some(ClusterMembership {
            current_time_ns,
            leader_member_id,
            member_id,
            active_members: Some(members),
        });
    }

    if header.template_id() == MEMBERS_RESPONSE_TEMPLATE_ID {
        let decoder = ClusterMembersResponseDecoder::default().header(header, 0);
        if decoder.correlation_id() != correlation_id {
            return None;
        }
        let leader_member_id = decoder.leader_member_id();
        return Some(ClusterMembership {
            current_time_ns: 0,
            leader_member_id,
            member_id: -1,
            active_members: None,
        });
    }

    None
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

/// `ClusterToolOperator.updateRecordingLog`'s backup step
/// (`ClusterToolOperator.java:212-226`): `recording.log.bak`, replacing what is
/// there, **with the original's times**. The reference uses
/// `Files.copy(..., COPY_ATTRIBUTES)`; `std::fs::copy` copies permissions only,
/// so the modification and access times are set explicitly.
fn backup_recording_log(path: &Path) -> Result<(), String> {
    let backup = path.with_file_name(format!(
        "{}.bak",
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    std::fs::copy(path, &backup).map_err(|error| error.to_string())?;
    let metadata = std::fs::metadata(path).map_err(|error| error.to_string())?;
    let mut times = std::fs::FileTimes::new();
    if let Ok(modified) = metadata.modified() {
        times = times.set_modified(modified);
    }
    if let Ok(accessed) = metadata.accessed() {
        times = times.set_accessed(accessed);
    }
    std::fs::File::options()
        .write(true)
        .open(&backup)
        .and_then(|file| file.set_times(times))
        .map_err(|error| error.to_string())
}

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
