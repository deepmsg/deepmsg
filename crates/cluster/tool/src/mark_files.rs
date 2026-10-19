//! Reading the mark files a cluster leaves behind, the way
//! `ClusterToolOperator` reads them.
//!
//! The tool never adopts a mark file: [`ClusterMarkFile::open`] maps it without
//! stamping a pid over it, which is exactly what an outside reader needs. The
//! `.lnk` indirection is `resolveClusterMarkFileDir`
//! (`ClusterToolOperator.java:1309-1331`): a `cluster-mark.lnk` next to the
//! cluster directory is the ASCII path of the directory the mark file really
//! lives in.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_cluster_common::mark::{self, ClusterMarkFile};
use deepmsg_cnc::error_log::ErrorLogReader;
use deepmsg_codec_cluster_mark::mark_file_header_codec::{SBE_BLOCK_LENGTH, SBE_TEMPLATE_ID};
use deepmsg_codec_cluster_mark::{SBE_SCHEMA_ID, SBE_SCHEMA_VERSION};

use crate::date_time;

/// How long the tool sleeps between looks when a timeout asked it to wait.
const WAIT_STEP: Duration = Duration::from_millis(10);

/// `resolveClusterMarkFileDir`: the directory the mark files are in.
///
/// # Errors
///
/// A directory-name file that cannot be read (`resolveDirectoryFromLinkFile`,
/// `ClusterToolOperator.java:1315-1331`, raises there too).
pub fn resolve_mark_file_directory(cluster_dir: &Path) -> Result<PathBuf, String> {
    let link = cluster_dir.join(mark::LINK_FILENAME);
    if !link.exists() {
        return Ok(cluster_dir.to_path_buf());
    }
    let bytes = fs::read(&link)
        .map_err(|error| format!("failed to read link file={}: {error}", link.display()))?;
    let text = String::from_utf8_lossy(&bytes);
    Ok(PathBuf::from(text.trim()))
}

/// The cluster mark file's path, whichever directory it is really in.
pub fn mark_file_path(cluster_dir: &Path) -> Result<PathBuf, String> {
    Ok(resolve_mark_file_directory(cluster_dir)?.join(mark::FILENAME))
}

/// `markFileExists` (`ClusterToolOperator.java:496-502`).
#[must_use]
pub fn mark_file_exists(cluster_dir: &Path) -> bool {
    mark_file_path(cluster_dir).is_ok_and(|path| path.exists())
}

/// `openMarkFile`: map the mark file, waiting up to `timeout_ms` for the
/// writer to finish if the tool was given a timeout.
///
/// The wait is the reference's: its `ClusterMarkFile` constructor waits until
/// the version field is non-zero, and `describe`/`pid`/`errors`/`list-members`
/// enter the wait when `timeoutMs > 0` even if the file does not exist yet.
///
/// # Errors
///
/// The last error once the deadline passes; a major-version mismatch is
/// refused at once, as `ClusterMarkFile` refuses it.
pub fn open_mark_file(cluster_dir: &Path, timeout_ms: i64) -> Result<ClusterMarkFile, String> {
    let path = mark_file_path(cluster_dir)?;
    let deadline =
        Instant::now() + Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
    loop {
        if path.exists() {
            match ClusterMarkFile::open(&path) {
                Ok(file) => {
                    match file.version() {
                        Some(version) if version != 0 => {
                            if mark::major_of(version) != mark::MAJOR_VERSION {
                                return Err(format!("incompatible version: {}", version));
                            }
                            return Ok(file);
                        }
                        // Not signalled ready yet: keep waiting.
                        _ => {}
                    }
                }
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error.to_string());
                    }
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("{} does not exist", path.display()));
        }
        std::thread::sleep(WAIT_STEP);
    }
}

/// `openServiceMarkFiles` (`ClusterToolOperator.java:1190-1254`): the container
/// mark files named in a directory, with `.lnk` indirection resolved and each
/// base name taken once (the link wins).
///
/// A file that cannot be opened is reported through `logger` and skipped, as
/// the reference's `ClusterMarkFile` constructor does with its `logger`.
#[must_use]
pub fn open_service_mark_files(
    directory: &Path,
    timeout_ms: i64,
    logger: &mut dyn Write,
) -> Vec<ClusterMarkFile> {
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };

    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| {
            name.starts_with(mark::SERVICE_FILENAME_PREFIX)
                && (name.ends_with(mark::FILE_EXTENSION)
                    || name.ends_with(mark::LINK_FILE_EXTENSION))
        })
        .collect();
    names.sort();

    // Links first, then the plain files whose base name no link claimed.
    let mut resolved: Vec<PathBuf> = Vec::new();
    let mut services: Vec<String> = Vec::new();
    for name in &names {
        if let Some(stem) = name.strip_suffix(mark::LINK_FILE_EXTENSION) {
            let link = directory.join(name);
            match resolve_link(&link) {
                Ok(mark_file_directory) => {
                    resolved
                        .push(mark_file_directory.join(format!("{stem}{}", mark::FILE_EXTENSION)));
                    services.push(stem.to_owned());
                }
                Err(error) => {
                    let _ = writeln!(logger, "{error}");
                }
            }
        }
    }
    for name in &names {
        if let Some(stem) = name.strip_suffix(mark::FILE_EXTENSION) {
            if services.iter().any(|service| service == stem) {
                continue;
            }
            resolved.push(directory.join(name));
            services.push(stem.to_owned());
        }
    }

    let mut files = Vec::with_capacity(resolved.len());
    for path in resolved {
        match open_path(&path, timeout_ms) {
            Ok(file) => files.push(file),
            Err(error) => {
                let _ = writeln!(logger, "{error}");
            }
        }
    }
    files
}

fn resolve_link(link: &Path) -> Result<PathBuf, String> {
    let bytes = fs::read(link)
        .map_err(|error| format!("failed to read link file={}: {error}", link.display()))?;
    Ok(PathBuf::from(String::from_utf8_lossy(&bytes).trim()))
}

fn open_path(path: &Path, timeout_ms: i64) -> Result<ClusterMarkFile, String> {
    let deadline =
        Instant::now() + Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
    loop {
        match ClusterMarkFile::open(path) {
            Ok(file) => match file.version() {
                Some(version) if version != 0 => return Ok(file),
                _ => {}
            },
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(error.to_string());
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("{} is not ready", path.display()));
        }
        std::thread::sleep(WAIT_STEP);
    }
}

/// `printTypeAndActivityTimestamp` (`ClusterToolOperator.java:1256-1276`).
pub fn print_type_and_activity(out: &mut dyn Write, mark_file: &ClusterMarkFile) -> io::Result<()> {
    let component_type = mark_file
        .component_type()
        .map(|component| component.to_string())
        .unwrap_or_else(|| "NullVal".to_owned());
    let now_ms = deepmsg_core::clock::epoch_millis();
    let start_ms = mark_file.start_timestamp().unwrap_or(0);
    let activity_ms = mark_file.activity_timestamp().unwrap_or(0);
    writeln!(
        out,
        "Type: {component_type} {} (start: {}, activity: {})",
        date_time::time_of_day(now_ms),
        date_time::date_time(start_ms),
        date_time::date_time(activity_ms),
    )
}

/// `MarkFileHeaderDecoder.toString()` — the SBE generated format, which
/// `describe` prints verbatim (`ClusterToolOperator.java:1147`).
#[must_use]
pub fn header_line(mark_file: &ClusterMarkFile) -> String {
    let var = |index: usize| mark_file.var_data(index).unwrap_or_default();
    let (archive_stream_id, service_stream_id, consensus_module_stream_id, ingress_stream_id) =
        mark_file.stream_ids().unwrap_or((0, 0, 0, 0));
    format!(
        "[MarkFileHeader](sbeTemplateId={SBE_TEMPLATE_ID}|sbeSchemaId={SBE_SCHEMA_ID}|\
         sbeSchemaVersion={SBE_SCHEMA_VERSION}|sbeBlockLength={SBE_BLOCK_LENGTH}):\
         version={}|componentType={}|activityTimestamp={}|startTimestamp={}|pid={}|\
         candidateTermId={}|archiveStreamId={archive_stream_id}|serviceStreamId={service_stream_id}|\
         consensusModuleStreamId={consensus_module_stream_id}|ingressStreamId={ingress_stream_id}|\
         memberId={}|serviceId={}|headerLength={}|errorBufferLength={}|clusterId={}|\
         aeronDirectory='{}'|controlChannel='{}'|ingressChannel='{}'|serviceName='{}'|\
         authenticator='{}'|servicesClusterDir='{}'",
        mark_file.version().unwrap_or(0),
        mark_file
            .component_type()
            .map(|component| component.to_string())
            .unwrap_or_else(|| "NullVal".to_owned()),
        mark_file.activity_timestamp().unwrap_or(0),
        mark_file.start_timestamp().unwrap_or(0),
        mark_file.pid().unwrap_or(0),
        mark_file.candidate_term_id().unwrap_or(0),
        mark_file.member_id().unwrap_or(0),
        mark_file.service_id().unwrap_or(0),
        mark_file.header_length().unwrap_or(0),
        mark_file.recorded_error_buffer_length().unwrap_or(0),
        mark_file.cluster_id().unwrap_or(0),
        var(0),
        var(1),
        var(2),
        var(3),
        var(4),
        var(5),
    )
}

/// `CommonContext.printErrorLog` (`CommonContext.java:1227-1257`).
pub fn print_error_log(out: &mut dyn Write, reader: &ErrorLogReader<'_>) -> io::Result<()> {
    if !reader.has_entries() {
        write!(out, "\n0 distinct errors observed\n")?;
        return Ok(());
    }

    let mut entries = Vec::new();
    let scan = reader.read(i64::MIN, &mut entries);
    for entry in &entries {
        writeln!(out)?;
        writeln!(
            out,
            "{} observations from {} to {} for:",
            entry.observation_count,
            date_time::date_time_millis_zone(entry.first_observation_timestamp_ms),
            date_time::date_time_millis_zone(entry.last_observation_timestamp_ms),
        )?;
        writeln!(out, "{}", entry.text)?;
    }
    writeln!(out)?;
    writeln!(out, "{} distinct errors observed.", scan.entries)?;
    Ok(())
}

/// `printErrors` (`ClusterToolOperator.java:1363-1366`): the literal name is
/// `"Cluster"` for every cluster mark file, the module's and the containers'.
pub fn print_mark_file_errors(out: &mut dyn Write, mark_file: &ClusterMarkFile) -> io::Result<()> {
    writeln!(out, "Cluster component error log:")?;
    match mark_file.error_buffer_read_only() {
        Some(buffer) => print_error_log(out, &ErrorLogReader::new(buffer)),
        None => write!(out, "\n0 distinct errors observed\n"),
    }
}

/// `printDriverErrors` (`ClusterToolOperator.java:1285-1310`).
pub fn print_driver_errors(
    out: &mut dyn Write,
    aeron_directory: &str,
    timeout_ms: i64,
) -> Result<(), String> {
    writeln!(
        out,
        "Aeron driver error log (directory: {aeron_directory}):"
    )
    .map_err(|error| error.to_string())?;
    let timeout = Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
    let cnc = deepmsg_cnc::file::CncFile::open(Path::new(aeron_directory), timeout)
        .map_err(|error| error.to_string())?;
    match cnc.error_log() {
        Some(reader) => print_error_log(out, &reader).map_err(|error| error.to_string())?,
        None => write!(out, "\n0 distinct errors observed\n").map_err(|error| error.to_string())?,
    }
    Ok(())
}
