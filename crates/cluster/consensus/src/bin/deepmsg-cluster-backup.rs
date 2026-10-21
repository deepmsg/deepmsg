//! One backup node process: a media driver, an archive and a `ClusterBackup`.
//!
//! This is the reference's `ClusterBackupMediaDriver` (`ClusterBackupMediaDriver
//! .java:32-161`), and its argument list is the reference's too: **property
//! files** (`SystemUtil.loadPropertiesFiles`, `:53`). A `-Dname=value` argument
//! is accepted here for the same reason [`deepmsg_cluster_node`] accepts one —
//! this build's tests have no JVM to put a property on — and is applied first,
//! so a file overrides it, which is the order `PropertyAction.REPLACE` produces.
//!
//! # The three parts, and who runs each
//!
//! The reference launches three agents in one process and gives each its own
//! thread: `MediaDriver.launch` (its runners), `Archive.launch` (a conductor
//! runner) and `ClusterBackup.launch` (an `AgentRunner`). This binary is the
//! same three, each on a thread of its own:
//!
//! * the **driver** runs on [`Driver::run`], whose slot 0 is the calling
//!   thread and whose other runners the threading mode decides (`:520-582`);
//! * the **archive** attaches to that driver over the same aeron directory —
//!   [`deepmsg_archive::launch::DriverHalf::Attached`] — because the driver is
//!   another thread here rather than an object this loop can borrow. Its loop
//!   is [`deepmsg_archive::launch::run`], the loop the archiving media driver
//!   uses;
//! * the **backup agent** runs on this thread, which
//!   [`ClusterBackup::launch`] names `cluster-backup` (or `aeron-cl-bu` under
//!   `aeron.thread.naming=new`, `ClusterBackup.java:225-226`).
//!
//! # The two readiness gates
//!
//! Both come from things the reference's own aggregate takes for granted
//! because its launches are synchronous:
//!
//! 1. The driver must have **published its CnC file** before anything connects
//!    to it. `Driver::new` builds the conductor, and the conductor publishes the
//!    file (`Conductor::without_agents` → `conductor.rs`), so this is a wait on
//!    the file rather than on a heartbeat.
//! 2. The archive must have **signalled its mark file's version** before the
//!    backup's `on_start` connects to it. That byte is written last in
//!    `launch::run`, after both control subscriptions are up (see that module's
//!    note), so it is the strict form of "the archive is serving".
//!
//! # Termination
//!
//! `SIGINT`/`SIGTERM` stop all three loops: the driver's `run` and the
//! archive's loop both poll [`deepmsg_driver::sys::stop_signal`], and this
//! thread does the same around the backup's duty cycle. The close order is the
//! reference's (`CloseHelper.closeAll(clusterBackup, archive, driver)`,
//! `ClusterBackupMediaDriver.java:159`): the backup first — it is the client of
//! both — then the archive, then the driver.
//!
//! A backup that ends its duty cycle itself (a termination) still closes the
//! same way and exits 0, which is how `AgentRunner` treats a
//! `ClusterTerminationException`.

use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use deepmsg_archive::launch::{self, DriverHalf};
use deepmsg_archive::mark_file::ArchiveMarkFile;
use deepmsg_archive::server::config::{ArchiveConfig, ThreadingMode as ArchiveThreadingMode};
use deepmsg_cluster::{ClusterBackup, ClusterBackupConfig, ClusterBackupContext};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cnc::{CncCreateError, CncFile, CncIdentity};
use deepmsg_core::clock;
use deepmsg_driver::config::DriverConfig;
use deepmsg_driver::driver::Driver;
use deepmsg_driver::idle::Strategy;
use deepmsg_driver::{cpuset, dir, sys};

const NAME: &str = "deepmsg-cluster-backup";

const USAGE: &str = "\
Usage: deepmsg-cluster-backup [options] [properties file ...]

  -Dname=value   set one backup, archive or driver property (a file overrides it)
  -h, --help     print this message

The property file format and the names are the reference's: `aeron.cluster.backup.*`
and `cluster.backup.initial.replay.start` in `docs/compat.md`, plus the driver's
and the archive's own `aeron.*` names. The driver and the archive belong to this
process, as they do in the reference's `ClusterBackupMediaDriver`.
";

/// How long this process will wait for the driver and the archive to come up.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How often the readiness gates are re-read.
const READY_RETRY: Duration = Duration::from_millis(10);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{NAME}: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print!("{USAGE}");
        return Ok(());
    }

    // One property list for three configurations, because the reference hands
    // the same system properties to all three (`ClusterBackupMediaDriver.main`
    // loads them before any context is concluded), and each reads the names it
    // knows.
    let mut properties = Properties::new();
    properties
        .apply_args(args.iter().filter(|arg| arg.starts_with("-D")).cloned())
        .map_err(|error| error.to_string())?;
    for arg in &args {
        if arg.starts_with("-D") {
            continue;
        }
        if arg.starts_with('-') {
            return Err(format!("unknown option '{arg}'\n\n{USAGE}"));
        }
        properties
            .load_file(Path::new(arg))
            .map_err(|error| error.to_string())?;
    }

    // The driver's and the archive's resolvers take `-D` arguments, so the
    // merged list is handed to them in that shape. Building it from
    // [`Properties`] rather than re-reading the argv is what makes a file's
    // value visible to all three halves.
    let forwarded: Vec<String> = properties
        .iter()
        .map(|(name, value)| format!("-D{name}={value}"))
        .collect();

    let mut driver_config =
        DriverConfig::from_args(forwarded.clone()).map_err(|error| error.to_string())?;
    // `ClusterBackupMediaDriver.launch` sets this on the driver it owns
    // (`:95-96`): a stream with only spies on it looks connected. The backup
    // itself never relies on that, but the aggregate the reference ships does
    // it, and a process that starts this driver should look the same.
    driver_config.spies_simulate_connection = true;

    let archive_config = ArchiveConfig::from_args(forwarded).map_err(|error| error.to_string())?;
    if matches!(archive_config.threading_mode, ArchiveThreadingMode::Invoker) {
        return Err(
            "aeron.archive.threading.mode is INVOKER; this build runs the archive's \
                    agents itself, and INVOKER asks for an agent the caller drives"
                .to_owned(),
        );
    }

    let backup_config = ClusterBackupConfig::from_properties(&properties).map_err(|error| {
        format!(
            "the backup's configuration is refused: {error} \
             (see `aeron.cluster.backup.*` in docs/compat.md)"
        )
    })?;

    // Before anything touches the file system, in the reference's order — the
    // same sequence `deepmsg-archiving-media-driver` follows.
    sys::install_stop_handler()
        .map_err(|error| format!("could not install the signal handler: {error}"))?;

    let affinity = cpuset::apply(&driver_config).map_err(|error| format!("{error}"))?;
    let now_ms = clock::epoch_millis();
    let prepared = dir::prepare(&driver_config, now_ms).map_err(|error| format!("{error}"))?;

    let identity = CncIdentity {
        liveness_timeout_ns: driver_config.client_liveness_timeout_ns,
        start_timestamp_ms: now_ms,
        pid: i64::from(std::process::id()),
    };

    let cnc = match CncFile::create(&driver_config.aeron_dir, &driver_config.layout, &identity) {
        Ok(cnc) => cnc,
        Err(CncCreateError::Io(source)) if std::io::ErrorKind::AlreadyExists == source.kind() => {
            return Err(format!(
                "another driver is creating {}: EBUSY",
                driver_config.aeron_dir.display()
            ));
        }
        Err(error) => {
            let _ = prepared.remove();
            return Err(error.to_string());
        }
    };

    let mut driver = match Driver::new(cnc, &driver_config, affinity) {
        Ok(driver) => driver,
        Err(error) => {
            let _ = prepared.remove();
            return Err(error.to_string());
        }
    };

    // The driver's thread. `run` returns when the conductor stops it (a
    // termination command) or a signal arrives; `close` is what joins the
    // runners it started, and the reference keeps the two apart the same way.
    let driver_thread = std::thread::Builder::new()
        .name("aeron-driver".to_owned())
        .spawn(move || {
            let outcome = driver.run().map_err(|error| error.to_string());
            driver.close().map_err(|error| error.to_string())?;
            outcome
        })
        .map_err(|error| format!("could not start the driver's thread: {error}"))?;

    // The CnC file is published by `Driver::new`, so this wait is about the
    // file being **readable** rather than about waiting for the driver to make
    // progress; the loop keeps it honest if that ever stops being true.
    if let Err(message) = await_cnc(&driver_config.aeron_dir) {
        sys::raise_stop_signal();
        let _ = driver_thread.join();
        let _ = prepared.remove();
        return Err(message);
    }

    let archive_dir = archive_config.archive_dir.clone();
    let archive_thread = std::thread::Builder::new()
        .name("aeron-archive".to_owned())
        .spawn(move || {
            let mut half = DriverHalf::Attached;
            launch::run(NAME, &mut half, &archive_config)
        })
        .map_err(|error| format!("could not start the archive's thread: {error}"))?;

    if let Err(message) = await_archive(&archive_dir) {
        sys::raise_stop_signal();
        let _ = archive_thread.join();
        let _ = driver_thread.join();
        let _ = prepared.remove();
        return Err(message);
    }

    // `ClusterBackup::launch` concludes the context — the cluster directory,
    // the BACKUP mark file and its `.lnk`, the five counters — and names this
    // thread, then the agent's `on_start` opens the recording log and connects
    // to the local archive.
    let context = ClusterBackupContext::new(backup_config);
    let mut agent = match ClusterBackup::launch(context) {
        Ok(agent) => agent,
        Err(error) => {
            sys::raise_stop_signal();
            let _ = archive_thread.join();
            let _ = driver_thread.join();
            let _ = prepared.remove();
            return Err(error.to_string());
        }
    };

    let mut idle = Strategy::by_agrona_name("backoff")
        .expect("the reference's default idle strategy is backoff");

    let mut failure = None;
    while sys::stop_signal().is_none() {
        match agent.do_work() {
            Ok(work) => idle.idle(work),
            // `AgentTerminationException` in the reference: an orderly end.
            Err(error) if error.is_termination() => break,
            Err(error) => {
                failure = Some(error.to_string());
                break;
            }
        }
    }

    let closed = agent.close().map_err(|error| error.to_string());
    sys::raise_stop_signal();
    let archived = archive_thread
        .join()
        .unwrap_or_else(|_| Err("the archive's thread panicked".to_owned()));
    let driven = driver_thread
        .join()
        .unwrap_or_else(|_| Err("the driver's thread panicked".to_owned()));

    // The backup's own failure wins, then the archive's, then the driver's —
    // the first thing that went wrong is the useful one to be told about.
    if let Some(message) = failure {
        let _ = closed;
        return Err(message);
    }
    closed?;
    archived?;
    driven?;

    Ok(())
}

/// Wait for the driver's CnC file to be readable.
fn await_cnc(aeron_dir: &Path) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if CncFile::try_open(aeron_dir).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the driver did not publish its CnC file in {} within {READY_TIMEOUT:?}",
                aeron_dir.display()
            ));
        }
        std::thread::sleep(READY_RETRY);
    }
}

/// Wait for the archive's mark file to carry a version, which
/// [`ArchiveMarkFile::signal_ready`] writes once both control subscriptions are
/// up.
fn await_archive(archive_dir: &Path) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Ok(mark) = ArchiveMarkFile::open(archive_dir) {
            if mark.version().is_some() {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the archive did not signal its mark file in {} within {READY_TIMEOUT:?}",
                archive_dir.display()
            ));
        }
        std::thread::sleep(READY_RETRY);
    }
}
