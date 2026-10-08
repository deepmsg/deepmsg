//! The archiving media driver: a media driver and an archive in one process.
//!
//! This is the reference's `ArchivingMediaDriver` (`ArchivingMediaDriver
//! .java:79-106`), and it is the shape the C archive suite drives: 214 of the
//! 276 archive spawns a rehearsal of the whole suite counted are this class,
//! which is why P2-S1 builds this binary and no other (plan §2.1).
//!
//! # The three wires the reference runs between the two
//!
//! `ArchivingMediaDriver.launch` launches the driver, then the archive, and
//! hands the archive three things (`:87-97`):
//!
//! 1. **The driver's `ERRORS` counter**, taken from the driver's own counters
//!    buffer rather than allocated fresh (`:87-89`) — so an archive error is
//!    counted where a driver error is. That is the id below.
//! 2. **The driver's shared agent invoker** (`:94`), which is how the archive
//!    drives the driver. In this build that seam is [`Driver::do_work`], whose
//!    own documentation says it is "the entry point for a caller that embeds
//!    the driver and drives it itself".
//! 3. **The driver's aeron directory** (`:95`), which is where the CnC file and
//!    the archive's mark file both live.
//!
//! # Why the loop is here and not in the driver
//!
//! [`Driver::run`] never returns: it threads the mode's runners and keeps slot
//! 0 on the calling thread. A process that also has an archive to drive
//! therefore cannot call it, and drives both itself, in the reference's order —
//! the driver's work, then the archive's. That is only the same thing as the
//! reference when the driver starts no threads of its own, so a driver
//! configured otherwise is refused rather than run into a stall nobody would
//! see: [`Driver::do_work`] under `DEDICATED` drives the conductor and nothing
//! else, so the sender and the receiver would stop and the archive would sit
//! there looking healthy.
//!
//! # When this process says it is ready
//!
//! The suite waits for the archive by polling the length of
//! `archive-mark.dat` until it passes `ARCHIVE_MARK_FILE_HEADER_LENGTH`, which
//! is 8192 (`TestArchive.h:141-153`, `TestProcessUtils.h:40`). That is a
//! **file-length** probe, and the file reaches 8192 as soon as it is created —
//! its own comment says it is "an indicator that Archive process is running",
//! not that it is serving. The genuine ready byte is the mark file's version
//! field, which [`ArchiveMarkFile::signal_ready`] writes.
//!
//! The reference writes `signalReady` at the end of `Context.conclude()`
//! (`Archive.java:1665`), which is *before* its conductor is constructed
//! (`:146-147`) and before either control subscription exists
//! (`ArchiveConductor.java:229-241`). So the reference is exposed to a first
//! connect that arrives in that window. This build creates the mark file last,
//! after both subscriptions are in place, which costs nothing and cannot fail
//! a case the reference passes.
//!
//! # The two commands that cannot be waited for
//!
//! [`Client::add_subscription`] blocks until the driver answers, and the driver
//! only advances when this loop calls [`Driver::do_work`]. Waiting for it here
//! would wait forever. Everything on the way up therefore goes through the
//! client's asynchronous path — the CnC file's publication, and both control
//! subscriptions — each polled with a driver turn in between.

use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use deepmsg_archive::catalog::Catalog;
use deepmsg_archive::mark_file::{ArchiveMarkFile, ERROR_BUFFER_LENGTH_DEFAULT, Header};
use deepmsg_archive::server::conductor::{ARCHIVE_ID_DEFAULT, ArchiveConductor};
use deepmsg_archive::server::config::ArchiveConfig;
use deepmsg_client::client::{AsyncAddPoll, Client};
use deepmsg_cnc::create::FILE_PAGE_SIZE_DEFAULT;
use deepmsg_cnc::{CncCreateError, CncFile, CncIdentity};
use deepmsg_core::clock;
use deepmsg_core::uri::ChannelUri;
use deepmsg_driver::config::{DriverConfig, ThreadingMode};
use deepmsg_driver::driver::Driver;
use deepmsg_driver::{cpuset, dir, sys};

/// `SystemCounterDescriptor.ERRORS.id()` (`ArchivingMediaDriver.java:87`) —
/// the driver's error counter, which the archive writes its own errors into
/// instead of keeping one of its own.
const DRIVER_ERROR_COUNTER_ID: i32 = 15;

/// `Archive.Configuration.MAX_CATALOG_ENTRIES_DEFAULT` (`Archive.java:472`).
///
/// Reached through `getSizeAsLong` (`:849-851`), so a property may spell it
/// `128` or `8k`; this build does not read the name yet and uses the default.
const MAX_CATALOG_ENTRIES_DEFAULT: usize = 8 * 1024;

/// `AeronArchive.Configuration.RECORDING_EVENTS_STREAM_ID_DEFAULT`
/// (`client/AeronArchive.java:2791`).
///
/// Nothing here records yet, so the channel this id belongs to is never
/// created (`events_channel` is `None`); the number is written into the mark
/// file because the reference writes it there.
const RECORDING_EVENTS_STREAM_ID_DEFAULT: i32 = 30;

/// How long this process will wait for the driver to answer a command.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // One property list, two configurations: the reference passes the same argv
    // to both halves of the process, and each reads the names it knows. Both
    // parsers ignore a name they do not know, which is what makes that work
    // (`driver/src/config.rs`, `archive/src/server/config.rs`).
    let driver_config = match DriverConfig::from_args(args.iter().cloned()) {
        Ok(config) => config,
        Err(error) => return fail(&format_args!("{error}")),
    };
    let archive_config = match ArchiveConfig::from_args(args.iter().cloned()) {
        Ok(config) => config,
        Err(error) => return fail(&format_args!("{error}")),
    };

    // The archive drives the driver on its own thread, which is only the same
    // thing as the reference's `mediaDriverAgentInvoker` wiring when the driver
    // has no threads of its own to do the work with. See the module note.
    if !matches!(
        driver_config.threading_mode,
        ThreadingMode::Shared | ThreadingMode::Invoker
    ) {
        return fail(&format_args!(
            "aeron.threading.mode is {}; archiving media driver runs the driver and the archive \
             on one thread and needs it to be SHARED or INVOKER",
            driver_config.threading_mode.as_str()
        ));
    }

    // Before anything touches the file system, and in the reference's order —
    // see `deepmsg-driver`'s own binary, which this follows to the letter.
    if let Err(error) = sys::install_stop_handler() {
        eprintln!("deepmsg-archiving-media-driver: could not install the signal handler: {error}");
        return ExitCode::FAILURE;
    }

    let affinity = match cpuset::apply(&driver_config) {
        Ok(affinity) => affinity,
        Err(error) => return fail(&format_args!("{error}")),
    };

    let now_ms = clock::epoch_millis();

    let prepared = match dir::prepare(&driver_config, now_ms) {
        Ok(prepared) => prepared,
        Err(error) => return fail(&format_args!("{error}")),
    };

    let identity = CncIdentity {
        liveness_timeout_ns: driver_config.client_liveness_timeout_ns,
        start_timestamp_ms: now_ms,
        pid: i64::from(std::process::id()),
    };

    let cnc = match CncFile::create(&driver_config.aeron_dir, &driver_config.layout, &identity) {
        Ok(cnc) => cnc,
        Err(CncCreateError::Io(source)) if std::io::ErrorKind::AlreadyExists == source.kind() => {
            eprintln!(
                "deepmsg-archiving-media-driver: another driver is creating {}: EBUSY",
                driver_config.aeron_dir.display()
            );
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("deepmsg-archiving-media-driver: {error}");
            let _ = prepared.remove();
            return ExitCode::FAILURE;
        }
    };

    let mut driver = match Driver::new(cnc, &driver_config, affinity) {
        Ok(driver) => driver,
        Err(error) => {
            eprintln!("deepmsg-archiving-media-driver: {error}");
            let _ = prepared.remove();
            return ExitCode::FAILURE;
        }
    };

    match run(&mut driver, &driver_config, &archive_config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("deepmsg-archiving-media-driver: {message}");
            let _ = driver.close();
            let _ = prepared.remove();
            ExitCode::FAILURE
        }
    }
}

/// Everything after the driver exists, so that one failure path unwinds it.
fn run(
    driver: &mut Driver,
    driver_config: &DriverConfig,
    archive_config: &ArchiveConfig,
) -> Result<(), String> {
    let deadline = Instant::now() + COMMAND_TIMEOUT;
    let aeron_dir = driver_config.aeron_dir.as_path();

    // The archive's own directory, before anything is put in it
    // (`Archive.java:1275-1282`), where the reference does it in `conclude`:
    // an archive told to start clean deletes what is there, and either way both
    // directories are made if they are missing.
    //
    // **This is not housekeeping.** The reference's `IoUtil.ensureDirectoryExists`
    // is what makes `<archive.dir>/source` exist before the catalog and the mark
    // file are created inside it, and a harness that hands the archive a
    // directory it expects the archive to make — which is what the C suite does
    // (`TestArchive.h:203` deletes it on teardown) — meets an archive that will
    // not start without it.
    if archive_config.delete_dir_on_start {
        // `IoUtil.delete(archiveDir, false)`: the second argument is
        // `ignoreFailures`, and it is **false** — a directory that will not go
        // is an archive that will not start.
        match std::fs::remove_dir_all(&archive_config.archive_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "could not delete the archive directory {}: {error}",
                    archive_config.archive_dir.display()
                ));
            }
        }
    }

    ensure_directory(&archive_config.archive_dir, "archive")?;

    if let Some(directory) = archive_config.mark_file_path().parent() {
        ensure_directory(directory, "mark file")?;
    }

    // The CnC file is created unpublished, and the driver's conductor is what
    // publishes it; a client cannot connect until it has (`CncFile::try_open`
    // is the same gate the test harness waits on, `tests/src/driver.rs:505`).
    await_published(driver, aeron_dir, deadline)?;

    let mut client = Client::connect_with_timeout(aeron_dir, COMMAND_TIMEOUT)
        .map_err(|error| format!("could not connect to the driver: {error}"))?;

    // The control channel the archive subscribes to is not quite the one it was
    // configured with: the reference forces its own sparse setting into the
    // channel before subscribing (`ArchiveConductor.java:231-232`). In place,
    // so that a channel which already carries one keeps its position — the
    // channel's text is its identity.
    let control_channel = match (
        &archive_config.control_channel,
        archive_config.control_channel_enabled,
    ) {
        (Some(channel), true) => {
            let mut uri = ChannelUri::parse(channel).map_err(|error| {
                format!("aeron.archive.control.channel is not a channel: {error}")
            })?;

            uri.put(
                "sparse",
                archive_config.control_term_buffer_sparse.to_string(),
            );

            Some(uri.build())
        }
        _ => None,
    };

    let remote_subscription_id = match &control_channel {
        Some(channel) => Some(add_subscription(
            driver,
            &mut client,
            channel,
            archive_config.control_stream_id,
            deadline,
        )?),
        None => None,
    };

    // Always, and with no condition on it: the reference builds the local one
    // unconditionally (`:239-240`), and it is what makes an IPC client able to
    // reach the archive at all.
    let local_subscription_id = add_subscription(
        driver,
        &mut client,
        &archive_config.local_control_channel,
        archive_config.local_control_stream_id,
        deadline,
    )?;

    // Last of the setup, so that nothing can be waiting on a mark file this
    // process cannot yet serve. See the module note for why this is later than
    // the reference's own `signalReady`.
    let mark_file = create_mark_file(archive_config, driver_config, control_channel.as_deref())?;

    // The ready byte, which is the mark file's *version* field rather than its
    // length (`ArchiveMarkFile.java:331-338`). Written here, after everything
    // that serves is up, rather than where the reference writes it.
    mark_file
        .signal_ready(clock::epoch_millis())
        .map_err(|error| format!("could not signal the archive ready: {error}"))?;

    let cnc = CncFile::open_writable(aeron_dir, COMMAND_TIMEOUT)
        .map_err(|error| format!("could not open the counter region: {error}"))?;

    // The recordings this archive holds. Opened here rather than inside the
    // conductor for the same reason the mark file is made here: it is a
    // file-system step, and this is where the file system is already being
    // dealt with.
    //
    // Two things about the call are worth stating, because the reference's are
    // implicit. The capacity is `aeron.archive.max.catalog.entries`'s default
    // (`Archive.java:472`, read as a size at `:849-851`) — the property itself
    // is not read yet, so a deployment that sets it gets the default. And a
    // **fresh** catalog starts at recording id 0, because the reference's field
    // starts at 0 and `Archive` constructs its `Catalog` without seeding it
    // (`Catalog.java:148`, `Archive.java:1503-1511`).
    let catalog =
        Catalog::open_or_create(&archive_config.archive_dir, MAX_CATALOG_ENTRIES_DEFAULT, 0)
            .map_err(|error| format!("could not open the archive's catalog: {error}"))?;

    let mut conductor = ArchiveConductor::new(
        archive_config,
        cnc,
        mark_file,
        catalog,
        DRIVER_ERROR_COUNTER_ID,
        remote_subscription_id,
        local_subscription_id,
    )
    .map_err(|error| format!("could not build the archive's conductor: {error}"))?;

    // stdout, not stderr: this is written on every healthy start, and the
    // reference's harness treats the archive's stderr as a diagnostic channel.
    println!(
        "deepmsg-archiving-media-driver: archive running, aeron.dir={}, archive.id={} (pid {})",
        aeron_dir.display(),
        archive_config.archive_id.unwrap_or(ARCHIVE_ID_DEFAULT),
        std::process::id()
    );

    loop {
        if let Some(signal) = sys::stop_signal() {
            conductor.signal_terminated();
            client.close();
            let _ = driver.close();
            println!("deepmsg-archiving-media-driver: stopped by signal {signal}");
            return Ok(());
        }

        // The driver first, so that everything the archive's turn hears is
        // already in the term buffers, then the archive.
        driver.do_work();
        conductor
            .do_work(&mut client, clock::epoch_millis())
            .map_err(|error| format!("the control plane stopped: {error}"))?;

        // The reference's `ctx.idleStrategy()`, which the suite sets to `yield`.
        std::thread::yield_now();
    }
}

/// Wait for the driver to publish its CnC file, driving it all the while.
///
/// Waiting without driving is waiting for a file nobody is going to write.
fn await_published(driver: &mut Driver, aeron_dir: &Path, deadline: Instant) -> Result<(), String> {
    loop {
        driver.do_work();

        match CncFile::try_open(aeron_dir) {
            Ok(_) => return Ok(()),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
            }
            Err(error) => {
                return Err(format!("the driver did not publish its CnC file: {error}"));
            }
        }
    }
}

/// Ask for a subscription and drive the driver until it answers.
///
/// `Client::add_subscription` cannot be used here: it waits for an answer that
/// only this loop can cause. See the module note.
fn add_subscription(
    driver: &mut Driver,
    client: &mut Client,
    channel: &str,
    stream_id: i32,
    deadline: Instant,
) -> Result<i64, String> {
    let add = client
        .async_add_subscription(channel, stream_id, COMMAND_TIMEOUT)
        .map_err(|error| format!("could not ask for a subscription to {channel}: {error}"))?;

    loop {
        // Both sides, or neither moves: the driver reads the command ring only
        // when it is driven, and the client hears the answer only when it polls
        // the ring the driver writes back into. Driving one of them is a
        // deadlock with extra steps.
        driver.do_work();
        client.poll();

        match client.async_add_poll(add) {
            AsyncAddPoll::Ready => return Ok(add.registration_id()),
            AsyncAddPoll::Awaiting => {}
            AsyncAddPoll::Failed(error) => {
                return Err(format!(
                    "the driver refused a subscription to {channel}: {error}"
                ));
            }
            AsyncAddPoll::Unknown => {
                return Err(format!(
                    "the answer about a subscription to {channel} was already taken"
                ));
            }
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "the driver did not answer about a subscription to {channel} in time"
            ));
        }
    }
}

/// `IoUtil.ensureDirectoryExists` (`Archive.java:1281-1282`), which makes the
/// directory and refuses a path that is something else.
///
/// `create_dir_all` is Agrona's `mkdirs`; the second check is Agrona's, and the
/// words are this build's — nothing reads them but a person.
fn ensure_directory(path: &Path, name: &str) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|error| {
        format!(
            "the {name} directory {} does not exist and could not be created: {error}",
            path.display()
        )
    })?;

    if !path.is_dir() {
        return Err(format!(
            "the {name} path {} is not a directory",
            path.display()
        ));
    }

    Ok(())
}

/// Create the archive's mark file, sized and ready to be written.
fn create_mark_file(
    archive_config: &ArchiveConfig,
    driver_config: &DriverConfig,
    control_channel: Option<&str>,
) -> Result<ArchiveMarkFile, String> {
    let aeron_directory = driver_config
        .aeron_dir
        .to_str()
        .ok_or_else(|| "aeron.dir is not valid UTF-8".to_owned())?;

    let header = Header {
        start_timestamp: clock::epoch_millis(),
        control_channel,
        local_control_channel: &archive_config.local_control_channel,
        // Nothing records yet, so no events channel exists to name
        // (`Archive.java:1216-1226` requires the control channel; the events
        // one is off unless `aeron.archive.recording.events.enabled`).
        events_channel: None,
        aeron_directory,
        control_stream_id: archive_config.control_stream_id,
        local_control_stream_id: archive_config.local_control_stream_id,
        events_stream_id: RECORDING_EVENTS_STREAM_ID_DEFAULT,
        archive_id: archive_config.archive_id.unwrap_or(ARCHIVE_ID_DEFAULT),
    };

    let directory = archive_config
        .mark_file_path()
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "aeron.archive.mark.file.dir has no directory".to_owned())?;

    ArchiveMarkFile::create(
        &directory,
        &header,
        ERROR_BUFFER_LENGTH_DEFAULT,
        FILE_PAGE_SIZE_DEFAULT,
        i64::from(std::process::id()),
    )
    .map_err(|error| format!("could not create the archive mark file: {error}"))
}

fn fail(error: &dyn std::fmt::Display) -> ExitCode {
    eprintln!("deepmsg-archiving-media-driver: {error}");
    ExitCode::FAILURE
}
