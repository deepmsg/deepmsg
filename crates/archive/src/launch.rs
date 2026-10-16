//! Launching an archive, in the two shapes the reference launches it in.
//!
//! The reference has two ways in, and the difference between them is one thing:
//! **who runs the driver**.
//!
//! - `ArchivingMediaDriver.launch` (`ArchivingMediaDriver.java:79-106`) runs a
//!   media driver and an archive **in one process**, and hands the archive
//!   three things (`:87-97`): the driver's `ERRORS` counter, the driver's shared
//!   agent invoker, and the driver's aeron directory. In this build that is
//!   [`DriverHalf::Owned`], and the seam for the invoker is [`Driver::do_work`],
//!   whose own documentation says it is "the entry point for a caller that
//!   embeds the driver and drives it itself".
//! - `Archive.main` (`Archive.java:183-184`) "launch[es] an `Archive` that
//!   communicates with an out of process `MediaDriver` and await[s] a shutdown
//!   signal" — the archive **attaches** to a driver somebody else started. That
//!   is [`DriverHalf::Attached`]: nothing here advances the driver, because the
//!   process that owns it does, and the archive allocates an error counter of
//!   its own rather than being handed one (`Archive.java:1354-1361`).
//!
//! The C archive suite spawns both: 214 of the 276 archive spawns a rehearsal
//! counted are `ArchivingMediaDriver`, and 56 are `Archive`
//! (`TestStandaloneArchive.h:136`, which is what the resilience suite's
//! kill-and-restart cases need — an archive that can die without the driver
//! dying with it).
//!
//! # Why the loop is here and not in the driver
//!
//! [`Driver::run`] never returns: it threads the mode's runners and keeps slot
//! 0 on the calling thread. A process that also has an archive to drive
//! therefore cannot call it, and drives both itself, in the reference's order —
//! the driver's work, then the archive's. That is only the same thing as the
//! reference when the driver starts no threads of its own, so a driver
//! configured otherwise is refused by the binary that owns it rather than run
//! into a stall nobody would see. Under [`DriverHalf::Attached`] there is no
//! such order to keep: the driver is not here.
//!
//! # When this process says it is ready
//!
//! The suite waits for the archive by polling the length of `archive-mark.dat`
//! until it passes `ARCHIVE_MARK_FILE_HEADER_LENGTH`, which is 8192
//! (`TestArchive.h:141-153`, `TestProcessUtils.h:40`; `TestStandaloneArchive.h:166`
//! is the same wait). That is a **file-length** probe, and the file reaches 8192
//! as soon as it is created — its own comment says it is "an indicator that
//! Archive process is running", not that it is serving. The genuine ready byte
//! is the mark file's version field, which [`ArchiveMarkFile::signal_ready`]
//! writes.
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
//! Under [`DriverHalf::Owned`], [`Client::add_subscription`] blocks until the
//! driver answers, and the driver only advances when this loop calls
//! [`Driver::do_work`]. Waiting for it here would wait forever. Everything on
//! the way up therefore goes through the client's asynchronous path — the CnC
//! file's publication, and both control subscriptions — each polled with a
//! driver turn in between. Under [`DriverHalf::Attached`] the problem does not
//! exist and is not solved: the driver is a process of its own and answers on
//! its own, so the blocking calls are the ones to use, which is what the
//! reference's `Aeron.addSubscription` does there.

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_client::client::{AsyncAddPoll, Client};
use deepmsg_cnc::CncFile;
use deepmsg_cnc::create::FILE_PAGE_SIZE_DEFAULT;
use deepmsg_core::clock;
use deepmsg_core::uri::ChannelUri;
use deepmsg_driver::driver::Driver;
use deepmsg_driver::sys;

use crate::catalog::Catalog;
use crate::mark_file::{ArchiveMarkFile, Header};
use crate::server::conductor::{ARCHIVE_ID_DEFAULT, ArchiveConductor};
use crate::server::config::{ArchiveConfig, ThreadingMode};
use crate::server::counters::DutyCycleRequests;
use crate::server::counters::{
    ARCHIVE_ERROR_COUNT_TYPE_ID, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, archive_id_key,
    archive_id_label, find_archive_id_counter,
};
use crate::server::recorder::{RecorderHalf, RecorderThread};
use crate::server::replayer::{ReplayerHalf, ReplayerThread};
use crate::server::threading::{self, Role};

/// `SystemCounterDescriptor.ERRORS.id()` (`ArchivingMediaDriver.java:87`) —
/// the driver's error counter, which the archive writes its own errors into
/// instead of keeping one of its own.
///
/// This is the slot our driver always allocates it in; it is only an answer
/// under [`DriverHalf::Owned`], where the driver is this process's own. See
/// [`DriverHalf::error_counter_id`].
const DRIVER_ERROR_COUNTER_ID: i32 = 15;

/// `AeronArchive.Configuration.RECORDING_EVENTS_STREAM_ID_DEFAULT`
/// (`client/AeronArchive.java:2791`).
///
/// Nothing here records yet, so the channel this id belongs to is never
/// created (`events_channel` is `None`); the number is written into the mark
/// file because the reference writes it there.
const RECORDING_EVENTS_STREAM_ID_DEFAULT: i32 = 30;

/// How long this process will wait for the driver to answer a command.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// Who runs the media driver this archive serves: this process, or another.
pub enum DriverHalf<'a> {
    /// The driver is this process's own, and only this process can advance it.
    Owned(&'a mut Driver),
    /// The driver belongs to another process and advances itself.
    Attached,
}

impl DriverHalf<'_> {
    /// One turn of the driver, where this process is the one that owes it, and
    /// how much work it did.
    ///
    /// The count is what the idle strategy is given, and it is the reference's
    /// own arrangement: an archive that owns its driver drives it from inside
    /// `ArchiveConductor.doWork` (`invokeDriverConductor`, `:426`, which
    /// `SharedModeArchiveConductor.doWork:57-64` calls and adds in), so the
    /// driver's work is part of the turn's work and a turn that drove the driver
    /// is not a turn that idles.
    fn turn(&mut self) -> usize {
        match self {
            Self::Owned(driver) => driver.do_work(),
            Self::Attached => 0,
        }
    }

    /// Get the client, and with it whatever the driver has to be told first.
    ///
    /// Owned: the CnC file is created unpublished and the driver's conductor is
    /// what publishes it, so waiting for it *is* driving
    /// (`CncFile::try_open` is the same gate the test harness waits on,
    /// `tests/src/driver.rs:505`). Attached: publication is the other process's
    /// business, and [`Client::connect_with_timeout`] is itself the wait.
    fn connect(&mut self, archive_config: &ArchiveConfig) -> Result<Client, String> {
        let aeron_dir = archive_config.aeron_dir.as_path();

        match self {
            Self::Owned(_) => {
                let deadline = Instant::now() + COMMAND_TIMEOUT;
                while CncFile::try_open(aeron_dir).is_err() {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "the driver did not publish its CnC file in {}",
                            aeron_dir.display()
                        ));
                    }

                    self.turn();
                }
            }
            Self::Attached => {}
        }

        Client::connect_with_timeout(aeron_dir, COMMAND_TIMEOUT)
            .map_err(|error| format!("could not connect to the driver: {error}"))
    }

    /// Ask for a subscription and wait for the answer.
    ///
    /// Owned: the asynchronous path, polled with a driver turn in between —
    /// `Client::add_subscription` would wait for an answer only this loop can
    /// cause. Attached: the blocking call, which is what the reference does. See
    /// the module note.
    fn add_subscription(
        &mut self,
        client: &mut Client,
        channel: &str,
        stream_id: i32,
    ) -> Result<i64, String> {
        let Self::Owned(driver) = self else {
            return client
                .add_subscription(channel, stream_id, COMMAND_TIMEOUT)
                .map_err(|error| {
                    format!("the driver refused a subscription to {channel}: {error}")
                });
        };

        let deadline = Instant::now() + COMMAND_TIMEOUT;
        let add = client
            .async_add_subscription(channel, stream_id, COMMAND_TIMEOUT)
            .map_err(|error| format!("could not ask for a subscription to {channel}: {error}"))?;

        loop {
            // Both sides, or neither moves: the driver reads the command ring
            // only when it is driven, and the client hears the answer only when
            // it polls the ring the driver writes back into. Driving one of them
            // is a deadlock with extra steps.
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

    /// The counter this archive's errors are counted in.
    ///
    /// Owned: the driver's own `ERRORS` counter, taken from its counters buffer
    /// rather than allocated fresh (`ArchivingMediaDriver.java:87-89`) — so an
    /// archive error is counted where a driver error is.
    ///
    /// Attached: one the archive allocates for itself, which is the reference's
    /// answer to the same absence (`Archive.java:1354-1361` is the `null ==
    /// errorCounter` arm of `conclude`, reached whenever no `ArchivingMediaDriver`
    /// handed one over): type `ARCHIVE_ERROR_COUNT_TYPE_ID` (101), keyed by the
    /// archive id, labelled `Archive Errors` + the archive id
    /// (`ArchiveCounters.allocateErrorCounter` `:70-92`).
    fn error_counter_id(&mut self, client: &mut Client, archive_id: i64) -> Result<i32, String> {
        let Self::Attached = self else {
            return Ok(DRIVER_ERROR_COUNTER_ID);
        };

        // An archive id another archive is already serving is refused **by the
        // 101 counter it would have allocated** (`Archive.java:1356-1360`), and
        // the message is the reference's own. This is the standalone path's
        // check; the 105 one the launcher makes for both paths is a different
        // question with a different answer (`ArchiveConductor.java:1587-1593`).
        let counters = client
            .counters_reader()
            .ok_or_else(|| "the counter region is not readable".to_owned())?;

        if find_archive_id_counter(&counters, ARCHIVE_ERROR_COUNT_TYPE_ID, archive_id).is_some() {
            return Err(format!("found existing archive for archiveId={archive_id}"));
        }

        let counter = client
            .add_counter(
                ARCHIVE_ERROR_COUNT_TYPE_ID,
                &archive_id_key(archive_id),
                &archive_id_label("Archive Errors", archive_id),
                COMMAND_TIMEOUT,
            )
            .map_err(|error| format!("could not allocate the archive's error counter: {error}"))?;

        Ok(counter.counter_id())
    }

    /// Close what this process owns of the driver.
    ///
    /// Public because the failure path of the binary that owns the driver
    /// unwinds it the same way the signal path does.
    pub fn close(&mut self) {
        if let Self::Owned(driver) = self {
            let _ = driver.close();
        }
    }
}

/// Everything after the driver exists, so that one failure path unwinds it.
///
/// `name` is the binary's own name, which two of the lines below carry: they are
/// read by a person watching a suite run, and by nothing else.
pub fn run(
    name: &str,
    half: &mut DriverHalf<'_>,
    archive_config: &ArchiveConfig,
) -> Result<(), String> {
    let aeron_dir = archive_config.aeron_dir.clone();

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

    let mut client = half.connect(archive_config)?;

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
        Some(channel) => {
            Some(half.add_subscription(&mut client, channel, archive_config.control_stream_id)?)
        }
        None => None,
    };

    // Always, and with no condition on it: the reference builds the local one
    // unconditionally (`:239-240`), and it is what makes an IPC client able to
    // reach the archive at all.
    let local_subscription_id = half.add_subscription(
        &mut client,
        &archive_config.local_control_channel,
        archive_config.local_control_stream_id,
    )?;

    // Where the reference concludes it (`Archive.java:1354-1361`), which is
    // before the ready byte rather than after it: an archive that refuses this
    // id should not have signalled ready under it first.
    let archive_id = archive_config.archive_id.unwrap_or(ARCHIVE_ID_DEFAULT);
    let error_counter_id = half.error_counter_id(&mut client, archive_id)?;

    let cnc = CncFile::open_writable(&aeron_dir, COMMAND_TIMEOUT)
        .map_err(|error| format!("could not open the counter region: {error}"))?;

    // ★ P2-9b: the three agents' duty-cycle counters, allocated where the
    // reference allocates them (`Archive.Context.conclude`, `Archive.java:1402-1480`)
    // — at start-up, before the conductor has a turn to be measured by. Six
    // counters, of which a `SHARED` archive measures one pair (see [`DutyCycles`]).
    //
    // The region is read through the CnC file rather than the client, because the
    // allocation below needs the client mutably and a reader taken from it would
    // hold it.
    let duty_cycle_requests = request_duty_cycles(&mut client, archive_config, archive_id)?;

    // An archive id that is already being recorded by is one this archive
    // refuses to serve (`Archive.java:1589-1595`): the reference looks for a
    // 105 — the recorder's max write time, the one counter a second archive
    // would fight the first over — and throws out of `conclude` if it finds one.
    // The check is a **read** of the counters region, so unlike the allocation
    // itself it can happen here, before anything is served.
    if let Some(counters) = cnc.counters() {
        if find_archive_id_counter(
            &counters,
            ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            archive_id,
        )
        .is_some()
        {
            return Err(format!(
                "existing max write time counter detected for archiveId={archive_id}"
            ));
        }
    }

    // The recordings this archive holds. Opened here rather than inside the
    // conductor for the same reason the mark file is made here: it is a
    // file-system step, and this is where the file system is already being
    // dealt with.
    //
    // Two things about the call are worth stating, because the reference's are
    // implicit. The capacity is `aeron.archive.catalog.capacity` (`Archive.java:487`,
    // read as a size at `:861`) — **not** `aeron.archive.max.catalog.entries`,
    // which this used to pass the default of: that name has no reader in the
    // reference either (`maxCatalogEntries()` is defined and never called), so
    // what a deployment set there was being used as the file's starting size.
    // And a
    // **fresh** catalog starts at recording id 0, because the reference's field
    // starts at 0 and `Archive` constructs its `Catalog` without seeding it
    // (`Catalog.java:148`, `Archive.java:1503-1511`).
    let mut catalog = Catalog::open_or_create(
        &archive_config.archive_dir,
        archive_config.catalog_capacity,
        0,
    )
    .map_err(|error| format!("could not open the archive's catalog: {error}"))?;

    // **The repair the caller has to ask for**, and this is that caller: a
    // recording that was being written when the last archive died has a `VALID`
    // record and a **null** stop position, because the position is written when
    // a recording stops and this one never did. The reference repairs every
    // descriptor as it loads the catalog (`Catalog.java:228-231`); this build's
    // repair is a method precisely so a *reader* — `ArchiveTool describe` is one
    // — does not write to somebody's catalog because it looked at it, and the
    // archive starting up is the caller that means it.
    //
    // Without this the recording is still listed and still replayable **up to a
    // position** — and a reader that replays to the end gets
    // `replay length must be positive: ... stopPosition=-1`, because "to the
    // end" of a recording with no stop position is not a length. That is what
    // the C suite's kill-and-restart cases met.
    //
    // `None` is the checksum, which is what the reference passes by default
    // (`Archive.Configuration.recordChecksum()` is empty unless
    // `aeron.archive.record.checksum` names a class); the recording's stop
    // **timestamp** is now, which is the reference's answer too
    // (`Catalog.java:1093-1095`) — the time it actually stopped is not knowable
    // from the files.
    let repaired = catalog
        .refresh_and_fix(None, clock::epoch_millis())
        .map_err(|error| format!("could not repair the archive's catalog: {error}"))?;

    if repaired > 0 {
        println!("{name}: repaired {repaired} recording(s) with no stop position");
    }

    // Last of the setup, so that nothing can be waiting on a mark file this
    // process cannot yet serve. See the module note for why this is later than
    // the reference's own `signalReady`.
    let mark_file = create_mark_file(archive_config, control_channel.as_deref(), &aeron_dir)?;

    // The ready byte, which is the mark file's *version* field rather than its
    // length (`ArchiveMarkFile.java:331-338`). Written here, after everything
    // that serves is up, rather than where the reference writes it.
    mark_file
        .signal_ready(clock::epoch_millis())
        .map_err(|error| format!("could not signal the archive ready: {error}"))?;

    // ★ P2-9a, and the whole of what `aeron.archive.threading.mode` decides:
    // under `DEDICATED` the recorder and the replayer are agents on threads of
    // their own (`DedicatedModeArchiveConductor.java:54-55`), so what the
    // conductor holds is a channel to each; under `SHARED` both are the
    // conductor's own objects, driven inside the conductor's turn
    // (`SharedModeArchiveConductor.java:57-66`). The **conductor** is on a thread
    // of its own either way when there is more than one agent — see below.
    let dedicated = matches!(archive_config.threading_mode, ThreadingMode::Dedicated);

    // The workers are built here and started below, once the conductor that will
    // talk to them exists: a worker whose conductor was never built is a thread
    // nothing can stop.
    let (recorder, recorder_worker) = if dedicated {
        let (thread, worker) = RecorderThread::pair(
            archive_id,
            &aeron_dir,
            // `ctx.recorderIdleStrategy()`, which falls back to the archive's own
            // supplier when nothing names one (`Archive.java:1423-1437`).
            archive_config.idle_strategy.strategy(),
            COMMAND_TIMEOUT,
        )?;

        (RecorderHalf::threaded(thread), Some(worker))
    } else {
        (RecorderHalf::new(archive_id), None)
    };

    let (replayer, replayer_worker) = if dedicated {
        let (thread, worker) = ReplayerThread::pair(
            &aeron_dir,
            // `ctx.replayerIdleStrategy()`, the same rule (`:1432-1440`).
            archive_config.idle_strategy.strategy(),
            COMMAND_TIMEOUT,
        )?;

        (ReplayerHalf::threaded(thread), Some(worker))
    } else {
        (ReplayerHalf::new(), None)
    };

    let mut conductor = ArchiveConductor::new(
        archive_config,
        cnc,
        mark_file,
        catalog,
        error_counter_id,
        duty_cycle_requests,
        remote_subscription_id,
        local_subscription_id,
        recorder,
        replayer,
    )
    .map_err(|error| format!("could not build the archive's conductor: {error}"))?;

    // stdout, not stderr: this is written on every healthy start, and the
    // reference's harness treats the archive's stderr as a diagnostic channel.
    println!(
        "{name}: archive running, aeron.dir={}, archive.id={archive_id} (pid {})",
        aeron_dir.display(),
        std::process::id()
    );

    // The reference's `ctx.idleStrategy()` (`Archive.java:1397-1399`), made once
    // for the conductor's own loop — `AgentRunner.run` asks its context for one
    // strategy per runner and idles with it after every pass
    // (`org.agrona.concurrent.AgentRunner`), which is what this is.
    //
    // ★ It is no longer hardcoded to `yield`. That was the suite's own setting
    // written into the code (`TestArchive.h:47` passes
    // `aeron.archive.idle.strategy=yield`), so nothing could tell the two apart
    // and a deployment that asked for anything else was told nothing. The
    // reference's default is a `BackoffIdleStrategy` (`:420`), and so is this
    // one's.
    let idle = archive_config.idle_strategy.strategy();

    // One agent, one thread: without a recorder of its own there is nothing for
    // the conductor's thread to be apart *from*, and the archive runs where it
    // always has — on the caller's.
    let Some(recorder_worker) = recorder_worker else {
        return run_conductor(name, &mut conductor, &mut client, half, idle);
    };

    let recorder_handle =
        threading::spawn(Role::Recorder, archive_config.thread_naming, move || {
            recorder_worker.run()
        })
        .map_err(|error| format!("could not start the recorder's thread: {error}"))?;

    // ★ The replayer's thread, and there is one whenever there is a recorder's:
    // both are what the same mode asks for.
    let replayer_handle = match replayer_worker {
        Some(worker) => Some(
            threading::spawn(Role::Replayer, archive_config.thread_naming, move || {
                worker.run()
            })
            .map_err(|error| format!("could not start the replayer's thread: {error}"))?,
        ),
        None => None,
    };

    // ★ The conductor is an agent on a thread of its own in **both** the
    // reference's running modes — `DEDICATED` and `SHARED` alike are
    // `AgentRunner.startOnThread(archive.conductorRunner, ctx.threadFactory())`
    // (`Archive.java:262`) — and the process's own thread only waits
    // (`barrier.await()`, `:201`). This build puts it there when there is more
    // than one agent, which is where the two would otherwise be on one thread.
    //
    // What it costs is a scope: an archive that owns its media driver drives it
    // from this same loop, so the thread borrows `&mut Driver` rather than
    // owning it.
    let outcome = std::thread::scope(|scope| {
        let conductor_thread = threading::spawn_scoped(
            scope,
            Role::Conductor,
            archive_config.thread_naming,
            move || run_conductor(name, &mut conductor, &mut client, half, idle),
        )
        .map_err(|error| format!("could not start the conductor's thread: {error}"))?;

        conductor_thread
            .join()
            .unwrap_or_else(|_| Err("the archive's conductor thread panicked".to_owned()))
    });

    // The conductor's end of both channels went with it, so the recorder's loop
    // has ended or is about to — [`RecorderWorker::run`] returns when the
    // commands channel is gone. Joining it here is what makes the stop a
    // shutdown rather than an exit: a session still writing its segment is read
    // to the end of its turn rather than abandoned mid-write.
    let _ = recorder_handle.join();

    if let Some(handle) = replayer_handle {
        let _ = handle.join();
    }

    outcome
}

/// The archive's turn loop, on whichever thread runs it
/// (`ArchiveConductor.doWork` and the runner around it,
/// `org.agrona.concurrent.AgentRunner.run`).
///
/// `idle` is a strategy rather than a reference to one because
/// `AgentRunner.run` owns the one its context gave it, and so does this.
fn run_conductor(
    name: &str,
    conductor: &mut ArchiveConductor,
    client: &mut Client,
    half: &mut DriverHalf<'_>,
    mut idle: deepmsg_driver::idle::Strategy,
) -> Result<(), String> {
    loop {
        if let Some(signal) = sys::stop_signal() {
            conductor.signal_terminated();
            client.close();
            half.close();
            println!("{name}: stopped by signal {signal}");
            return Ok(());
        }

        // The driver first, so that everything the archive's turn hears is
        // already in the term buffers, then the archive. Under
        // [`DriverHalf::Attached`] the first of those is another process's, and
        // contributes no work — but the pass is still a pass.
        let driven = half.turn();

        let archived = conductor
            .do_work(client, clock::epoch_millis())
            .map_err(|error| format!("the control plane stopped: {error}"))?;

        idle.idle(driven + archived);
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
/// Ask for the six duty-cycle counters (`Archive.java:1402-1480`).
///
/// **Asked for, not allocated**: this runs before anything is driven, and in the
/// process where the archive and the driver are one binary nobody is driving the
/// driver yet — a blocking `add_counter` there waits for a heartbeat that will
/// not move. The conductor takes them up on its first turns
/// ([`DutyCycles::claim`]), which is the same shape the control-sessions counter
/// takes.
///
/// The labels carry the threading mode's own name and the threshold's
/// `formatDuration` spelling, so a reader sees what the archive was told rather
/// than what it assumed.
fn request_duty_cycles(
    client: &mut Client,
    config: &ArchiveConfig,
    archive_id: i64,
) -> Result<DutyCycleRequests, String> {
    DutyCycleRequests::request(
        client,
        archive_id,
        &config.threading_mode.to_string(),
        [
            config.conductor_cycle_threshold_ns,
            config.recorder_cycle_threshold_ns,
            config.replayer_cycle_threshold_ns,
        ],
        COMMAND_TIMEOUT,
    )
    .map_err(|error| format!("could not ask for the archive's duty cycle counters: {error}"))
}

fn create_mark_file(
    archive_config: &ArchiveConfig,
    control_channel: Option<&str>,
    aeron_dir: &Path,
) -> Result<ArchiveMarkFile, String> {
    let aeron_directory = aeron_dir
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
        // `aeron.archive.error.buffer.length` (`Archive.java:627`), which is what
        // this was hardcoded to.
        archive_config.error_buffer_length,
        FILE_PAGE_SIZE_DEFAULT,
        i64::from(std::process::id()),
    )
    .map_err(|error| format!("could not create the archive mark file: {error}"))
}
