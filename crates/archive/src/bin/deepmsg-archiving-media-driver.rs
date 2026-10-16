//! The archiving media driver: a media driver and an archive in one process.
//!
//! This is the reference's `ArchivingMediaDriver` (`ArchivingMediaDriver
//! .java:79-106`), and it is the shape the C archive suite drives: 214 of the
//! 276 archive spawns a rehearsal of the whole suite counted are this class,
//! which is why P2-S1 builds this binary and no other (plan §2.1).
//!
//! It is one of the reference's two ways to launch an archive, and the only
//! thing that distinguishes it from the other — [`deepmsg_archive::launch`]'s
//! `Archive.main`, the archive that attaches to a driver somebody else runs — is
//! **who runs the driver**. That module is where the archive's setup and its
//! loop live; what is left here is the half that is this process's own: the
//! driver, and the three wires the reference runs between the two.
//!
//! # The three wires the reference runs between the two
//!
//! `ArchivingMediaDriver.launch` launches the driver, then the archive, and
//! hands the archive three things (`:87-97`):
//!
//! 1. **The driver's `ERRORS` counter**, taken from the driver's own counters
//!    buffer rather than allocated fresh (`:87-89`) — so an archive error is
//!    counted where a driver error is.
//! 2. **The driver's shared agent invoker** (`:94`), which is how the archive
//!    drives the driver. In this build that seam is [`Driver::do_work`].
//! 3. **The driver's aeron directory** (`:95`), which is where the CnC file and
//!    the archive's mark file both live.
//!
//! # Why the driver must be SHARED or INVOKER
//!
//! The driver is driven from the archive's loop, which is only the same thing as
//! the reference's `mediaDriverAgentInvoker` wiring when the driver starts no
//! threads of its own — otherwise [`Driver::do_work`] under `DEDICATED` drives
//! the conductor and nothing else, so the sender and the receiver would stop and
//! the archive would sit there looking healthy. A driver configured that way is
//! therefore refused rather than run into that stall.
//!
//! # The two commands that cannot be waited for
//!
//! Under this half the driver advances only when the archive's loop drives it,
//! so the way up goes through the client's asynchronous path. The archive that
//! attaches has no such problem. Both are written down in
//! [`deepmsg_archive::launch`].

use std::process::ExitCode;

use deepmsg_archive::launch::{self, DriverHalf};
use deepmsg_archive::server::config::{ArchiveConfig, ThreadingMode as ArchiveThreadingMode};
use deepmsg_cnc::{CncCreateError, CncFile, CncIdentity};
use deepmsg_core::clock;
use deepmsg_driver::config::{DriverConfig, ThreadingMode};
use deepmsg_driver::driver::Driver;
use deepmsg_driver::{cpuset, dir, sys};

const NAME: &str = "deepmsg-archiving-media-driver";

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

    // The archive half's mode, refused the way `deepmsg-archive` refuses it and
    // with the same words: two binaries that disagree about a mode cannot be
    // reasoned about from a configuration file, and this one used to run
    // `INVOKER` as `SHARED` without saying so.
    //
    // `INVOKER` asks for an agent the *caller* drives (`Archive.launch:253-256`
    // starts an `AgentInvoker` and no runner at all), and the caller here is
    // this program's own loop — `launch::run`. An agent nobody invokes is the
    // reference's `Archive.main` under `INVOKER`, which waits on
    // `barrier.await()` and serves nothing.
    if matches!(archive_config.threading_mode, ArchiveThreadingMode::Invoker) {
        return fail(&format_args!(
            "aeron.archive.threading.mode is INVOKER; this build runs the archive's agents \
             itself, and INVOKER asks for an agent the caller drives"
        ));
    }

    // Before anything touches the file system, and in the reference's order —
    // see `deepmsg-driver`'s own binary, which this follows to the letter.
    if let Err(error) = sys::install_stop_handler() {
        eprintln!("{NAME}: could not install the signal handler: {error}");
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
                "{NAME}: another driver is creating {}: EBUSY",
                driver_config.aeron_dir.display()
            );
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{NAME}: {error}");
            let _ = prepared.remove();
            return ExitCode::FAILURE;
        }
    };

    let mut driver = match Driver::new(cnc, &driver_config, affinity) {
        Ok(driver) => driver,
        Err(error) => {
            eprintln!("{NAME}: {error}");
            let _ = prepared.remove();
            return ExitCode::FAILURE;
        }
    };

    let mut half = DriverHalf::Owned(&mut driver);

    match launch::run(NAME, &mut half, &archive_config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{NAME}: {message}");
            half.close();
            let _ = prepared.remove();
            ExitCode::FAILURE
        }
    }
}

fn fail(error: &dyn std::fmt::Display) -> ExitCode {
    eprintln!("{NAME}: {error}");
    ExitCode::FAILURE
}
