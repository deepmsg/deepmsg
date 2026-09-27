//! Standalone media driver process — the counterpart of the reference
//! `aeronmd` (deployment forms: M17).
//!
//! The shape of `main` is the reference's (`aeron-driver/src/main/c/aeronmd.c:100-190`),
//! and the order matters more than it looks:
//!
//! 1. parse the configuration,
//! 2. install the signal handlers, before anything can take a long time,
//! 3. settle the aeron directory — somebody else may already own it,
//! 4. create the CnC file (not published yet),
//! 5. start the conductor, which writes the first heartbeat and then publishes
//!    the ready version,
//! 6. loop until a termination command or a signal,
//! 7. publish the stop signal, flush, and delete the directory if configured.
//!
//! Step 3 before step 4 is the reference's order too, and it is the one that
//! cannot be swapped: creating a file over a live driver's is not something a
//! later check can undo. Step 2 before step 3 is the second one that cannot be
//! swapped, and for the opposite reason: it is the only step whose failure
//! mode is "the process is killed mid-delete".

use std::io;
use std::process::ExitCode;

use deepmsg_cnc::{CncCreateError, CncFile, CncIdentity};
use deepmsg_core::clock;
use deepmsg_driver::conductor::Conductor;
use deepmsg_driver::config::DriverConfig;
use deepmsg_driver::idle::Backoff;
use deepmsg_driver::{dir, sys};

fn main() -> ExitCode {
    let config = match DriverConfig::from_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => return fail(&format_args!("{error}")),
    };

    // Before anything touches the file system. `prepare` may be deleting a
    // large stale directory tree and `create` may be allocating 46 MB, and a
    // `SIGTERM` during either of those must take the clean path out rather
    // than the default disposition's — which is exactly the order the reference
    // installs its handlers in, before its context does any work
    // (`aeron-driver/src/main/c/aeronmd.c:112-113`).
    if let Err(error) = sys::install_stop_handler() {
        eprintln!("deepmsg-driver: could not install the signal handler: {error}");
        return ExitCode::FAILURE;
    }

    let now_ms = clock::epoch_millis();

    // Holding this is what makes the directory this process's to delete: every
    // failure arm below goes through it, so none of them can remove a directory
    // this process never prepared.
    let prepared = match dir::prepare(&config, now_ms) {
        Ok(prepared) => prepared,
        Err(error) => return fail(&format_args!("{error}")),
    };

    for notice in prepared.notices() {
        match notice {
            dir::Notice::DirectoryExists { path } => {
                eprintln!("deepmsg-driver: WARNING: {} exists", path.display());
            }
        }
    }

    let identity = CncIdentity {
        liveness_timeout_ns: config.client_liveness_timeout_ns,
        start_timestamp_ms: now_ms,
        pid: i64::from(std::process::id()),
    };

    // The file is created but not published: nothing may read it until the
    // conductor has a heartbeat to show, and the conductor is what publishes.
    let cnc = match CncFile::create(&config.aeron_dir, &config.layout, &identity) {
        Ok(cnc) => cnc,
        // Another driver won the `O_EXCL` race — it created its file between
        // `prepare` looking at the directory and this call. Deleting the
        // directory here would unlink the winner's `cnc.dat` and its
        // `publications/` and `images/` trees and leave it running as a driver
        // nobody can reach: the failure the directory discipline exists to
        // prevent, reached through the other door. The directory is not ours,
        // so it is not ours to remove.
        Err(CncCreateError::Io(source)) if io::ErrorKind::AlreadyExists == source.kind() => {
            eprintln!(
                "deepmsg-driver: another driver is creating {}: EBUSY",
                config.aeron_dir.display()
            );
            return ExitCode::FAILURE;
        }
        // Everything past this point happens in a directory this process
        // created, so removing it on the way out is removing our own work.
        Err(error) => {
            eprintln!("deepmsg-driver: {error}");
            let _ = prepared.remove();
            return ExitCode::FAILURE;
        }
    };

    let mut conductor = match Conductor::new(cnc, &config) {
        Ok(conductor) => conductor,
        Err(error) => {
            eprintln!("deepmsg-driver: {error}");
            let _ = prepared.remove();
            return ExitCode::FAILURE;
        }
    };

    eprintln!(
        "deepmsg-driver: media driver running, aeron.dir={} (CnC version {}, pid {})",
        config.aeron_dir.display(),
        deepmsg_core::version::format_version(deepmsg_core::version::CNC_VERSION),
        std::process::id()
    );

    let mut idle = Backoff::new();
    while conductor.is_running() && sys::stop_signal().is_none() {
        idle.idle(conductor.do_work());
    }

    shutdown(&mut conductor, prepared)
}

/// Stop the driver the way the reference does, and report how it stopped.
fn shutdown(conductor: &mut Conductor, prepared: dir::PreparedDir) -> ExitCode {
    if let Err(error) = conductor.close() {
        eprintln!("deepmsg-driver: the shutdown signal could not be published: {error}");
    }

    // After the close, never before: the flush inside it is what the *next*
    // driver reads to decide whether this one is still alive, and removing the
    // directory first would leave that check with nothing to read.
    if let Err(error) = prepared.remove() {
        eprintln!("deepmsg-driver: {error}");
    }

    let unhandled = conductor.unhandled_commands();
    let unknown = conductor.unknown_commands();
    if 0 != unhandled || 0 != unknown {
        eprintln!(
            "deepmsg-driver: {unhandled} commands were not implemented and {unknown} were not in the \
             protocol; {last:?} was the last, and the clients that sent one have timed out",
            last = conductor.last_unhandled(),
        );
    }

    match sys::stop_signal() {
        // The signal number, not a failure code: the reference records what it
        // was stopped by (`aeronmd.c:39-42`), and returning 0 here would make a
        // signalled driver indistinguishable from one that was asked to stop.
        Some(signal) => {
            eprintln!("deepmsg-driver: stopping on signal {signal}");
            ExitCode::from(u8::try_from(signal).unwrap_or(1))
        }
        None => ExitCode::SUCCESS,
    }
}

fn fail(message: &dyn std::fmt::Display) -> ExitCode {
    eprintln!("deepmsg-driver: {message}");
    ExitCode::FAILURE
}
