//! The standalone archive: an archive that attaches to a media driver somebody
//! else runs.
//!
//! This is the reference's `io.aeron.archive.Archive` — `Archive.main`
//! (`Archive.java:183-184`), "launch an `Archive` that communicates with an out
//! of process `MediaDriver` and await a shutdown signal". It is the third of the
//! three main classes the reference's archive suite spawns, and the one this
//! build was missing: a rehearsal of the whole suite counted 276 archive spawns,
//! 214 of them `ArchivingMediaDriver` (which `deepmsg-archiving-media-driver`
//! replaces) and **56 of them this class** (`crates/tools/src/bin/archive-shim.rs`
//! carries the same count in its table).
//!
//! What the 56 are for is in `TestStandaloneArchive.h`: it spawns this class
//! (`:136`) and then waits, **with no timeout**, for the archive mark file to
//! reach its header length (`:166`). The resilience suite's kill-and-restart
//! cases need an archive that can die without the driver dying with it
//! (`:1671-1672`: "a separate standalone archive process talking to the same
//! aeron.dir"), which is a topology the one-process
//! `ArchivingMediaDriver` cannot give them however well it works.
//!
//! Everything this binary does past parsing is [`deepmsg_archive::launch`]: the
//! only thing it decides is that the driver is **not here**. That half — reading
//! the CnC file another process published, blocking on commands that driver
//! answers itself, and allocating the archive's own error counter rather than
//! being handed the driver's — is what `DriverHalf::Attached` is.
//!
//! # What is not here
//!
//! `aeron.archive.threading.mode` other than `SHARED` is refused by name rather
//! than run as something else. The reference's default for it is `DEDICATED`,
//! and a dedicated archive is a conductor, a recorder and a replayer on threads
//! of their own (P2-9a); this build runs the archive on the calling thread, so
//! DEDICATED and INVOKER are refused loudly until that slice exists. The
//! reference's own suite sets `SHARED` (`TestStandaloneArchive.h:121`).
//!
//! Property **files** named on the command line are not read either: the
//! reference's `loadPropertiesFiles` takes URLs or filenames, and this build's
//! parsers read `-D` only. Nothing in the C suite passes one.

use std::process::ExitCode;

use deepmsg_archive::launch::{self, DriverHalf};
use deepmsg_archive::server::config::{ArchiveConfig, ThreadingMode};
use deepmsg_driver::sys;

const NAME: &str = "deepmsg-archive";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let archive_config = match ArchiveConfig::from_args(args.iter().cloned()) {
        Ok(config) => config,
        Err(error) => return fail(&format_args!("{error}")),
    };

    if !matches!(archive_config.threading_mode, ThreadingMode::Shared) {
        return fail(&format_args!(
            "aeron.archive.threading.mode is {}; this build runs the archive on the calling \
             thread and needs it to be SHARED",
            archive_config.threading_mode
        ));
    }

    // The signal is how the suite stops an archive, and how the kill-and-restart
    // cases do it too — so this is installed before anything is served, in the
    // same place the archiving media driver installs it.
    if let Err(error) = sys::install_stop_handler() {
        eprintln!("{NAME}: could not install the signal handler: {error}");
        return ExitCode::FAILURE;
    }

    match launch::run(NAME, &mut DriverHalf::Attached, &archive_config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => fail(&format_args!("{message}")),
    }
}

fn fail(error: &dyn std::fmt::Display) -> ExitCode {
    eprintln!("{NAME}: {error}");
    ExitCode::FAILURE
}
