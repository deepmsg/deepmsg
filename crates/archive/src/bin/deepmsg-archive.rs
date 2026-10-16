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
//! `aeron.archive.threading.mode=INVOKER` is refused by name rather than run as
//! something else, and for a reason that is this program's rather than a missing
//! implementation: `INVOKER` is the mode where the **caller** drives the
//! archive's agent (`Archive.launch:253-256` starts an `AgentInvoker` and no
//! runner), and a process that is its own caller has nobody to be invoked by —
//! the reference's `Archive.main` under `INVOKER` waits on `barrier.await()` and
//! serves nothing at all.
//!
//! `DEDICATED` and `SHARED` are both served. The reference's own suite sets
//! `SHARED` (`TestStandaloneArchive.h:121`), and what a deployment says nothing
//! about is `DEDICATED` (`Archive.java:393`), which is the mode with threads of
//! its own.
//!
//! ★ The replayer is still driven inside the conductor's turn under `DEDICATED`
//! as well: it is the other half of P2-9a, and until it lands a DEDICATED archive
//! here runs two of the reference's three agents.
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

    // `DEDICATED` and `SHARED` are both run; `INVOKER` is not, and the reason is
    // a property of this program rather than a missing implementation: in the
    // reference `INVOKER` means the *caller* drives the archive's agent
    // (`Archive.launch:253-256` starts an `AgentInvoker` and no runner at all),
    // and there is no caller here — `launch::run` is the loop. A process whose
    // archive is nobody's to invoke is the reference's `Archive.main` under
    // `INVOKER`, which waits on `barrier.await()` and serves nothing.
    if matches!(archive_config.threading_mode, ThreadingMode::Invoker) {
        return fail(&format_args!(
            "aeron.archive.threading.mode is INVOKER; this build runs the archive's agents \
             itself, and INVOKER asks for an agent the caller drives"
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
