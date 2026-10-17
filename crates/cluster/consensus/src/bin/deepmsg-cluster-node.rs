//! One consensus module process, the counterpart of
//! `io.aeron.cluster.ConsensusModule.main` (`ConsensusModule.java:251-274`).
//!
//! The reference has two ways to run a node: `ClusteredMediaDriver.main`, which
//! starts a media driver, an archive and a consensus module in one process, and
//! `ConsensusModule.main`, which attaches to a driver and an archive somebody
//! else runs. This is the second — the one a deployment that keeps the media on
//! one process and the consensus state on another uses, and the one the
//! heterogeneous harness drives for LC-2/LC-3.
//!
//! # Arguments
//!
//! The reference's argument list is **property files** (loaded by
//! `SystemUtil.loadPropertiesFiles`, which the reference calls with the default
//! `PropertyAction.REPLACE`). A `-Dname=value` argument is accepted here as
//! well, because this build's tests have no JVM to put one on; it is applied
//! first, so a file overrides it — the same order `REPLACE` produces when the
//! JVM's `-D` values exist before `main` runs.
//!
//! # Termination
//!
//! The agent ends in one of two ways and they exit differently:
//!
//! * `ABORT`, `SHUTDOWN`, or a closed client — `do_work` returns
//!   [`ClusterError::AgentTermination`](deepmsg_cluster::ClusterError::AgentTermination);
//!   the agent is closed and the process exits **0**. That is the reference's
//!   `ClusterTerminationException`, which `AgentRunner` treats as a normal stop.
//! * Any other error — the process prints it, closes what it can, and exits
//!   **1**.
//!
//! `SIGINT`/`SIGTERM` stop the duty cycle and close the agent cleanly. The
//! handler is installed **after** `launch`: on this build `launch` blocks in
//! the election's waits (a service's `JoinLog` ack, the log recording), and a
//! swallowed signal there would leave a process that ignores a kill. Before
//! launch the default disposition still applies.

use std::path::Path;
use std::process::ExitCode;

use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig};
use deepmsg_cluster_common::config::Properties;
use deepmsg_driver::idle::Strategy;
use deepmsg_driver::sys;

const NAME: &str = "deepmsg-cluster-node";

const USAGE: &str = "\
Usage: deepmsg-cluster-node [options] [properties file ...]

  -Dname=value   set one cluster or driver property (a file overrides it)
  -h, --help     print this message

The property file format and the names are the reference's: see
`aeron.cluster.*` in `docs/compat.md`. The driver and the archive are a
separate process in the same `-Daeron.dir` (the
`deepmsg-archiving-media-driver` binary).
";

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

    let config = ConsensusModuleConfig::from_properties(&properties).map_err(|error| {
        format!("the configuration is refused: {error} (see `aeron.cluster.*` in docs/compat.md)")
    })?;

    // The duty-cycle clock: one idle strategy for the quiet cycles, the same
    // name the agent uses inside its own waits (`aeron.cluster.idle.strategy`).
    let mut idle = Strategy::by_agrona_name(&config.idle_strategy).ok_or_else(|| {
        format!(
            "aeron.cluster.idle.strategy={} names no strategy this build has",
            config.idle_strategy
        )
    })?;

    let mut agent = ConsensusModuleAgent::launch(config).map_err(|error| error.to_string())?;

    if let Err(error) = sys::install_stop_handler() {
        let _ = agent.close();
        return Err(format!("could not install the signal handler: {error}"));
    }

    while sys::stop_signal().is_none() {
        match agent.do_work() {
            Ok(work) => idle.idle(work),
            // `ClusterTerminationException` in the reference: an orderly end,
            // including the one `ABORT`/`SHUTDOWN` ask for.
            Err(error) if error.is_termination() => {
                return agent.close().map_err(|error| error.to_string());
            }
            Err(error) => {
                let close = agent.close();
                return Err(match close {
                    Ok(()) => error.to_string(),
                    Err(close_error) => format!("{error}; closing also failed: {close_error}"),
                });
            }
        }
    }

    agent.close().map_err(|error| error.to_string())
}
