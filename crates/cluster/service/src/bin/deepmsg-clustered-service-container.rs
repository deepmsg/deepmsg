//! One service container process, the counterpart of
//! `io.aeron.cluster.service.ClusteredServiceContainer.main`
//! (`ClusteredServiceContainer.java:97-112`).
//!
//! # Arguments
//!
//! The reference's arguments are **properties files** (loaded by
//! `SystemUtil.loadPropertiesFiles`). `-Dname=value` is accepted here as well,
//! because this build's tests have no JVM to put one on; it is applied first,
//! so a file overrides it — the same order `PropertyAction.REPLACE` produces
//! when the JVM's `-D` values exist before `main` runs.
//!
//! # The business
//!
//! The reference loads a Java class named by
//! `aeron.cluster.service.class.name`; this build has no class loader, so it
//! selects from a compiled-in registry (`services::by_name`) by
//! `deepmsg.cluster.service`. A name that is absent, or that the registry does
//! not know, is a **refused start** that lists the names it does know — never a
//! silent default. `aeron.cluster.service.class.name` is refused by name, in
//! `ServiceContainerConfig::from_properties`.
//!
//! # Termination
//!
//! The agent ends in one of two ways and they exit differently:
//!
//! * `ABORT`, `SHUTDOWN`, or a closed client — `do_work` returns
//!   [`ContainerError::AgentTermination`](deepmsg_cluster_service::ContainerError::AgentTermination);
//!   the agent is closed and the process exits **0**. That is the reference's
//!   `ClusterTerminationException`, which `AgentRunner` treats as a normal stop.
//! * Any other error — the process prints it, closes what it can, and exits
//!   **1**.
//!
//! `SIGINT`/`SIGTERM` stop the duty cycle and close the agent cleanly, exit 0.
//! The handler is installed **after** `launch`, which is where the agent joins
//! the log; a signal swallowed there would leave a process that ignores a kill.
//!
//! # The media driver and the archive are somebody else's
//!
//! This process connects to a driver's `aeron.dir` and talks to the consensus
//! module over stream 104/105 and to the archive over its local control
//! channel. Both are separate processes (`deepmsg-driver`,
//! `deepmsg-archiving-media-driver`, or the reference's own).

use std::path::Path;
use std::process::ExitCode;

use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_service::services;
use deepmsg_cluster_service::{ClusteredServiceAgent, ContainerError, ServiceContainerConfig};
use deepmsg_driver::idle::Strategy;
use deepmsg_driver::sys;

const NAME: &str = "deepmsg-clustered-service-container";

const USAGE: &str = "\
Usage: deepmsg-clustered-service-container [options] [properties file ...]

  -Dname=value   set one cluster or driver property (a file overrides it)
  -h, --help     print this message

The business is chosen by `deepmsg.cluster.service` from the compiled-in
registry (echo, basic-auction, noop); `aeron.cluster.service.class.name` is
refused. The media driver and the archive are separate processes in the same
`-Daeron.dir` (`deepmsg-driver`, `deepmsg-archiving-media-driver`).
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

    let config = ServiceContainerConfig::from_properties(&properties).map_err(|error| {
        format!("the configuration is refused: {error} (see `aeron.cluster.*` in docs/compat.md)")
    })?;

    let service_name = config.service.as_deref().ok_or_else(|| {
        format!(
            "deepmsg.cluster.service is not set; it must name one of: {}",
            services::NAMES.join(", ")
        )
    })?;
    let service = services::by_name(service_name).ok_or_else(|| {
        format!(
            "deepmsg.cluster.service={service_name} names no business this build has; it must be \
             one of: {}",
            services::NAMES.join(", ")
        )
    })?;

    // The reference's thread name (`:926-930`, `:942`): the agent's role name,
    // on the thread the agent runs on. The kernel keeps fifteen bytes, which is
    // what `set_current_thread_name` leaves; P3-8 owns the naming-mode table and
    // the tests that read the whole name back.
    sys::set_current_thread_name(&config.service_name)
        .map_err(|error| format!("the thread cannot be named: {error}"))?;

    let mut idle = Strategy::by_agrona_name(&config.idle_strategy).ok_or_else(|| {
        format!(
            "aeron.cluster.idle.strategy={} names no strategy this build has",
            config.idle_strategy
        )
    })?;

    let mut agent = ClusteredServiceAgent::launch(config, service).map_err(|e| e.to_string())?;

    if let Err(error) = sys::install_stop_handler() {
        let _ = agent.close();
        return Err(format!("could not install the signal handler: {error}"));
    }

    // The line a harness (or an operator) can wait for. The mark file is
    // signalled by `launch` **before** the agent has recovered, so it says
    // "concluded", not "running"; this says the duty cycle has begun and a
    // SIGTERM will be handled rather than killing the process.
    println!("STARTED serviceId={}", agent.service_id());

    while sys::stop_signal().is_none() {
        match agent.do_work() {
            Ok(work) => idle.idle(work),
            // `ClusterTerminationException` in the reference: an orderly end,
            // including the one `ABORT`/`SHUTDOWN` ask for.
            Err(error) if is_termination(&error) => {
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

/// `ClusterError::is_termination`'s counterpart on the container's error.
fn is_termination(error: &ContainerError) -> bool {
    matches!(error, ContainerError::AgentTermination(_))
}
