//! The far end of an echo measurement, as a program: the reference's
//! `io.aeron.benchmarks.aeron.EchoNode`.
//!
//! Started the way the reference's own scripts start it — as a process in the
//! background, one per receiver, killed when the run is over — and given its
//! arguments the same way: `-Dname=value` for what a JVM would call a system
//! property, and properties file paths for the rest. The files are read in
//! order, and the `-D` arguments win over them.
//!
//! Unlike the reference's node it does not wait for a signal: it returns when
//! the client's publication goes away, which is the same moment a run that has
//! finished leaves it at. A node started for a run is also killed between runs
//! by whatever started it, and this is what makes that unnecessary.

use std::path::Path;
use std::process::ExitCode;

use deepmsg_bench::loadtest::config::{IdleStrategy, Properties};
use deepmsg_bench::loadtest::transport::node::EchoNode;
use deepmsg_bench::loadtest::transport::util::{ChannelSettings, property};

/// What this program accepts.
const USAGE: &str = "\
usage: echo-node [-Dname=value ...] [file.properties ...]

  -Dname=value     a setting, which wins over any properties file given here
  file.properties  settings from a file, later files winning over earlier ones

  The channels, the streams and the receiver index are the same settings the rig
  reads, and the node answers on the mirror of them.
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("echo-node: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut command_line = Properties::new();
    let mut from_files = Properties::new();

    for argument in std::env::args().skip(1) {
        match argument.as_str() {
            "--help" | "-h" => {
                print!("{USAGE}");
                return Ok(());
            }
            _ if argument.starts_with("-D") => {
                let (name, value) = argument[2..]
                    .split_once('=')
                    .ok_or_else(|| format!("-D wants name=value, and '{argument}' has no '='"))?;
                command_line.set(name, value);
            }
            _ if argument.starts_with('-') => {
                return Err(format!("unknown option '{argument}'\n\n{USAGE}"));
            }
            _ => from_files
                .load_file(Path::new(&argument))
                .map_err(|error| error.to_string())?,
        }
    }

    command_line.merge_keeping_existing(&from_files);

    let settings =
        ChannelSettings::from_properties(&command_line).map_err(|error| error.to_string())?;

    // The node's own idle strategy is `io.aeron.benchmarks.aeron.idle.strategy`,
    // which is the reference's separate setting for the far end — the rig's
    // `io.aeron.benchmarks.idle.strategy` is the client's.
    let idle = match command_line.get(property::IDLE_STRATEGY) {
        Some(text) => IdleStrategy::parse(text).map_err(|error| error.to_string())?,
        None => IdleStrategy::default(),
    };
    let receiver_index = settings.receiver_index;

    let mut node =
        EchoNode::new(settings, idle, receiver_index).map_err(|error| error.to_string())?;

    node.run().map_err(|error| error.to_string())?;
    node.close();

    Ok(())
}
