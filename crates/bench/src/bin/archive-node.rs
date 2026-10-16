//! The far end of an archive measurement, as a program: the reference's
//! `io.aeron.benchmarks.aeron.ArchiveNode`.
//!
//! Started the way the reference's own scripts start it — a process in the
//! background, one per run, killed when the run is over — and given its arguments
//! the same way: `-Dname=value` for what a JVM would call a system property, and
//! properties file paths for the rest.
//!
//! Unlike the reference's node it does not wait for a signal: it returns when the
//! client's publication goes away, which is the same moment a finished run leaves
//! it at. See `echo-node`'s note for why that is the whole of it.
//!
//! # Two spellings of "where the archive is"
//!
//! The archive is reached through the reference's own **client** property names
//! (`aeron.archive.control.channel`, `aeron.archive.control.response.channel`,
//! `AeronArchive.java:2698`, `:2752`), so that a cell describes its archive the
//! same way whichever archive that cell ran — and they are mapped here onto this
//! build's [`ArchiveContext`], which is configured through `AERON_ARCHIVE_*`
//! environment names instead. The mapping is this file's, deliberately: the *cell*
//! is the thing that has to look the same on both sides, and the client's own
//! spelling is an implementation detail of one of the two.

use std::path::Path;
use std::process::ExitCode;

use deepmsg_bench::loadtest::config::{IdleStrategy, Properties};
use deepmsg_bench::loadtest::transport::archive_node::ArchiveNode;
use deepmsg_bench::loadtest::transport::util::{ChannelSettings, archive_context, property};

/// What this program accepts.
const USAGE: &str = "\
usage: archive-node [-Dname=value ...] [file.properties ...]

  -Dname=value     a setting, which wins over any properties file given here
  file.properties  settings from a file, later files winning over earlier ones

  Reads the same channel settings the rig does — destination, record — plus the
  archive's own control channel and response channel, under the reference's
  client's names (aeron.archive.control.channel, .control.response.channel).
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("archive-node: {message}");
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

    let idle = match command_line.get(property::IDLE_STRATEGY) {
        Some(text) => IdleStrategy::parse(text).map_err(|error| error.to_string())?,
        None => IdleStrategy::default(),
    };

    let context = archive_context(&command_line, &settings.directory);

    let mut node = ArchiveNode::new(settings, idle, &context).map_err(|error| error.to_string())?;

    node.run().map_err(|error| error.to_string())?;
    node.close();

    Ok(())
}
