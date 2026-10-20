//! The command table and the argument parsing, the counterpart of
//! `ClusterTool.main` (`ClusterTool.java:239-270`) and its static `COMMANDS`
//! map (`:128-227`).
//!
//! The reference's shape is positional: **the cluster directory first, the
//! command second**, then the command's own arguments. Settings arrive as JVM
//! system properties there; here they are `-Dname=value` arguments, the way
//! every other binary in this workspace takes one. An argument order that puts
//! `-D` anywhere is accepted, because Rust has no JVM to put it on.

use std::io::Write;
use std::path::Path;

use deepmsg_cluster_common::config::Properties;

use crate::operator::{ClusterTool, FAILURE};
use crate::properties::ToolProperties;

/// `HELP_PREFIX` (`ClusterTool.java:124`).
pub const HELP_PREFIX: &str = "Usage: <cluster-dir> <command> [options]";

/// One row of the reference's command map, in the order the help prints it.
pub struct CommandSpec {
    /// The command word.
    pub name: &'static str,
    /// The reference's description (`ClusterTool.java:135-226`).
    pub description: &'static str,
}

/// All eighteen commands `ClusterTool` registers. `standby-snapshot` is in the
/// class javadoc but is **not** in the map — it is premium tooling
/// (`ClusterTool.java:128-227`), and an unknown command is refused here too.
pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "describe",
        description: "prints out all descriptors in the mark file.",
    },
    CommandSpec {
        name: "pid",
        description: "prints PID of cluster component.",
    },
    CommandSpec {
        name: "recovery-plan",
        description: "[service count] prints recovery plan of cluster component.",
    },
    CommandSpec {
        name: "recording-log",
        description: "prints recording log of cluster component.",
    },
    CommandSpec {
        name: "sort-recording-log",
        description: "reorders entries in the recording log to match the order in memory.",
    },
    CommandSpec {
        name: "seed-recording-log-from-snapshot",
        description: "creates a new recording log based on the latest valid snapshot.",
    },
    CommandSpec {
        name: "errors",
        description: "prints Aeron and cluster component error logs.",
    },
    CommandSpec {
        name: "list-members",
        description: "prints leader memberId and active members.",
    },
    CommandSpec {
        name: "backup-query",
        description: "[delay] get, or set, time of next backup query.",
    },
    CommandSpec {
        name: "invalidate-latest-snapshot",
        description: "marks the latest snapshot as a invalid so the previous is loaded.",
    },
    CommandSpec {
        name: "snapshot",
        description: "triggers a snapshot on the leader.",
    },
    CommandSpec {
        name: "suspend",
        description: "suspends appending to the log.",
    },
    CommandSpec {
        name: "resume",
        description: "resumes reading from the log.",
    },
    CommandSpec {
        name: "shutdown",
        description: "initiates an orderly stop of the cluster with a snapshot.",
    },
    CommandSpec {
        name: "abort",
        description: "stops the cluster without a snapshot.",
    },
    CommandSpec {
        name: "describe-latest-cm-snapshot",
        description: "prints the contents of the latest valid consensus module snapshot.",
    },
    CommandSpec {
        name: "is-leader",
        description: "returns zero if the cluster node is leader, non-zero if not.",
    },
    CommandSpec {
        name: "validate-recording-log",
        description: "triggers the cluster node to validate its recording log against the archive.",
    },
];

/// `ClusterToolCommand.printHelp` (`ClusterToolCommand.java:67-81`): the prefix,
/// then every command name padded to the widest one.
#[must_use]
pub fn usage() -> String {
    let width = COMMANDS
        .iter()
        .map(|command| command.name.len())
        .max()
        .unwrap_or(0);
    let mut text = format!("{HELP_PREFIX}\n");
    for command in COMMANDS {
        // The reference right-aligns the names (`ClusterToolCommand.java:70-78`).
        let padding = " ".repeat(width - command.name.len());
        text.push_str(&format!(
            "{padding}{}: {}\n",
            command.name, command.description
        ));
    }
    text
}

/// Run one command line, the way `ClusterTool.main` does.
///
/// Returns the reference's exit status: `0`, or `-1` (which the binary maps to
/// the process status `255`, what `System.exit(-1)` produces).
pub fn run(args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let mut properties = Properties::new();
    let mut positional = Vec::new();
    for arg in args {
        if arg.starts_with("-D") {
            if let Err(error) = properties.apply_args([arg.clone()]) {
                let _ = writeln!(err, "{error}");
                return FAILURE;
            }
        } else {
            positional.push(arg.clone());
        }
    }

    if positional.len() < 2 {
        let _ = write!(out, "{}", usage());
        return FAILURE;
    }

    let cluster_dir = Path::new(&positional[0]);
    if !cluster_dir.is_dir() {
        let absolute =
            std::path::absolute(cluster_dir).unwrap_or_else(|_| cluster_dir.to_path_buf());
        let _ = writeln!(
            err,
            "ERR: cluster directory not found: {}",
            absolute.display()
        );
        let _ = write!(out, "{}", usage());
        return FAILURE;
    }

    let command = positional[1].as_str();
    if !COMMANDS.iter().any(|spec| spec.name == command) {
        let _ = writeln!(out, "Unknown command: {command}");
        let _ = write!(out, "{}", usage());
        return FAILURE;
    }

    let tool_properties = match ToolProperties::from_properties(&properties) {
        Ok(properties) => properties,
        Err(message) => {
            let _ = writeln!(err, "{message}");
            return FAILURE;
        }
    };
    let mut tool = ClusterTool::new(tool_properties);
    if let Some(aeron_dir) = properties.get("aeron.dir") {
        tool = tool.with_aeron_dir(aeron_dir);
    }

    let remaining = &positional[2..];
    match command {
        "describe" => tool.describe(cluster_dir, out),
        "pid" => tool.pid(cluster_dir, out),
        "recovery-plan" => {
            let Some(count) = remaining.first() else {
                let _ = write!(out, "{}", usage());
                return FAILURE;
            };
            match count.parse::<i32>() {
                Ok(service_count) => tool.recovery_plan(cluster_dir, out, service_count),
                Err(_) => {
                    let _ = writeln!(err, "recovery-plan: [{count}] is not a service count");
                    FAILURE
                }
            }
        }
        "recording-log" => tool.recording_log(cluster_dir, out),
        "sort-recording-log" => tool.sort_recording_log(cluster_dir, out),
        "seed-recording-log-from-snapshot" => {
            tool.seed_recording_log_from_snapshot(cluster_dir, out)
        }
        "errors" => tool.errors(cluster_dir, out),
        "list-members" => tool.list_members(cluster_dir, out),
        "backup-query" => {
            let delay_ms = match remaining.first() {
                None => None,
                Some(text) => match deepmsg_cluster_common::config::duration_ns(text) {
                    Ok(nanos) => Some(nanos / 1_000_000),
                    Err(_) => return FAILURE,
                },
            };
            tool.backup_query(cluster_dir, out, delay_ms)
        }
        "invalidate-latest-snapshot" => tool.invalidate_latest_snapshot(cluster_dir, out),
        "is-leader" => tool.is_leader(cluster_dir, out),
        "snapshot" | "suspend" | "resume" | "shutdown" | "abort" | "validate-recording-log" => {
            tool.toggle(cluster_dir, out, command)
        }
        "describe-latest-cm-snapshot" => tool.describe_latest_cm_snapshot(cluster_dir, out),
        _ => unreachable!("the command was checked against the table"),
    }
}
