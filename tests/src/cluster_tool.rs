//! This build's `deepmsg-cluster-tool`, run as a process against a cluster
//! directory — the counterpart of [`crate::java::cluster_tool`].
//!
//! `ClusterTool` is the reference's operator surface for a cluster directory
//! (`ClusterTool.java:239-270`: the directory first, the command second), and
//! LC-5's fourth criterion is that **each implementation reads the directory the
//! other one left behind**. The reference's side of that is
//! `java::cluster_tool`; this is ours, with the same argument order.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::driver;

/// Environment variable naming this build's `deepmsg-cluster-tool`.
pub const CLUSTER_TOOL_ENV: &str = "DEEPMSG_CLUSTER_TOOL_BIN";

/// Where the tool is expected to be when the variable is unset, relative to
/// this crate's manifest directory.
pub const DEFAULT_CLUSTER_TOOL: &str = "../target/debug/deepmsg-cluster-tool";

/// The tool's path, if the workspace has been built.
#[must_use]
pub fn locate_cluster_tool() -> Option<PathBuf> {
    driver::locate_tool(CLUSTER_TOOL_ENV, DEFAULT_CLUSTER_TOOL)
}

/// Run `command` on `cluster_dir` and answer what the tool printed, **without
/// judging the exit status** — for a failure message, which must report the
/// case's own failure rather than die on the instrument it reached for.
#[must_use]
pub fn try_cluster_tool(cluster_dir: &Path, command: &[&str]) -> String {
    let Some(binary) = locate_cluster_tool() else {
        return format!("(no {DEFAULT_CLUSTER_TOOL})");
    };
    match Command::new(&binary)
        .arg(cluster_dir)
        .args(command)
        .output()
    {
        Ok(output) => format!(
            "exit {}:\n{}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(error) => format!("{} did not run: {error}", binary.display()),
    }
}

/// Run `command` on `cluster_dir` and answer what the tool printed.
///
/// # Panics
///
/// When the tool is not built, cannot be run, or exits non-zero — a non-zero
/// exit is the tool saying the **directory** is unreadable, which is exactly
/// what this arm is judging.
#[must_use]
pub fn cluster_tool(cluster_dir: &Path, command: &[&str]) -> String {
    let binary = locate_cluster_tool().unwrap_or_else(|| {
        panic!(
            "no {DEFAULT_CLUSTER_TOOL}; build it with `cargo build --workspace --bins` or set \
             {CLUSTER_TOOL_ENV}"
        )
    });
    let output = Command::new(&binary)
        .arg(cluster_dir)
        .args(command)
        .output()
        .unwrap_or_else(|error| panic!("{} did not run: {error}", binary.display()));
    assert!(
        output.status.success(),
        "{} {} exited with {}:\nstdout:\n{}\nstderr:\n{}",
        binary.display(),
        command.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}
