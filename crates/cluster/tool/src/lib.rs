//! `deepmsg-cluster-tool`: the operator's view of a cluster, the counterpart of
//! the reference's `ClusterTool` (`ClusterTool.java`, 657 lines) and
//! `ClusterToolOperator` (1,426 lines).
//!
//! The tool reads what a cluster leaves behind — the mark files, the recording
//! log, the counters in the media driver's CnC file — and it writes the two
//! operator toggles (counters 202 and 233) on a running cluster. It never
//! adopts a mark file and never touches a cluster's state other than through
//! the toggles the reference defines.
//!
//! # Shape
//!
//! * [`command`] is `ClusterTool.main`: the positional `<cluster-dir>
//!   <command>` table, the help text and the exit status.
//! * [`operator::ClusterTool`] is `ClusterToolOperator`: one method per
//!   subcommand, writing to whatever `Write` it is handed, so a test can read
//!   the output in-process the way the reference's `CapturingPrintStream` does.
//! * [`mark_files`] resolves the `.lnk` indirection and prints mark files and
//!   error logs in the reference's own formats.
//! * [`properties`] holds the four `aeron.cluster.tool.*` settings, and
//!   [`date_time`] formats the epoch stamps the reference formats through the
//!   JVM's default time zone (this build formats UTC; `docs/compat.md` records
//!   it).

#![forbid(unsafe_code)]

pub mod command;
pub mod date_time;
pub mod mark_files;
pub mod operator;
pub mod properties;

pub use command::{COMMANDS, CommandSpec, run, usage};
pub use operator::ClusterTool;
pub use properties::ToolProperties;
