//! The `deepmsg-cluster-tool` binary: arguments in, `ClusterTool` out.
//!
//! Everything the binary does is in [`deepmsg_cluster_tool::command::run`]; this
//! is the process shell around it, and the one place the reference's `-1` exit
//! status becomes an operating-system status. `System.exit(-1)` leaves `255` in
//! the process status, so `-1` becomes `255` here rather than `1` — a script
//! that checks `$?` sees the same number it sees with the reference tool.

use std::io::Write;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    let mut out = stdout.lock();
    let mut err = stderr.lock();

    let status = deepmsg_cluster_tool::command::run(&args, &mut out, &mut err);
    let _ = out.flush();
    let _ = err.flush();

    if status == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(255)
    }
}
