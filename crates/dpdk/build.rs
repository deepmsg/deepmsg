//! The build chain ADR-0006 names: `pkg-config libdpdk`, the shim compiled
//! against the *installed* headers, DPDK itself linked as shared libraries.
//!
//! Three things are deliberate.
//!
//! * **No new crate.** `cc` and `ar` are invoked as programs rather than
//!   through a build-dependency, so the workspace's dependency graph is the
//!   same with this crate in it as without (D0's probe built the same way).
//! * **Shared, not static.** A statically linked DPDK drops the PMDs that
//!   register themselves through constructors in plugin objects unless the
//!   linker is told to keep them (`--whole-archive`); the system's shared
//!   libraries do not have that problem.
//! * **Nothing happens with the feature off.** The crate is empty then, and a
//!   machine without `libdpdk-dev` builds the whole workspace, tests included.

use std::env;
use std::path::PathBuf;
use std::process::Command;

/// One `pkg-config libdpdk` query, split into flags.
fn pkg_config(args: &[&str]) -> Vec<String> {
    let output = Command::new("pkg-config")
        .args(args)
        .arg("libdpdk")
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "running `pkg-config {} libdpdk` failed: {error}\n\
                 The `dpdk` feature needs DPDK's development files (`libdpdk-dev`).",
                args.join(" ")
            )
        });
    assert!(
        output.status.success(),
        "`pkg-config {} libdpdk` answered {}: {}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("pkg-config's flags are UTF-8")
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

fn run(command: &mut Command, what: &str) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("running {what}: {error}"));
    assert!(status.success(), "{what} answered {status}");
}

fn main() {
    println!("cargo:rerun-if-changed=shim/shim.c");

    if env::var_os("CARGO_FEATURE_DPDK").is_none() {
        return;
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    let object = out_dir.join("shim.o");
    let archive = out_dir.join("libshim.a");
    let cc = env::var("CC").unwrap_or_else(|_| "cc".to_owned());

    // `--cflags` carries `-march=corei7 -include rte_config.h` on Ubuntu's
    // libdpdk.pc (measured in D0), which is why the flags are taken as given
    // and not assembled here.
    let mut compile = Command::new(cc);
    compile
        .args(pkg_config(&["--cflags"]))
        .args([
            "-O2",
            "-fPIC",
            "-Wall",
            "-Wextra",
            "-c",
            "shim/shim.c",
            "-o",
        ])
        .arg(&object);
    run(&mut compile, "compiling shim/shim.c");

    run(
        Command::new("ar").arg("crs").arg(&archive).arg(&object),
        "archiving the shim",
    );

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=shim");

    for flag in pkg_config(&["--libs"]) {
        if let Some(name) = flag.strip_prefix("-l") {
            println!("cargo:rustc-link-lib=dylib={name}");
        } else if let Some(path) = flag.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={path}");
        } else {
            println!("cargo:rustc-link-arg={flag}");
        }
    }
}
