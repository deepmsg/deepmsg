//! Running the reference's own Java tools.
//!
//! The archive server is Java-only — the C tree ships a client — so its tools
//! are **classes in `aeron-all-1.53.2.jar`** rather than binaries beside
//! `aeronmd`. `ArchiveTool` is the one P2 leans on: it is the reference's own
//! reader of the two files an archive directory is made of, which makes it the
//! only instrument that can say whether what this build writes is what the
//! reference expects to read.
//!
//! Two things about running it are worth keeping in one place rather than in
//! every test that needs it:
//!
//! * **the classpath is the jar and nothing else**, and the jar carries Agrona
//!   with it — which is why one entry is enough;
//! * **the JVM needs two `--add-*` flags** ([`AGRONA_JVM_ARGS`]) or every Agrona
//!   class dies at class-initialisation with an `IllegalAccessError` about
//!   `jdk.internal.misc.Unsafe`. The reference's own Gradle and CMake builds pass
//!   the same two; a test that forgets them fails in a way that says nothing
//!   about the code under test.
//!
//! [`AGRONA_JVM_ARGS`]: crate::driver::AGRONA_JVM_ARGS

use std::path::Path;
use std::process::Command;

use crate::driver::AGRONA_JVM_ARGS;

/// Run one of the reference's Java classes, and hand back what it printed.
///
/// The caller locates the jar first ([`driver::locate_aeron_all`]) so that a
/// missing reference build is a **skip** rather than a failure.
///
/// # Panics
///
/// When `java` cannot be run, or when the class exits non-zero — which a test
/// that has already decided the reference is there has no business continuing
/// through.
pub fn run(jar: &Path, class: &str, args: &[&str]) -> String {
    let output = Command::new("java")
        .args(AGRONA_JVM_ARGS)
        .arg("-cp")
        .arg(jar)
        .arg(class)
        .args(args)
        .output()
        .expect("java runs");

    assert!(
        output.status.success(),
        "{class} {args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// `ArchiveTool <directory> <command…>`.
///
/// The directory comes first, which is the opposite of what the command name
/// suggests: `ArchiveTool /tmp/archive pid` and not `pid /tmp/archive`.
pub fn archive_tool(jar: &Path, directory: &Path, command: &[&str]) -> String {
    let mut args = vec![directory.to_str().expect("a path")];

    args.extend_from_slice(command);

    run(jar, "io.aeron.archive.ArchiveTool", &args)
}
