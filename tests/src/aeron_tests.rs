//! The C and Java this suite runs, and where they live: **another repository**.
//!
//! Three of the probes below are C programs linked against the reference's own
//! `libaeron`, and six are Java programs compiled against `aeron-all-1.53.2.jar`.
//! Neither belongs in this tree. Their build, their classpath and their lifetime
//! belong to the reference's toolchain rather than to a Cargo workspace, and a
//! `.c` or `.java` file here is a file no gate in this repository compiles — CI
//! would lint it never and break it silently.
//!
//! They live in the fork of the reference, `deepmsg/aeron`, on the branch
//! `deepmsg-tests`, under `deepmsg-tests/` — see that directory's README. The
//! branch is based on the one carrying the two **test-side holds** the C archive
//! suite is run with, so a single checkout of it is the whole of what this
//! suite needs from a *modified* reference.
//!
//! # Two checkouts, and which is which
//!
//! [`driver::locate_aeron_all`](crate::driver::locate_aeron_all) and the C
//! probes' include and library paths point at the **reference** checkout: the
//! one that has been built, whose headers and `libaeron` are what a probe
//! compiles against. This module points at the **fork**: where our test sources
//! are. They are the same upstream twice, and they are deliberately separate —
//! CI clones the pristine one by tag, and a probe that built against a modified
//! tree would be measuring that tree.
//!
//! # A missing checkout is a skip
//!
//! As everywhere else in this crate: `None` means "this workspace does not have
//! it", the caller announces a skip, and the test passes. A machine that has not
//! cloned the fork is not a machine on which the driver is broken.

use std::path::PathBuf;

/// The environment variable naming the `deepmsg-tests` directory.
pub const ENV: &str = "DEEPMSG_AERON_TESTS";

/// Where it is when the variable is unset.
///
/// Relative to `CARGO_MANIFEST_DIR`, which is `tests/` — the same base every
/// other path in this crate uses, and the reason this is two levels up rather
/// than one.
const DEFAULT: &str = "../../aeron-deepmsg/deepmsg-tests";

/// The directory the C and Java live in, or `None` when it is not there.
///
/// `DEEPMSG_AERON_TESTS` wins when it is set: a caller that names a directory
/// gets that directory, or a **file that is missing** rather than a silent fall
/// back to another one — the rule [`locate_tool`](crate::driver::locate_tool)
/// follows, and for the same reason.
#[must_use]
pub fn locate() -> Option<PathBuf> {
    if let Some(overridden) = std::env::var_os(ENV) {
        let path = PathBuf::from(overridden);

        return path.is_dir().then_some(path);
    }

    let candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT);

    candidate.is_dir().then_some(candidate)
}

/// One of the C probes (`probes/<name>`).
#[must_use]
pub fn probe(name: &str) -> Option<PathBuf> {
    located("probes", name)
}

/// One of the Java programs (`java/<name>`).
#[must_use]
pub fn java(name: &str) -> Option<PathBuf> {
    located("java", name)
}

/// One of the fixture generators (`generators/<relative>`).
#[must_use]
pub fn generator(relative: &str) -> Option<PathBuf> {
    let path = locate()?.join("generators").join(relative);

    path.is_file().then_some(path)
}

/// A file under one of the three directories, if it is there.
fn located(directory: &str, name: &str) -> Option<PathBuf> {
    let path = locate()?.join(directory).join(name);

    path.is_file().then_some(path)
}

/// Announce that a test could not run because the fork is not cloned.
pub fn announce_skip() {
    eprintln!(
        "SKIPPED: not verified -- the C and Java this suite runs are not here. Clone \
         https://github.com/deepmsg/aeron into a sibling directory as `aeron-deepmsg` \
         (branch `deepmsg-tests`), or set {ENV} to that checkout's `deepmsg-tests` directory."
    );
}
