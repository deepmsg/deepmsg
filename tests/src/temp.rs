//! A directory of our own in the system temp directory.
//!
//! The workspace has no test dependencies, so this is hand-rolled — and this
//! copy lives in the shared harness rather than inside one test file because
//! the integration suite is where a test that needs a real directory on disk
//! belongs (the crates' own unit tests carry their own, since a crate's tests
//! cannot depend on this package without a cycle).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

/// A directory under the system temp directory, removed when dropped.
pub struct TempDir(PathBuf);

impl TempDir {
    /// Create a directory whose name begins with `prefix`.
    pub fn new(prefix: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create the temp directory");
        Self(path)
    }

    /// The directory.
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
