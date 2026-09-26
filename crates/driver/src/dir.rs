//! The aeron directory's lifecycle: what has to be true before a CnC file
//! exists, and what happens to the directory afterwards.
//!
//! The reference's rule is one question asked in one place
//! (`aeron-driver/src/main/c/aeron_driver.c:136-235`): **is somebody else
//! living here?** If the directory exists it maps whatever `cnc.dat` is in it
//! and reads the to-driver ring's consumer heartbeat — the same liveness field
//! a client reads, with the same timeout (`aeron_driver_context.c:1595-1643`).
//! A heartbeat inside `aeron.driver.timeout` (10 s by default, `:217`) means a
//! live driver and the answer is `EBUSY`; anything else — no CnC file, a
//! version this build cannot read, a heartbeat that stopped — means a dead
//! one, and the directory is deleted rather than reused.
//!
//! There is no lock file anywhere in that discipline, and that is the point:
//! the liveness signal is a field the driver already maintains, so a driver
//! that was killed with `SIGKILL` leaves a directory that the next driver
//! reclaims without an operator having to clear anything.
//!
//! # What is not here yet
//!
//! The reference rescues the *old* driver's error log before deleting the
//! directory, and prints a warning when it has
//! (`aeron_driver.c:83-132`, called at `:198`). That belongs with the error
//! log itself, and arrives with it in P1-3; until then a stale directory's
//! error log is deleted with everything else.

use std::io;
use std::path::{Path, PathBuf};

use deepmsg_cnc::{CncFile, CncOpenError};

use crate::config::DriverConfig;

/// Subdirectory for IPC publication log buffers
/// (`aeron-client/src/main/c/util/aeron_fileutil.h:101`).
pub const PUBLICATIONS_DIR: &str = "publications";

/// Subdirectory for image log buffers (`aeron_fileutil.h:102`).
pub const IMAGES_DIR: &str = "images";

/// Why the directory could not be prepared.
#[derive(Debug)]
pub enum DirError {
    /// Another driver is alive in this directory, by the only test there is:
    /// its heartbeat is recent.
    Busy {
        /// The directory that is in use.
        path: PathBuf,
        /// The heartbeat it was judged by, epoch milliseconds.
        heartbeat_ms: i64,
        /// The window it was judged against.
        timeout_ms: i64,
    },
    /// The directory could not be inspected, removed, or created.
    Io {
        /// The path being worked on when it failed.
        path: PathBuf,
        /// Why it failed.
        source: io::Error,
    },
}

impl std::fmt::Display for DirError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy {
                path,
                heartbeat_ms,
                timeout_ms,
            } => write!(
                f,
                "an active media driver holds {}: its heartbeat was {heartbeat_ms} ms, \
                 within the {timeout_ms} ms window",
                path.display()
            ),
            Self::Io { path, source } => {
                write!(f, "{}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for DirError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Busy { .. } => None,
        }
    }
}

/// Make `aeron_dir` ready for a new CnC file.
///
/// Returns the lines the reference would have logged while doing it, in order,
/// for the caller to print. `now_ms` is passed in rather than read so that a
/// test can age a heartbeat without waiting for one.
///
/// # Errors
///
/// [`DirError::Busy`] if a live driver owns the directory, or
/// [`DirError::Io`] if it could not be inspected, removed or created.
pub fn prepare(config: &DriverConfig, now_ms: i64) -> Result<Vec<String>, DirError> {
    let dir = config.aeron_dir.as_path();
    let mut messages = Vec::new();

    if dir.is_dir() {
        if config.warn_if_dirs_exist {
            messages.push(format!("WARNING: {} exists", dir.display()));
        }

        if config.dirs_delete_on_start {
            remove_dir_all(dir)?;
        } else {
            check_for_a_live_driver(config, now_ms)?;
            remove_dir_all(dir)?;
        }
    }

    create_dir(dir)?;
    create_dir(&dir.join(PUBLICATIONS_DIR))?;
    create_dir(&dir.join(IMAGES_DIR))?;

    Ok(messages)
}

/// Delete the directory if the configuration asks for it.
///
/// # Errors
///
/// [`DirError::Io`] if the directory is there and cannot be removed.
pub fn remove(config: &DriverConfig) -> Result<(), DirError> {
    if !config.dirs_delete_on_shutdown {
        return Ok(());
    }

    let dir = config.aeron_dir.as_path();
    if dir.is_dir() {
        remove_dir_all(dir)?;
    }

    Ok(())
}

/// Refuse the directory if a driver in it looks alive.
///
/// Every other outcome — a directory with no CnC file, a file whose version
/// this build cannot read, a heartbeat that went stale or was set to
/// [`deepmsg_cnc::layout::NULL_VALUE`] on the way out — is a dead driver, and
/// the caller deletes it.
fn check_for_a_live_driver(config: &DriverConfig, now_ms: i64) -> Result<(), DirError> {
    let dir = config.aeron_dir.as_path();

    match CncFile::try_open(dir) {
        Ok(cnc) => {
            let heartbeat_ms = cnc
                .consumer_heartbeat_ms()
                .unwrap_or(deepmsg_cnc::layout::NULL_VALUE);

            if cnc.driver_is_active(now_ms, config.driver_timeout_ms) {
                return Err(DirError::Busy {
                    path: dir.to_owned(),
                    heartbeat_ms,
                    timeout_ms: config.driver_timeout_ms,
                });
            }

            Ok(())
        }
        // Nothing to read: an empty directory, or one whose CnC file was never
        // created. Both mean nobody is home.
        Err(CncOpenError::Io(error)) if io::ErrorKind::NotFound == error.kind() => Ok(()),
        // A file that exists and is not a CnC file this build can use: too
        // short to hold a region, version still zero, a version from another
        // implementation, or metadata that does not describe a layout. The
        // reference reaches the same conclusion by reading the version first
        // and finding it unusable (`aeron_driver_context.c:1620-1624`).
        Err(
            CncOpenError::TooShort { .. }
            | CncOpenError::NotReady
            | CncOpenError::Incompatible(_)
            | CncOpenError::Malformed(_),
        ) => Ok(()),
        // The file is there and could not be read, which is not a statement
        // about who owns the directory.
        Err(CncOpenError::Io(source)) => Err(DirError::Io {
            path: dir.to_owned(),
            source,
        }),
    }
}

fn create_dir(path: &Path) -> Result<(), DirError> {
    std::fs::create_dir_all(path).map_err(|source| DirError::Io {
        path: path.to_owned(),
        source,
    })
}

fn remove_dir_all(path: &Path) -> Result<(), DirError> {
    std::fs::remove_dir_all(path).map_err(|source| DirError::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
    use deepmsg_cnc::layout;
    use deepmsg_cnc::{CncIdentity, CncLayout, ToDriverRingConsumer};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A directory of our own in the system temp directory, removed on drop.
    ///
    /// Hand-rolled, like its counterparts under `deepmsg-core` and
    /// `deepmsg-cnc`: the workspace has no test dependencies. It is the third
    /// copy, and the reason there are three is that a shared test-support
    /// crate would have to be a dependency of the crates it tests.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("deepmsg-dir-{}-{n}", std::process::id()));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const NOW_MS: i64 = 1_700_000_000_000;
    const SMALL_TIMEOUT_MS: i64 = 10_000;

    fn config(dir: &Path) -> DriverConfig {
        DriverConfig {
            aeron_dir: dir.to_owned(),
            driver_timeout_ms: SMALL_TIMEOUT_MS,
            ..DriverConfig::default()
        }
    }

    /// The smallest layout the driver can be configured with, so that a test
    /// which only cares about liveness does not allocate 46 MB.
    fn layout() -> CncLayout {
        CncLayout {
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
            ..CncLayout::default()
        }
    }

    /// A CnC file in `dir`, with the consumer heartbeat set to `heartbeat_ms`
    /// — what a driver that ran and stopped, or is still running, leaves
    /// behind.
    fn cnc_with_heartbeat(dir: &Path, heartbeat_ms: i64) {
        let cnc = CncFile::create(
            dir,
            &layout(),
            &CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: NOW_MS,
                pid: 4242,
            },
        )
        .expect("create the CnC file");

        let region = cnc.to_driver_region().expect("created read-write");
        let consumer =
            ToDriverRingConsumer::new(&region.as_read_only()).expect("a valid to-driver ring");
        consumer
            .write_consumer_heartbeat(&region, heartbeat_ms)
            .expect("write the heartbeat");
    }

    #[test]
    fn creates_the_directory_and_the_two_subdirectories() {
        let temp = TempDir::new();
        let config = config(temp.path());

        let messages = prepare(&config, NOW_MS).expect("prepare");

        assert!(messages.is_empty());
        assert!(temp.path().is_dir());
        assert!(temp.path().join(PUBLICATIONS_DIR).is_dir());
        assert!(temp.path().join(IMAGES_DIR).is_dir());
    }

    #[test]
    fn a_live_driver_owns_the_directory() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        cnc_with_heartbeat(temp.path(), NOW_MS - 100);
        let config = config(temp.path());

        let error = prepare(&config, NOW_MS).expect_err("the heartbeat is 100 ms old");

        match error {
            DirError::Busy {
                heartbeat_ms,
                timeout_ms,
                ..
            } => {
                assert_eq!(NOW_MS - 100, heartbeat_ms);
                assert_eq!(SMALL_TIMEOUT_MS, timeout_ms);
            }
            other => panic!("expected Busy, got {other:?}"),
        }
        assert!(
            temp.path().join(deepmsg_cnc::CNC_FILE_NAME).exists(),
            "and the live driver's file is left alone"
        );
    }

    #[test]
    fn a_heartbeat_older_than_the_window_is_a_dead_driver() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        cnc_with_heartbeat(temp.path(), NOW_MS - SMALL_TIMEOUT_MS - 1);
        let config = config(temp.path());

        prepare(&config, NOW_MS).expect("a stale heartbeat is reclaimable");

        assert!(!temp.path().join(deepmsg_cnc::CNC_FILE_NAME).exists());
        assert!(temp.path().join(PUBLICATIONS_DIR).is_dir(), "and rebuilt");
    }

    #[test]
    fn a_driver_that_stopped_on_purpose_is_a_dead_driver() {
        // NULL_VALUE is what a clean shutdown writes
        // (`aeron_driver_conductor.c:3493`), and it is a different answer from
        // a timestamp, not an older one.
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        cnc_with_heartbeat(temp.path(), layout::NULL_VALUE);
        let config = config(temp.path());

        prepare(&config, NOW_MS).expect("a stopped driver is reclaimable");

        assert!(!temp.path().join(deepmsg_cnc::CNC_FILE_NAME).exists());
    }

    #[test]
    fn a_directory_with_no_cnc_file_is_reclaimable() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        std::fs::write(temp.path().join("something-else"), b"not ours").expect("write");
        let config = config(temp.path());

        prepare(&config, NOW_MS).expect("nothing to check");

        assert!(
            !temp.path().join("something-else").exists(),
            "deleted whole"
        );
        assert!(temp.path().join(PUBLICATIONS_DIR).is_dir());
    }

    #[test]
    fn a_cnc_file_from_another_implementation_is_reclaimable() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");

        // 256 bytes with a major version of 1: long enough to hold a metadata
        // region, and a file this build will not read.
        let mut block = vec![0u8; 256];
        block[0..4].copy_from_slice(&0x0001_0000i32.to_le_bytes());
        std::fs::write(temp.path().join(deepmsg_cnc::CNC_FILE_NAME), &block).expect("write");
        let config = config(temp.path());

        prepare(&config, NOW_MS).expect("a foreign CnC file is not a live driver");

        assert!(temp.path().join(PUBLICATIONS_DIR).is_dir(), "rebuilt");
    }

    #[test]
    fn delete_on_start_wipes_even_a_live_driver() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        cnc_with_heartbeat(temp.path(), NOW_MS);
        let config = DriverConfig {
            dirs_delete_on_start: true,
            ..config(temp.path())
        };

        prepare(&config, NOW_MS).expect("delete.on.start does not ask");

        assert!(!temp.path().join(deepmsg_cnc::CNC_FILE_NAME).exists());
    }

    #[test]
    fn warn_if_dirs_exist_reports_the_directory() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        let config = DriverConfig {
            warn_if_dirs_exist: true,
            ..config(temp.path())
        };

        let messages = prepare(&config, NOW_MS).expect("prepare");

        assert_eq!(1, messages.len());
        assert!(messages[0].contains("WARNING"));
        assert!(messages[0].contains(&temp.path().display().to_string()));
    }

    #[test]
    fn warns_before_deleting_not_after() {
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        cnc_with_heartbeat(temp.path(), layout::NULL_VALUE);
        let config = DriverConfig {
            warn_if_dirs_exist: true,
            ..config(temp.path())
        };

        let messages = prepare(&config, NOW_MS).expect("prepare");

        assert_eq!(1, messages.len(), "the directory existed when it was asked");
    }

    #[test]
    fn removal_only_happens_when_it_is_configured() {
        let temp = TempDir::new();
        prepare(&config(temp.path()), NOW_MS).expect("prepare");

        remove(&config(temp.path())).expect("remove");
        assert!(temp.path().is_dir(), "delete.on.shutdown is off by default");

        let config = DriverConfig {
            dirs_delete_on_shutdown: true,
            ..config(temp.path())
        };
        remove(&config).expect("remove");
        assert!(!temp.path().exists());
    }

    #[test]
    fn removal_of_an_absent_directory_is_not_an_error() {
        let temp = TempDir::new();
        let config = DriverConfig {
            dirs_delete_on_shutdown: true,
            ..config(temp.path())
        };

        remove(&config).expect("nothing to remove is nothing to fail");
    }
}
