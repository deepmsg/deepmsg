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
use std::time::Duration;

use deepmsg_cnc::{CncFile, CncOpenError};
use deepmsg_core::version::CncVersionCompatibility;

use crate::config::DriverConfig;

/// How long to wait between looks while another driver creates its CnC file.
///
/// The reference sleeps a millisecond in the same loop (`aeron_micro_sleep(1000)`,
/// `aeron-driver/src/main/c/aeron_driver_context.c:1610`).
pub const POLL_INTERVAL: Duration = Duration::from_millis(1);

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
    /// A driver owns the directory and publishes a CnC version this build may
    /// not read, so its heartbeat cannot be checked and its liveness cannot be
    /// judged. Refusing is the conservative answer — see
    /// [`check_for_a_live_driver`].
    BusyIncompatible {
        /// The directory that is in use.
        path: PathBuf,
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
            Self::BusyIncompatible { path } => write!(
                f,
                "a media driver holds {} and its CnC version is not one this build may open; \
                 refusing to take the directory over",
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
            Self::Busy { .. } | Self::BusyIncompatible { .. } => None,
        }
    }
}

/// A directory this process made ready: created here, or taken over because
/// nobody else was living in it.
///
/// Holding one is the only way to [`PreparedDir::remove`], so a failure arm
/// cannot delete a directory this process never prepared — which is what the
/// binary's error paths used to do, and how a driver that lost an `O_EXCL` race
/// could delete the winner's directory.
#[derive(Debug)]
pub struct PreparedDir {
    path: PathBuf,
    notices: Vec<Notice>,
    delete_on_shutdown: bool,
}

/// Something the caller may want to say out loud.
///
/// Data rather than a sentence: an operator-facing string is the binary's
/// business, and a library that formats warnings is a library that cannot be
/// used by anything which wants to log them differently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Notice {
    /// The directory was already there.
    DirectoryExists {
        /// Which directory.
        path: PathBuf,
    },
}

impl PreparedDir {
    /// The directory this process prepared.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// What happened on the way, in order.
    pub fn notices(&self) -> &[Notice] {
        &self.notices
    }

    /// Delete the directory, if the configuration asks for it on shutdown.
    ///
    /// The reference deletes on shutdown only when configured to
    /// (`aeron.dir.delete.on.shutdown`), and so does this — including on the
    /// failure paths, which is why they call the same method.
    ///
    /// # Errors
    ///
    /// [`DirError::Io`] if the directory is there and cannot be removed.
    pub fn remove(self) -> Result<(), DirError> {
        if !self.delete_on_shutdown || !self.path.is_dir() {
            return Ok(());
        }

        remove_dir_all(&self.path)
    }
}

/// Make `aeron_dir` ready for a new CnC file.
///
/// `now_ms` is passed in rather than read so that a test can age a heartbeat
/// without waiting for one.
///
/// # Errors
///
/// [`DirError::Busy`] if a live driver owns the directory, or
/// [`DirError::Io`] if it could not be inspected, removed or created.
pub fn prepare(config: &DriverConfig, now_ms: i64) -> Result<PreparedDir, DirError> {
    let dir = config.aeron_dir.as_path();
    let mut notices = Vec::new();

    if dir.is_dir() {
        if config.warn_if_dirs_exist {
            notices.push(Notice::DirectoryExists {
                path: dir.to_owned(),
            });
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

    Ok(PreparedDir {
        path: dir.to_owned(),
        notices,
        delete_on_shutdown: config.dirs_delete_on_shutdown,
    })
}

/// Refuse the directory if a driver in it looks alive.
///
/// The question the reference asks is one question, but it is asked in two
/// steps, and this used to ask only the second. A CnC file whose version field
/// is still **zero** is a driver that is creating its file right now: the file
/// is written first and the version is published when the conductor is ready,
/// and for a 46 MB file that window is tens to hundreds of milliseconds wide.
/// Deciding on the first look therefore means a driver starting beside a live
/// one deletes its directory and then serves the file the other one is still
/// writing — so the reference spins until the version appears or its timeout
/// expires (`aeron_is_driver_active_with_cnc`'s `while (0 == cnc_version)`,
/// `aeron-driver/src/main/c/aeron_driver_context.c:1603-1612`).
///
/// The second step is a heartbeat check, and it is only meaningful for a file
/// this build can read. That is why an *incompatible* file is not
/// automatically a dead one: the reference judges liveness by **major** version
/// alone, and this build also applies the Java minor rule — so an older-minor
/// driver with a fresh heartbeat would be refused by the reference and deleted
/// by a naive port. Refusing is the conservative answer, and the review that
/// found this put the principle plainly: when liveness cannot be judged, err
/// towards a live driver, never towards a deleted one.
///
/// Every other outcome — no CnC file, a file that was created and never
/// published, a different major version, a corrupt block, a heartbeat that went
/// stale or was set to [`deepmsg_cnc::layout::NULL_VALUE`] on the way out — is
/// a dead driver, and the caller deletes it.
fn check_for_a_live_driver(config: &DriverConfig, now_ms: i64) -> Result<(), DirError> {
    let dir = config.aeron_dir.as_path();

    // The wait is a *duration*, so it is measured with a monotonic clock — not
    // with the epoch clock the judgement below uses. The reference measures it
    // against `aeron_epoch_clock()` (`aeron_driver_context.c:1605`), which a
    // clock step turns into a different window: a fresh VM correcting its time
    // can step it forward by seconds and make a driver give up on a peer that
    // is still creating its file, or backward and make the wait longer than
    // configured. A duration has no such failure mode, and the judgement —
    // "is this heartbeat inside the driver timeout?" — stays on the epoch
    // clock, because that is the clock the heartbeat is written from.
    let started = std::time::Instant::now();
    let window = Duration::from_millis(u64::try_from(config.driver_timeout_ms).unwrap_or(u64::MAX));

    loop {
        match CncFile::try_open(dir) {
            Ok(cnc) => {
                // One read, one verdict: the heartbeat this reports on is the
                // heartbeat the answer was made from (`CncFile::driver_liveness`).
                let liveness = cnc
                    .driver_liveness(now_ms, config.driver_timeout_ms)
                    .unwrap_or(deepmsg_cnc::Liveness {
                        heartbeat_ms: deepmsg_cnc::layout::NULL_VALUE,
                        active: false,
                    });

                if liveness.active {
                    return Err(DirError::Busy {
                        path: dir.to_owned(),
                        heartbeat_ms: liveness.heartbeat_ms,
                        timeout_ms: config.driver_timeout_ms,
                    });
                }

                return Ok(());
            }
            // Creating, not dead. Look again until the window closes.
            Err(CncOpenError::NotReady) if started.elapsed() < window => {
                std::thread::sleep(POLL_INTERVAL);
            }
            // Same major, older minor: this build cannot read the file, which
            // is not the same thing as the file being unowned.
            Err(CncOpenError::Incompatible(CncVersionCompatibility::InsufficientMinor)) => {
                return Err(DirError::BusyIncompatible {
                    path: dir.to_owned(),
                });
            }
            // Nothing to read: an empty directory, or one whose CnC file was
            // never created. Both mean nobody is home.
            Err(CncOpenError::Io(error)) if io::ErrorKind::NotFound == error.kind() => {
                return Ok(());
            }
            // A file that exists and is not a CnC file this build can use:
            // too short to hold a region, created and never published, a
            // version from a different implementation, or metadata that does
            // not describe a layout.
            Err(
                CncOpenError::TooShort { .. }
                | CncOpenError::NotReady
                | CncOpenError::Incompatible(_)
                | CncOpenError::Malformed(_),
            ) => return Ok(()),
            // The file is there and could not be read, which is not a
            // statement about who owns the directory.
            Err(CncOpenError::Io(source)) => {
                return Err(DirError::Io {
                    path: dir.to_owned(),
                    source,
                });
            }
        }
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
    use std::io::{Seek as _, Write as _};
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
        let mut cnc = CncFile::create(
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

        // The version last, as a driver publishes it: a file with a heartbeat
        // and no version is not one this driver would read as a live peer.
        cnc.publish().expect("publish");
    }

    /// The identity a test's driver starts with, fixed so that a file's
    /// metadata is reproducible.
    fn identity() -> CncIdentity {
        CncIdentity {
            liveness_timeout_ns: 10_000_000_000,
            start_timestamp_ms: NOW_MS,
            pid: 4242,
        }
    }

    /// A driver that is creating its file right now: the CnC file exists, its
    /// heartbeat is written, and its version is not published yet.
    fn cnc_pending_publication(dir: &Path) -> CncFile {
        let cnc = CncFile::create(dir, &layout(), &identity()).expect("create the CnC file");

        let region = cnc.to_driver_region().expect("created read-write");
        let consumer =
            ToDriverRingConsumer::new(&region.as_read_only()).expect("a valid to-driver ring");
        consumer
            .write_consumer_heartbeat(&region, NOW_MS)
            .expect("write the heartbeat");

        cnc
    }

    #[test]
    fn a_driver_that_is_still_publishing_is_not_a_dead_one() {
        // The window the review found: a driver that has created its 46 MB file
        // and written its first heartbeat, but has not published the version
        // yet. Deciding on the first look deletes the directory of a driver
        // that is mid-start, so the question has to wait for the version.
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        let cnc = cnc_pending_publication(temp.path());

        // Published from another thread, as the conductor does once it is
        // ready — after the peer has already looked once.
        // The version is stored through the file system rather than through a
        // `CncFile`: opening one is exactly what the version gate refuses
        // while the version is zero, and that gate is what this test is about.
        // The bytes are the same ones `publish` writes.
        let dir = temp.path().to_owned();
        let publisher = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));

            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(dir.join(deepmsg_cnc::CNC_FILE_NAME))
                .expect("the CnC file");
            file.seek(std::io::SeekFrom::Start(0)).expect("seek");
            file.write_all(&deepmsg_core::version::CNC_VERSION.to_le_bytes())
                .expect("write the version");
            file.sync_all().expect("sync");
        });
        let _creating = cnc;

        let config = config(temp.path());
        let error = prepare(&config, NOW_MS).expect_err("a driver that is still starting");
        publisher.join().expect("the publishing thread");

        assert!(
            matches!(error, DirError::Busy { .. }),
            "the file's driver may be starting, so it may not be deleted: {error:?}"
        );
        assert!(
            temp.path().join("cnc.dat").is_file(),
            "and it is still there"
        );
    }

    #[test]
    fn a_file_that_never_gets_a_version_is_a_dead_directory() {
        // The other end of the same window: nobody published, so after the
        // window the directory is a dead one — which is the reference's
        // conclusion too ("CnC file is created but not initialised",
        // `aeron_driver_context.c:1605-1609`), reached after the same wait.
        let temp = TempDir::new();
        std::fs::create_dir_all(temp.path()).expect("mkdir");
        let _cnc = cnc_pending_publication(temp.path());

        let config = DriverConfig {
            driver_timeout_ms: 40,
            ..config(temp.path())
        };
        let started = std::time::Instant::now();
        prepare(&config, NOW_MS).expect("prepare");

        assert!(
            started.elapsed() >= Duration::from_millis(40),
            "it waits the window out before concluding nobody is coming: {:?}",
            started.elapsed()
        );
        assert!(
            temp.path().join(PUBLICATIONS_DIR).is_dir(),
            "and then takes the directory over"
        );
    }

    #[test]
    fn creates_the_directory_and_the_two_subdirectories() {
        let temp = TempDir::new();
        let config = config(temp.path());

        let prepared = prepare(&config, NOW_MS).expect("prepare");

        assert!(prepared.notices().is_empty());
        assert_eq!(temp.path(), prepared.path());
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

        let prepared = prepare(&config, NOW_MS).expect("prepare");

        assert_eq!(
            [Notice::DirectoryExists {
                path: temp.path().to_owned(),
            }],
            prepared.notices(),
            "the fact, not a sentence about it"
        );
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

        let prepared = prepare(&config, NOW_MS).expect("prepare");

        assert_eq!(
            1,
            prepared.notices().len(),
            "the directory existed when it was asked"
        );
    }

    #[test]
    fn removal_only_happens_when_it_is_configured() {
        let temp = TempDir::new();

        // Off by default: a prepared directory is left where it is.
        prepare(&config(temp.path()), NOW_MS)
            .expect("prepare")
            .remove()
            .expect("remove");
        assert!(temp.path().is_dir(), "delete.on.shutdown is off by default");

        // With it on, the same call removes it.
        let removing = DriverConfig {
            dirs_delete_on_shutdown: true,
            ..config(temp.path())
        };
        prepare(&removing, NOW_MS)
            .expect("prepare")
            .remove()
            .expect("remove");
        assert!(!temp.path().exists());
    }

    #[test]
    fn removal_of_an_absent_directory_is_not_an_error() {
        let temp = TempDir::new();
        let removing = DriverConfig {
            dirs_delete_on_shutdown: true,
            ..config(temp.path())
        };

        // Prepared, then removed by something else, then removed again: the
        // guard's job is to delete what is there, and nothing being there is
        // not a failure.
        let prepared = prepare(&removing, NOW_MS).expect("prepare");
        std::fs::remove_dir_all(temp.path()).expect("someone else got there first");

        prepared
            .remove()
            .expect("nothing to remove is nothing to fail");
    }
}
