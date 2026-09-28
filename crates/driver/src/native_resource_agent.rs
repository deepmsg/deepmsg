//! The thread that owns the file work the conductor must not block on.
//!
//! Creating a log buffer means declaring its whole length — for an IPC
//! publication at the default term length that is 192 MiB — and, when the
//! channel asks for a dense one, allocating and touching every page of it.
//! The conductor is the process's control plane: a conductor that spends tens
//! of milliseconds inside `fallocate` is a conductor that has stopped draining
//! commands, publishing positions and answering clients. The reference puts
//! that work on a **native resource agent** thread and has the conductor poll
//! for the result (`aeron-driver/src/main/c/aeron_driver_native_resource_agent.c:426-466`,
//! polled from `aeron_driver_conductor.c:4042-4067`), which is what this is.
//!
//! # Shape
//!
//! Two channels and one thread. The conductor sends a [`Request`]; the agent
//! does the syscalls; the conductor collects [`Completion`]s on its duty cycle
//! and advances the publication's state machine. Nothing here blocks the
//! conductor: `map_log_buffer` is an `mpsc::send` and `poll` is a
//! `try_recv` loop.
//!
//! A log buffer is created and later *freed* by the same thread — freeing is
//! `munmap` plus `unlink`, both of which can block on a busy filesystem — and
//! the [`LogFile`] is handed *back* to the agent to drop. That is the
//! reference's arrangement too (`free_log_buffer` is an agent command, not an
//! inline call), and it is why the transfer has to be legal: see the `Send`
//! impl on `MappedFile`.
//!
//! # What it does not do
//!
//! The reference's agent also resolves hostnames and runs the asynchronous
//! error-log and counter-reclaim duties. This one maps and frees log buffers,
//! which is what P1-2 needs; the rest arrives with the transport that needs it.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;

use deepmsg_core::logbuffer::logfile::LogFile;

/// What the conductor asks the agent to do.
enum Request {
    MapLogBuffer {
        path: PathBuf,
        term_length: i32,
        page_size: usize,
        is_sparse: bool,
    },
    FreeLogBuffer {
        log: Box<LogFile>,
    },
    Stop,
}

/// The space check that stands in front of every log buffer this agent
/// creates — the reference's `aeron_driver_context_run_storage_checks`
/// (`aeron_driver_context.c:1354-1377`), which its own agent runs at the
/// head of its map command (`aeron_driver_native_resource_agent.c:432`).
///
/// A filesystem that cannot hold the log about to be made answers before
/// the file is touched, so the client hears `STORAGE_SPACE` from a check
/// rather than `GENERIC` from a write that ran out of room halfway.
pub struct StorageChecks {
    /// Whether the check runs at all (`perform.storage.checks`). Off means
    /// the reference's "always plenty" probe (`aeron_usable_fs_space_disabled`,
    /// `aeron_fileutil.c:1203`).
    enabled: bool,
    /// The level at which the check stops refusing and starts warning
    /// (`low.file.store.warning.threshold`, `aeron_driver_context.h:207`).
    warning_threshold: u64,
    /// The filesystem asked: the driver's run directory, which is where a
    /// log buffer lands.
    dir: PathBuf,
}

/// A filesystem that could hold the log about to be made, but only just
/// (`aeron_driver_context.c:1368-1377`).
///
/// The reference records this straight into the distinct error log on its
/// agent thread and carries on — the create proceeds. Here the
/// de-duplication table lives on the conductor, so the warning travels a
/// channel of its own beside the completions and is recorded by the
/// conductor's next duty cycle: same entry, same log, and no counter it did
/// not raise in the reference either.
#[derive(Debug, PartialEq, Eq)]
pub struct StorageWarning {
    /// The configured `low.file.store.warning.threshold` as it stood.
    pub threshold: u64,
    /// What the filesystem said it had left.
    pub usable: u64,
    /// The directory the check asked about, which is the directory the log
    /// buffer lands in.
    pub dir: PathBuf,
}

/// What the space check said: [`io::Error`] refuses, [`StorageWarning`]
/// records, and the create proceeds with either, none, or both absent.
type Assessment = (Option<io::Error>, Option<StorageWarning>);

impl StorageChecks {
    /// Checks as `enabled` says, warning from `warning_threshold`, asking
    /// about `dir`.
    pub const fn new(enabled: bool, warning_threshold: u64, dir: PathBuf) -> Self {
        Self {
            enabled,
            warning_threshold,
            dir,
        }
    }

    /// The refusal and the warning for a log buffer of `term_length` +
    /// `page_size`, in the reference's order: a filesystem that cannot hold
    /// the log refuses and is never asked about the threshold, one that can
    /// hold it but sits at or below the threshold warns, and a check that is
    /// off — or a pair no log buffer may have, which the create itself will
    /// report with the right error for it — says neither.
    fn assess(&self, term_length: i32, page_size: usize) -> Assessment {
        if !self.enabled {
            return (None, None);
        }

        let Some(length) = LogFile::log_length(term_length, page_size) else {
            return (None, None);
        };
        let usable = crate::sys::usable_fs_space(&self.dir);

        // `ENOSPC`, because that is the errno the reference's own composition
        // turns into `STORAGE_SPACE` — its pre-check raises the negative
        // protocol code and its kernel raises the errno, and both arrive at
        // the client as the same code (`aeron_driver_conductor.c:2326-2341`).
        if usable < length as u64 {
            return (Some(io::Error::from_raw_os_error(libc::ENOSPC)), None);
        }

        // The second half (`:1368-1377`): at or below the threshold the
        // reference records a warning and **returns zero**, which is why the
        // warning is not a refusal and the create that follows still happens.
        let warning = (usable <= self.warning_threshold).then_some(StorageWarning {
            threshold: self.warning_threshold,
            usable,
            dir: self.dir.clone(),
        });

        (None, warning)
    }
}

/// What the agent finished.
#[derive(Debug)]
pub enum Completion {
    /// A log buffer is created, mapped and zeroed, and now belongs to the
    /// caller.
    Mapped {
        /// The file that was created.
        path: PathBuf,
        /// The mapping, moved here from the agent thread.
        log: Box<LogFile>,
    },
    /// The mapping could not be made. The file, if the create got that far, is
    /// already removed.
    MapFailed {
        /// The file that was to be created.
        path: PathBuf,
        /// Why not.
        error: io::Error,
    },
    /// A log buffer was unmapped and removed.
    Freed {
        /// The file that is gone.
        path: PathBuf,
    },
}

/// The agent thread and the two ends of the conversation.
pub struct NativeResourceAgent {
    requests: Sender<Request>,
    completions: Receiver<Completion>,
    warnings: Receiver<StorageWarning>,
    thread: Option<JoinHandle<()>>,
}

impl NativeResourceAgent {
    /// Start the thread, with the storage checks `checks` describes standing
    /// in front of every log buffer it creates.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the thread cannot be spawned.
    pub fn start(checks: StorageChecks) -> io::Result<Self> {
        let (request_tx, request_rx) = mpsc::channel::<Request>();
        let (completion_tx, completion_rx) = mpsc::channel::<Completion>();
        let (warning_tx, warning_rx) = mpsc::channel::<StorageWarning>();

        let thread = std::thread::Builder::new()
            .name("deepmsg-native-resource-agent".to_string())
            .spawn(move || Self::run(&request_rx, &completion_tx, &warning_tx, &checks))?;

        Ok(Self {
            requests: request_tx,
            completions: completion_rx,
            warnings: warning_rx,
            thread: Some(thread),
        })
    }

    /// Ask for a log buffer to be created and mapped, sparse or dense as
    /// `is_sparse` says — the URI's `sparse=` resolved against the driver's
    /// `term.buffer.sparse.file`.
    ///
    /// Returns as soon as the request is queued; the answer arrives from
    /// [`NativeResourceAgent::poll`].
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone.
    pub fn map_log_buffer(
        &self,
        path: &Path,
        term_length: i32,
        page_size: usize,
        is_sparse: bool,
    ) -> io::Result<()> {
        self.requests
            .send(Request::MapLogBuffer {
                path: path.to_owned(),
                term_length,
                page_size,
                is_sparse,
            })
            .map_err(|_| io::Error::other("the native resource agent has stopped"))
    }

    /// Hand a log buffer back to be unmapped and removed.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone — in which case the mapping is
    /// dropped here, on the caller's thread, which is slower but correct.
    pub fn free_log_buffer(&self, log: LogFile) -> io::Result<()> {
        self.requests
            .send(Request::FreeLogBuffer { log: Box::new(log) })
            .map_err(|_| io::Error::other("the native resource agent has stopped"))
    }

    /// Everything the agent finished since the last call.
    pub fn poll(&self) -> Vec<Completion> {
        let mut done = Vec::new();

        while let Ok(completion) = self.completions.try_recv() {
            done.push(completion);
        }

        done
    }

    /// Every storage warning raised since the last call — filesystems that
    /// could hold their log buffer but only just
    /// (`aeron_driver_context.c:1368-1377`). These do not fail anything: the
    /// create they accompanied went ahead, and the warning is for the
    /// driver's error log.
    pub fn poll_warnings(&self) -> Vec<StorageWarning> {
        let mut raised = Vec::new();

        while let Ok(warning) = self.warnings.try_recv() {
            raised.push(warning);
        }

        raised
    }

    /// Stop the thread and wait for it.
    ///
    /// The agent finishes what is queued before it stops, so a log buffer asked
    /// for and never collected is still created and does not become a mapping
    /// nobody can name.
    pub fn stop(mut self) {
        self.shutdown();
    }

    /// The same, for a caller that holds the agent behind a borrow and cannot
    /// give it up: the thread is joined and the value is left inert, so a
    /// second call does nothing.
    pub fn shutdown(&mut self) {
        let _ = self.requests.send(Request::Stop);

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// The agent's own loop.
    fn run(
        requests: &Receiver<Request>,
        completions: &Sender<Completion>,
        warnings: &Sender<StorageWarning>,
        checks: &StorageChecks,
    ) {
        while let Ok(request) = requests.recv() {
            match request {
                Request::MapLogBuffer {
                    path,
                    term_length,
                    page_size,
                    is_sparse,
                } => {
                    // The space question first, the file second: a refusal
                    // here never touches the filesystem, which is the point
                    // of asking before allocating (`aeron_driver_context.c:1354-1377`).
                    let (refusal, warning) = checks.assess(term_length, page_size);
                    let completion = match refusal {
                        Some(error) => Completion::MapFailed { path, error },
                        None => {
                            // A warning is not a refusal: the reference
                            // records it before it maps and carries on
                            // (`:1374` inside the check, the create after the
                            // `return 0`). Dropped rather than fatal if the
                            // conductor is gone — the completion below is
                            // what says the loop is over, and it says it.
                            if let Some(warning) = warning {
                                let _ = warnings.send(warning);
                            }
                            match LogFile::create(&path, term_length, page_size, is_sparse) {
                                Ok(log) => Completion::Mapped {
                                    path,
                                    log: Box::new(log),
                                },
                                Err(error) => Completion::MapFailed { path, error },
                            }
                        }
                    };

                    if completions.send(completion).is_err() {
                        break;
                    }
                }
                Request::FreeLogBuffer { log } => {
                    let path = log.path().to_owned();
                    let _ = log.remove();

                    if completions.send(Completion::Freed { path }).is_err() {
                        break;
                    }
                }
                Request::Stop => break,
            }
        }
    }
}

impl std::fmt::Debug for NativeResourceAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeResourceAgent")
            .field("running", &self.thread.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::logbuffer::descriptor;

    /// The smallest legal term, so a test file is 200 KiB rather than 192 MiB.
    const TERM_LENGTH: i32 = descriptor::TERM_MIN_LENGTH;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-agent-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create the directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Poll until something arrives, or give up.
    fn await_completion(agent: &NativeResourceAgent, within: std::time::Duration) -> Completion {
        let deadline = std::time::Instant::now() + within;

        loop {
            if let Some(completion) = agent.poll().into_iter().next() {
                return completion;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the agent did not answer in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn a_mapped_log_buffer_comes_back_to_the_caller() {
        let dir = TempDir::new();
        let path = dir.0.join("mapped.logbuffer");
        let agent = NativeResourceAgent::start(StorageChecks::new(false, 0, PathBuf::new()))
            .expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096, false)
            .expect("queued");

        match await_completion(&agent, std::time::Duration::from_secs(5)) {
            Completion::Mapped { path: done, log } => {
                assert_eq!(path, done);
                assert_eq!(
                    3 * TERM_LENGTH as usize + descriptor::METADATA_LENGTH,
                    log.length()
                );
                assert!(log.metadata().is_some(), "and it is usable where it landed");
            }
            other => panic!("expected a mapping, got {other:?}"),
        }

        agent.stop();
    }

    #[test]
    fn a_sparse_log_buffer_leaves_its_pages_to_the_filesystem() {
        // The reference's default shape: `term.buffer.sparse.file` true, so
        // a log buffer declares its length and touches none of it, and the
        // pages are the filesystem's to hand out on first write.
        let dir = TempDir::new();
        let path = dir.0.join("sparse.logbuffer");
        let agent = NativeResourceAgent::start(StorageChecks::new(false, 0, PathBuf::new()))
            .expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096, true)
            .expect("queued");
        match await_completion(&agent, std::time::Duration::from_secs(5)) {
            Completion::Mapped { .. } => {}
            other => panic!("expected a mapping, got {other:?}"),
        }
        agent.stop();

        // A dense twin, for comparison: the same length, every block in use.
        let dense = LogFile::create(&dir.0.join("dense.logbuffer"), TERM_LENGTH, 4096, false)
            .expect("a dense log buffer");

        use std::os::unix::fs::MetadataExt;
        let sparse = std::fs::metadata(&path).expect("the sparse file");
        let dense_meta = std::fs::metadata(dense.path()).expect("the dense file");

        assert_eq!(
            dense.length(),
            usize::try_from(sparse.len()).expect("a length that fits"),
            "the same length on the tin"
        );
        assert!(
            sparse.blocks() < dense_meta.blocks(),
            "sparse used {} blocks where dense used {}",
            sparse.blocks(),
            dense_meta.blocks()
        );
    }

    #[test]
    fn a_log_buffer_is_refused_when_the_filesystem_cannot_be_asked() {
        // The reference's `aeron_usable_fs_space` answers zero when the
        // filesystem cannot be asked (`aeron_fileutil.c:952-961`), and a
        // check that cannot ask refuses — with the `ENOSPC` the error
        // composition turns into `STORAGE_SPACE`
        // (`aeron_driver_conductor.c:2326-2341`).
        let dir = TempDir::new();
        let agent = NativeResourceAgent::start(StorageChecks::new(
            true,
            0,
            dir.0.join("no-such-directory"),
        ))
        .expect("the agent starts");

        agent
            .map_log_buffer(&dir.0.join("refused.logbuffer"), TERM_LENGTH, 4096, false)
            .expect("queued");

        match await_completion(&agent, std::time::Duration::from_secs(5)) {
            Completion::MapFailed { error, .. } => {
                assert_eq!(Some(libc::ENOSPC), error.raw_os_error());
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert!(
            !dir.0.join("refused.logbuffer").exists(),
            "the refusal never touched the filesystem"
        );
        // A filesystem with nothing left is below any threshold too, and the
        // refusal is what answers: the reference returns before the warning's
        // question is ever asked (`aeron_driver_context.c:1360-1366`).
        assert!(agent.poll_warnings().is_empty());

        agent.stop();
    }

    #[test]
    fn a_nearly_full_filesystem_warns_and_the_log_buffer_still_lands() {
        // The reference's second half (`aeron_driver_context.c:1368-1377`):
        // at or below the threshold it records a warning in the distinct
        // error log and returns zero, so the create it was asked about
        // happens anyway. A threshold no filesystem can exceed stands in for
        // a nearly-full one.
        let dir = TempDir::new();
        let path = dir.0.join("warned.logbuffer");
        let agent = NativeResourceAgent::start(StorageChecks::new(true, u64::MAX, dir.0.clone()))
            .expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096, true)
            .expect("queued");

        match await_completion(&agent, std::time::Duration::from_secs(5)) {
            Completion::Mapped { .. } => {}
            other => panic!("the warning did not stop the create: {other:?}"),
        }

        let warnings = agent.poll_warnings();
        assert_eq!(1, warnings.len(), "one warning per log buffer asked about");
        assert_eq!(u64::MAX, warnings[0].threshold);
        assert!(warnings[0].usable > 0, "a real filesystem answered");
        assert_eq!(dir.0, warnings[0].dir);

        // A second ask warns again: the reference records per check, and it
        // is the conductor's log that counts the sightings.
        agent
            .map_log_buffer(&dir.0.join("again.logbuffer"), TERM_LENGTH, 4096, true)
            .expect("queued");
        let _ = await_completion(&agent, std::time::Duration::from_secs(5));
        assert_eq!(1, agent.poll_warnings().len());

        agent.stop();
    }

    #[test]
    fn a_file_that_already_exists_comes_back_as_a_failure() {
        let dir = TempDir::new();
        let path = dir.0.join("taken.logbuffer");
        std::fs::write(&path, b"not a log buffer").expect("the file exists");

        let agent = NativeResourceAgent::start(StorageChecks::new(false, 0, PathBuf::new()))
            .expect("the agent starts");
        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096, false)
            .expect("queued");

        match await_completion(&agent, std::time::Duration::from_secs(5)) {
            Completion::MapFailed { error, .. } => {
                assert_eq!(io::ErrorKind::AlreadyExists, error.kind());
            }
            other => panic!("expected a failure, got {other:?}"),
        }

        agent.stop();
    }

    #[test]
    fn freeing_removes_the_file_on_the_agent_thread() {
        let dir = TempDir::new();
        let path = dir.0.join("freed.logbuffer");
        let agent = NativeResourceAgent::start(StorageChecks::new(false, 0, PathBuf::new()))
            .expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096, false)
            .expect("queued");
        let Completion::Mapped { log, .. } =
            await_completion(&agent, std::time::Duration::from_secs(5))
        else {
            panic!("expected a mapping");
        };

        agent.free_log_buffer(*log).expect("queued");

        assert!(matches!(
            await_completion(&agent, std::time::Duration::from_secs(5)),
            Completion::Freed { .. }
        ));
        assert!(!path.exists(), "the file is gone");

        agent.stop();
    }

    #[test]
    fn stopping_waits_for_the_work_already_queued() {
        // A log buffer asked for and never collected must not become a mapping
        // nobody can name: the agent finishes what it was given.
        let dir = TempDir::new();
        let path = dir.0.join("late.logbuffer");
        let agent = NativeResourceAgent::start(StorageChecks::new(false, 0, PathBuf::new()))
            .expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096, false)
            .expect("queued");
        agent.stop();

        assert!(path.exists(), "the mapping was made and the file is there");
    }
}
