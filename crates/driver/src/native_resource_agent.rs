//! The thread that owns the file work the conductor must not block on.
//!
//! Creating a log buffer means allocating and touching its whole length — for
//! an IPC publication at the default term length that is 192 MiB — and the
//! conductor is the process's control plane: a conductor that spends tens of
//! milliseconds inside `fallocate` is a conductor that has stopped draining
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
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::JoinHandle;

use deepmsg_core::logbuffer::logfile::LogFile;

/// What the conductor asks the agent to do.
enum Request {
    MapLogBuffer {
        path: PathBuf,
        term_length: i32,
        page_size: usize,
    },
    FreeLogBuffer {
        log: Box<LogFile>,
    },
    Stop,
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
    thread: Option<JoinHandle<()>>,
}

impl NativeResourceAgent {
    /// Start the thread.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the thread cannot be spawned.
    pub fn start() -> io::Result<Self> {
        let (request_tx, request_rx) = mpsc::channel::<Request>();
        let (completion_tx, completion_rx) = mpsc::channel::<Completion>();

        let thread = std::thread::Builder::new()
            .name("deepmsg-native-resource-agent".to_string())
            .spawn(move || Self::run(&request_rx, &completion_tx))?;

        Ok(Self {
            requests: request_tx,
            completions: completion_rx,
            thread: Some(thread),
        })
    }

    /// Ask for a log buffer to be created and mapped.
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
    ) -> io::Result<()> {
        self.requests
            .send(Request::MapLogBuffer {
                path: path.to_owned(),
                term_length,
                page_size,
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

        loop {
            match self.completions.try_recv() {
                Ok(completion) => done.push(completion),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }

        done
    }

    /// Stop the thread and wait for it.
    ///
    /// The agent finishes what is queued before it stops, so a log buffer asked
    /// for and never collected is still created and does not become a mapping
    /// nobody can name.
    pub fn stop(mut self) {
        let _ = self.requests.send(Request::Stop);

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// The agent's own loop.
    fn run(requests: &Receiver<Request>, completions: &Sender<Completion>) {
        while let Ok(request) = requests.recv() {
            match request {
                Request::MapLogBuffer {
                    path,
                    term_length,
                    page_size,
                } => {
                    let completion = match LogFile::create(&path, term_length, page_size) {
                        Ok(log) => Completion::Mapped {
                            path,
                            log: Box::new(log),
                        },
                        Err(error) => Completion::MapFailed { path, error },
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
        let agent = NativeResourceAgent::start().expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096)
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
    fn a_file_that_already_exists_comes_back_as_a_failure() {
        let dir = TempDir::new();
        let path = dir.0.join("taken.logbuffer");
        std::fs::write(&path, b"not a log buffer").expect("the file exists");

        let agent = NativeResourceAgent::start().expect("the agent starts");
        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096)
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
        let agent = NativeResourceAgent::start().expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096)
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
        let agent = NativeResourceAgent::start().expect("the agent starts");

        agent
            .map_log_buffer(&path, TERM_LENGTH, 4096)
            .expect("queued");
        agent.stop();

        assert!(path.exists(), "the mapping was made and the file is there");
    }
}
