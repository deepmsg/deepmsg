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
//! # The name resolver lives here
//!
//! A `getaddrinfo` that will not answer is the same problem as `fallocate` and
//! worse: a host whose nameserver is unreachable takes seconds, and the
//! conductor that waits is a conductor whose heartbeat stops — which every
//! client reads as a driver that has died. So the resolver is **built** where
//! the counters are allocated (the conductor, which owns the allocator) and
//! **run** here: its own clock turns on this thread's duty cycle, its `start`
//! resolves the bootstrap neighbours here, and a channel's names are resolved
//! here, as agent commands (`aeron_driver_native_resource_agent.c:253-270` for
//! the cycle, `:361-392` for the two commands, `:224-251` for `start`).
//!
//! # What it does not do
//!
//! The reference's agent also runs the asynchronous error-log and
//! counter-reclaim duties.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;

use crate::driver::Role;

use deepmsg_cnc::{CncFile, CounterManager};
use deepmsg_core::logbuffer::logfile::LogFile;

use crate::name_resolver::Resolver;
use crate::udp_channel::{UdpChannel, UdpChannelError, Unresolved};

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
    /// Finish parsing a channel: every name in it goes through the resolver
    /// (`aeron_driver_native_resource_agent_on_parse_udp_channel`,
    /// `:381-392`, and behind it `aeron_udp_channel_finish_parse`).
    ParseChannel {
        /// The URI as the client wrote it, which is what the parse is handed
        /// and what its error messages name (`aeron_udp_channel.c:346-381`).
        original_uri: Vec<u8>,
        /// What a name that does not answer means for this caller: a channel
        /// is refused, a send destination is kept (`:5332-5343`).
        unresolved: Unresolved,
        /// Whether the command is a **send destination**, whose address is
        /// validated and resolved beside the channel (`:5332-5413`).
        send_destination: bool,
        /// Where the answer goes.
        result: ResolutionCell<ParsedChannel>,
    },
    /// The driver's resolver, handed over once the CnC file has been published
    /// and can be shared.
    ///
    /// It arrives as a request rather than at start because of an ordering the
    /// driver keeps: the agent thread is up **before** the file is published,
    /// so that a driver which cannot create log buffers never claims to be
    /// ready, and the publication needs a mutable borrow of the file that an
    /// `Arc` would take away. The agent is idle until this arrives, which is
    /// microseconds inside the driver's construction.
    AttachResolver(AgentResolver),
    /// Resolve one `host:port` again, for a name that has gone stale
    /// (`aeron_driver_native_resource_agent_on_resolve_address`, `:361-379`).
    ResolveAddress {
        /// The name and port as the channel wrote them.
        text: String,
        /// The URI parameter it came from, which the message names.
        uri_param_name: String,
        /// Where the answer goes.
        result: ResolutionCell<SocketAddr>,
    },
    Stop,
}

/// The state cell one agent command answers through, which is the reference's
/// `aeron_driver_native_resource_agent_command_result_t` (`:120-150`): the
/// agent fills the payload and then **publishes** it, and the conductor's
/// command state machine reads it once per duty cycle and stays `RUNNING`
/// until it is there (`aeron_driver_conductor.c:4113-4121`).
///
/// The publish is what the reference's `AERON_SET_RELEASE` on `state` is for,
/// and [`OnceLock`] is this build's spelling of it: `set` happens once, under a
/// release, and `get` is the acquire. `PENDING` is `get() == None`, and the two
/// terminal states are the two arms of the `Result` — which is a state the
/// reference keeps in the same word and splits only to report an error.
pub type ResolutionCell<T> = Arc<OnceLock<Result<T, UdpChannelError>>>;

/// What a parsed channel answers with.
///
/// The address is the send destination's, and `None` for everything else: a
/// destination that names a host which does not answer is kept with the address
/// the reference calls `AF_UNSPEC`, and that `Option` is where it lives
/// (`aeron_driver_conductor_execute_add_send_destination`, `:5337-5343`).
#[derive(Clone, Debug)]
pub struct ParsedChannel {
    pub channel: UdpChannel,
    pub send_destination_address: Option<SocketAddr>,
}

/// A resolver, and everything the agent thread needs to run it.
///
/// Built where the counters are, and handed over rather than built here,
/// because a resolver takes counters out of the allocator and **the allocator
/// has one owner** — the conductor. The reference builds it from the context
/// inside the agent's own init (`aeron_driver_native_resource_agent.c:280-310`);
/// this build cannot copy that arrangement without giving two threads
/// allocators that can hand out the same id twice.
pub struct AgentResolver {
    resolver: Box<dyn Resolver + Send>,
    /// The file whose counter regions the resolver reads and writes, borrowed
    /// per call the way every other agent does it.
    cnc: Arc<CncFile>,
    /// How long a resolution may take before system counter 33 counts it
    /// (`aeron.name.resolver.threshold`).
    threshold_ns: i64,
    /// The value region's length and reuse window, which an agent-side view of
    /// the allocator is built from — it never allocates, so the view only has
    /// to agree about where an id's value lives.
    free_to_reuse_timeout_ms: i64,
    /// How long every resolution is held before it is answered
    /// (`debug.resolver.delay.millis`): zero on every driver but a test's, and
    /// what it stands in for is a nameserver that does not answer.
    debug_delay: std::time::Duration,
}

impl AgentResolver {
    /// Pair a resolver — built where the counters are allocated — with what the
    /// agent thread needs to run it.
    pub fn new(
        resolver: Box<dyn Resolver + Send>,
        cnc: Arc<CncFile>,
        threshold_ns: i64,
        free_to_reuse_timeout_ms: i64,
        debug_delay: std::time::Duration,
    ) -> Self {
        Self {
            resolver,
            cnc,
            threshold_ns,
            free_to_reuse_timeout_ms,
            debug_delay,
        }
    }
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

/// Something the agent could not do, for the conductor to record.
///
/// The reference's agent writes these straight into the shared error log
/// (`aeron_driver_distinct_error_log_record`, and its resolver's own failures at
/// `aeron_driver_name_resolver.c:214-216`, `:718-723`). That log is a
/// process-local structure here with the conductor as its only writer — the
/// same arrangement the storage warnings already use — so an agent fault is
/// handed over rather than recorded.
#[derive(Debug)]
pub struct AgentFault {
    /// The code the entry is recorded under, the reference's own negation.
    pub error_code: i32,
    /// The words, already composed.
    pub description: String,
}

/// The native resource agent in three pieces, before any of them has a thread.
pub(crate) struct AgentParts {
    /// The end every manager asks through.
    pub handle: AgentHandle,
    /// What the conductor reads.
    pub queues: AgentQueues,
    /// The work, which a runner drives one pass at a time.
    pub state: AgentLoop,
}

/// The three queues the conductor drains.
///
/// They are the agent's own ends, held apart from its work so that a manager
/// can hand the work to a runner without giving up the answers it reads.
pub(crate) struct AgentQueues {
    completions: Receiver<Completion>,
    warnings: Receiver<StorageWarning>,
    faults: Receiver<AgentFault>,
}

/// The agent thread and the ends of the conversation.
pub struct NativeResourceAgent {
    requests: Sender<Request>,
    queues: AgentQueues,
    thread: Option<JoinHandle<()>>,
}

impl AgentQueues {
    /// Everything the agent finished since the last call.
    pub fn poll(&self) -> Vec<Completion> {
        let mut done = Vec::new();

        while let Ok(completion) = self.completions.try_recv() {
            done.push(completion);
        }

        done
    }

    /// Every storage warning raised since the last call.
    pub fn poll_warnings(&self) -> Vec<StorageWarning> {
        let mut raised = Vec::new();

        while let Ok(warning) = self.warnings.try_recv() {
            raised.push(warning);
        }

        raised
    }

    /// Everything the agent could not do since the last call.
    pub fn poll_faults(&self) -> Vec<AgentFault> {
        let mut faults = Vec::new();

        while let Ok(fault) = self.faults.try_recv() {
            faults.push(fault);
        }

        faults
    }
}

impl NativeResourceAgent {
    /// Start the thread, with the storage checks `checks` describes standing
    /// in front of every log buffer it creates.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the thread cannot be spawned.
    pub fn start(checks: StorageChecks) -> io::Result<Self> {
        let AgentParts {
            handle,
            queues,
            state,
        } = Self::split(checks)?;

        let thread = crate::driver::run_agent(
            Role::NativeResourceAgent.classic_name(),
            state,
            crate::driver::default_strategy(),
            None,
        )?;

        Ok(Self {
            requests: handle.requests,
            queues,
            thread: Some(thread),
        })
    }

    /// The three pieces this is made of, **without** a thread: the handle every
    /// manager asks through, the queues the conductor drains, and the work a
    /// runner drives.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the channels cannot be made, which is a failure of the
    /// process rather than of a setting.
    pub(crate) fn split(checks: StorageChecks) -> io::Result<AgentParts> {
        let (request_tx, request_rx) = mpsc::channel::<Request>();
        let (completion_tx, completion_rx) = mpsc::channel::<Completion>();
        let (warning_tx, warning_rx) = mpsc::channel::<StorageWarning>();
        let (fault_tx, fault_rx) = mpsc::channel::<AgentFault>();

        Ok(AgentParts {
            handle: AgentHandle {
                requests: request_tx,
            },
            queues: AgentQueues {
                completions: completion_rx,
                warnings: warning_rx,
                faults: fault_rx,
            },
            state: AgentLoop::new(checks, request_rx, completion_tx, warning_tx, fault_tx),
        })
    }

    /// Hand the driver's resolver to the agent, which starts it and runs it
    /// from then on.
    ///
    /// The reference has **one** native resource agent and the resolver lives
    /// on it (`aeron_driver_native_resource_agent.c:224-270`), and so does this
    /// build: the three that were one per kind of log buffer were collapsed
    /// into this one, and the resolver came with them.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone.
    pub fn attach_resolver(&self, resolver: AgentResolver) -> io::Result<()> {
        self.requests
            .send(Request::AttachResolver(resolver))
            .map_err(|_| io::Error::other("the native resource agent has stopped"))
    }

    /// A handle for the requests that are not log buffers.
    ///
    /// The manager that owns this agent hands one out so that a caller can keep
    /// it while that manager is borrowed — the conductor does, because the
    /// commands it parks are decoded inside a closure that already holds the
    /// manager mutably.
    pub fn handle(&self) -> AgentHandle {
        AgentHandle {
            requests: self.requests.clone(),
        }
    }

    /// Ask for a channel to be parsed — every name in it through the resolver
    /// — and keep the cell the answer will arrive in.
    ///
    /// Returns as soon as the request is queued; the command stays `RUNNING`
    /// until [`ResolutionCell::get`] answers, which is the conductor's job to
    /// notice.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone. The cell is filled with the
    /// failure in that case, so a caller that only polls the cell still
    /// finishes.
    pub fn parse_channel(
        &self,
        original_uri: &[u8],
        unresolved: Unresolved,
    ) -> io::Result<ResolutionCell<ParsedChannel>> {
        self.parse(original_uri, unresolved, false)
    }

    /// The same for a **send destination**, whose address is resolved beside
    /// its channel.
    ///
    /// # Errors
    ///
    /// The same as [`AgentHandle::parse_channel`].
    pub fn parse_send_destination(
        &self,
        original_uri: &[u8],
    ) -> io::Result<ResolutionCell<ParsedChannel>> {
        self.parse(original_uri, Unresolved::Keep, true)
    }

    fn parse(
        &self,
        original_uri: &[u8],
        unresolved: Unresolved,
        send_destination: bool,
    ) -> io::Result<ResolutionCell<ParsedChannel>> {
        let result: ResolutionCell<ParsedChannel> = Arc::new(OnceLock::new());
        self.requests
            .send(Request::ParseChannel {
                original_uri: original_uri.to_vec(),
                unresolved,
                send_destination,
                result: Arc::clone(&result),
            })
            .map_err(|_| io::Error::other("the native resource agent has stopped"))?;

        Ok(result)
    }

    /// Ask for one `host:port` to be resolved again, and keep the cell.
    ///
    /// # Errors
    ///
    /// The same as [`NativeResourceAgent::parse_channel`].
    pub fn resolve_address(
        &self,
        text: &str,
        uri_param_name: &str,
    ) -> io::Result<ResolutionCell<SocketAddr>> {
        let result: ResolutionCell<SocketAddr> = Arc::new(OnceLock::new());
        self.requests
            .send(Request::ResolveAddress {
                text: text.to_owned(),
                uri_param_name: uri_param_name.to_owned(),
                result: Arc::clone(&result),
            })
            .map_err(|_| io::Error::other("the native resource agent has stopped"))?;

        Ok(result)
    }

    /// Everything the agent could not do since the last call, for the
    /// conductor's error log.
    pub fn poll_faults(&self) -> Vec<AgentFault> {
        self.queues.poll_faults()
    }
}

impl NativeResourceAgent {
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
        self.queues.poll()
    }

    /// Every storage warning raised since the last call — filesystems that
    /// could hold their log buffer but only just
    /// (`aeron_driver_context.c:1368-1377`). These do not fail anything: the
    /// create they accompanied went ahead, and the warning is for the
    /// driver's error log.
    pub fn poll_warnings(&self) -> Vec<StorageWarning> {
        self.queues.poll_warnings()
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
}

/// One pass of the agent, for the runner that drives it — and the close that
/// lets the resolver go with the thread that ran it.
impl crate::driver::Agent for AgentLoop {
    fn do_work(&mut self) -> Option<usize> {
        Self::do_work(self)
    }

    fn close(&mut self) {
        Self::close(self);
    }
}

/// What the agent owns **between passes**: the resolver it was handed and the
/// allocator view it resolves through.
///
/// The reference keeps the same two things in its agent's own state
/// (`aeron_driver_native_resource_agent_t`), and they are a struct here rather
/// than the loop's locals so that the loop body can be driven a pass at a time.
/// Give every path in `pending` one attempt at removal, keeping the ones that
/// fail and reporting each failure.
///
/// One pass over the queue, not a loop until it empties: a file that cannot be
/// removed will not remove itself on the second try within the same pass, and
/// the reference's own drain makes one attempt each
/// (`aeron_driver_native_resource_agent.c:205-222`).
fn retry_frees(pending: &mut VecDeque<PathBuf>, mut on_failure: impl FnMut()) {
    let mut attempts = pending.len();

    while attempts > 0 {
        attempts -= 1;

        let Some(path) = pending.pop_front() else {
            break;
        };

        if std::fs::remove_file(&path).is_err() {
            on_failure();
            pending.push_back(path);
        }
    }
}

pub(crate) struct AgentLoop {
    /// What the storage checks were built from, kept because every create asks
    /// them again.
    checks: StorageChecks,
    /// The requests every manager sends through its [`AgentHandle`].
    requests: Receiver<Request>,
    /// The three queues the conductor drains.
    completions: Sender<Completion>,
    warnings: Sender<StorageWarning>,
    faults: Sender<AgentFault>,
    /// The resolver arrives with the CnC file, once the driver has published
    /// it, and so does the counters view this thread resolves through: an
    /// allocator view of its own over the conductor's regions, which is how the
    /// sender and the receiver already hold theirs. Safe because **only the
    /// conductor allocates** — this view only has to agree about where an id's
    /// value lives, and it never hands one out.
    resolver: Option<AgentResolver>,
    counters: Option<CounterManager>,
    /// Log buffers whose file could not be removed, waiting to be tried again
    /// (`aeron_driver_native_resource_agent.c:213-220`: the reference keeps the
    /// same queue and re-adds to its tail).
    ///
    /// The **path**, not the `LogFile`: [`LogFile::remove`] drops the mapping
    /// before it removes the file, so a failure leaves nothing to retry but the
    /// file itself — and the mapping is what the reference's retry keeps, which
    /// is the one thing about it this build cannot reproduce.
    pending_frees: VecDeque<std::path::PathBuf>,
}

impl AgentLoop {
    /// The work, with the queues it reads and writes.
    ///
    /// It is a struct rather than a set of loop locals because a runner has to
    /// be able to drive it: in `DEDICATED` that runner owns a thread of its
    /// own, and in `SHARED` it is the thread the conductor, the sender and the
    /// receiver are on.
    const fn new(
        checks: StorageChecks,
        requests: Receiver<Request>,
        completions: Sender<Completion>,
        warnings: Sender<StorageWarning>,
        faults: Sender<AgentFault>,
    ) -> Self {
        Self {
            checks,
            requests,
            completions,
            warnings,
            faults,
            resolver: None,
            counters: None,
            pending_frees: VecDeque::new(),
        }
    }

    /// One pass: the resolver's duty cycle, then the requests that have arrived.
    ///
    /// `None` when a `Stop` came in — or when the handle that sends requests is
    /// gone — which is the reference's `running` flag cleared. `Some(work)` is
    /// what the idle strategy is given.
    /// Count a log buffer whose file could not be removed
    /// (`AERON_SYSTEM_COUNTER_FREE_FAILS`, `aeron_system_counters.c:43`).
    ///
    /// The counter regions are reached the way the resolver's own counters are
    /// (`:871-880`): through the resolver's file, which is where they live. The
    /// driver attaches a resolver before any log buffer can be freed — the
    /// resolver is built unconditionally in the conductor's start
    /// (`conductor.rs:815`) and attached a few lines before the agent is told
    /// anything else — so this is a reachable path, not a hopeful one.
    fn count_free_failure(&self) {
        let (Some(counters), Some(AgentResolver { cnc, .. })) =
            (self.counters.as_ref(), self.resolver.as_ref())
        else {
            return;
        };

        if let Some(regions) = cnc.counter_regions() {
            crate::system_counters::increment(
                counters,
                &regions,
                crate::system_counters::id::FREE_FAILS,
            );
        }
    }

    fn do_work(&mut self) -> Option<usize> {
        {
            // Log buffers whose removal failed on an earlier pass get their
            // next attempt here, before this pass's new work
            // (`aeron_driver_native_resource_agent.c:213-220`: the reference
            // drains the same queue at the top of its pass).
            let mut pending = std::mem::take(&mut self.pending_frees);
            retry_frees(&mut pending, || self.count_free_failure());
            self.pending_frees = pending;

            // The resolver's own clock, on this thread's duty cycle
            // (`aeron_driver_native_resource_agent.c:253-270`): a driver nobody
            // is talking to still has to answer when someone does, and its
            // gossip runs on its own intervals.
            let now_ms = deepmsg_core::clock::epoch_nano_time() / NANOS_PER_MILLI;

            let mut work = if let (Some(AgentResolver { resolver, cnc, .. }), Some(counters)) =
                (self.resolver.as_mut(), self.counters.as_ref())
            {
                match cnc.counter_regions() {
                    Some(regions) => resolver.do_work(now_ms, counters, &regions),
                    None => 0,
                }
            } else {
                0
            };

            let mut stopped = false;
            loop {
                match self.requests.try_recv() {
                    Ok(Request::Stop) => {
                        stopped = true;
                        break;
                    }
                    Ok(Request::AttachResolver(attached)) => {
                        work += 1;
                        self.counters = CounterManager::new(
                            attached.cnc.layout().counters_values.len(),
                            attached.free_to_reuse_timeout_ms,
                        );
                        self.resolver = Some(attached);

                        // `start` runs **here**, on the agent, and that is the
                        // whole point of the move: it resolves the bootstrap
                        // neighbours, and a nameserver that will not answer
                        // would otherwise stop the conductor before it has
                        // published a heartbeat
                        // (`aeron_driver_native_resource_agent.c:224-251`).
                        // A failure is recorded and the driver runs on, which
                        // is what the reference's agent does with it — a driver
                        // whose resolver is not the one it was configured with
                        // still resolves something.
                        if let (Some(agent), Some(counters)) =
                            (self.resolver.as_mut(), self.counters.as_ref())
                        {
                            if let Some(regions) = agent.cnc.counter_regions() {
                                if let Err(what) = agent.resolver.start(counters, &regions) {
                                    let _ = self.faults.send(resolver_start_fault(&what));
                                }
                            }
                        }
                    }
                    Ok(request) => {
                        work += 1;
                        if self.dispatch(request) {
                            stopped = true;
                            break;
                        }
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        stopped = true;
                        break;
                    }
                }
            }

            // What the resolver itself could not do, on its way to the
            // driver's error log (`aeron_name_resolver_log_and_clear_error`,
            // `aeron_driver_name_resolver.c:718-723`): the reference's resolver
            // writes into that log directly, and this one hands the entries
            // over because the log is the conductor's.
            if let Some(AgentResolver { resolver, .. }) = self.resolver.as_mut() {
                for fault in resolver.take_faults() {
                    let _ = self.faults.send(AgentFault {
                        error_code: fault.error_code,
                        description: fault.description,
                    });
                }
            }

            if stopped {
                return None;
            }

            Some(work)
        }
    }

    /// Let the resolver go, counters and all
    /// (`aeron_driver_name_resolver_close`, and `on_close` on the agent).
    fn close(&mut self) {
        if let (Some(AgentResolver { resolver, cnc, .. }), Some(counters)) =
            (self.resolver.as_mut(), self.counters.as_mut())
        {
            if let Some(regions) = cnc.counter_regions() {
                let now_ms = deepmsg_core::clock::epoch_nano_time() / NANOS_PER_MILLI;
                resolver.close(counters, &regions, now_ms);
            }
        }
    }

    /// One request, on the agent's thread. `true` when the loop should stop,
    /// which is a completion the conductor can no longer receive.
    fn dispatch(&mut self, request: Request) -> bool {
        let checks = &self.checks;
        let completions = &self.completions;
        let warnings = &self.warnings;

        match request {
            Request::ParseChannel {
                original_uri,
                unresolved,
                send_destination,
                result,
            } => {
                // Where a channel's names are resolved, and the reason the
                // conductor never waits on a nameserver again
                // (`aeron_udp_channel_finish_parse`,
                // `aeron-driver/src/main/c/aeron_udp_channel.c:346-381`).
                //
                // The delay is a test's, and it stands where a nameserver that
                // does not answer would.
                if let Some(AgentResolver { debug_delay, .. }) = self.resolver.as_ref() {
                    if !debug_delay.is_zero() {
                        std::thread::sleep(*debug_delay);
                    }
                }
                let parsed = match (self.resolver.as_mut(), self.counters.as_mut()) {
                    (
                        Some(AgentResolver {
                            resolver,
                            cnc,
                            threshold_ns,
                            ..
                        }),
                        Some(counters),
                    ) => match cnc.counter_regions() {
                        // A send destination is validated and its address
                        // resolved beside the channel, because the address is
                        // what a removal matches it by (`:311-350`).
                        Some(regions) if send_destination => {
                            crate::udp_channel::parse_send_destination_with(
                                &mut **resolver,
                                counters,
                                &regions,
                                *threshold_ns,
                                &original_uri,
                            )
                            .map(|(channel, address)| ParsedChannel {
                                channel,
                                send_destination_address: address,
                            })
                        }
                        Some(regions) => crate::udp_channel::parse_channel_with(
                            &mut **resolver,
                            counters,
                            &regions,
                            *threshold_ns,
                            unresolved,
                            &original_uri,
                        )
                        .map(|channel| ParsedChannel {
                            channel,
                            send_destination_address: None,
                        }),
                        None => Err(UdpChannelError::Resolution(
                            "the driver's counters are not mapped".to_owned(),
                        )),
                    },
                    _ => Err(UdpChannelError::Resolution(
                        "this agent has no resolver".to_owned(),
                    )),
                };

                Self::answer(&result, parsed);
                false
            }
            Request::ResolveAddress {
                text,
                uri_param_name,
                result,
            } => {
                let resolved = match (self.resolver.as_mut(), self.counters.as_mut()) {
                    (
                        Some(AgentResolver {
                            resolver,
                            cnc,
                            threshold_ns,
                            ..
                        }),
                        Some(counters),
                    ) => match cnc.counter_regions() {
                        Some(regions) => crate::udp_channel::resolve_host_and_port_with(
                            &mut **resolver,
                            counters,
                            &regions,
                            *threshold_ns,
                            &uri_param_name,
                            &text,
                        ),
                        None => Err(UdpChannelError::Resolution(
                            "the driver's counters are not mapped".to_owned(),
                        )),
                    },
                    _ => Err(UdpChannelError::Resolution(
                        "this agent has no resolver".to_owned(),
                    )),
                };

                Self::answer(&result, resolved);
                false
            }
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

                completions.send(completion).is_err()
            }
            Request::FreeLogBuffer { log } => {
                let path = log.path().to_owned();

                // The mapping goes either way — `LogFile::remove` drops it
                // first — so what a failure leaves behind is the **file**, and
                // that is what goes on the queue to be tried again
                // (`aeron_driver_native_resource_agent.c:405-412`).
                if log.remove().is_err() {
                    self.count_free_failure();
                    self.pending_frees.push_back(path.clone());
                }

                // Reported either way, which the reference does not do: its
                // failure path calls no callback at all. Nothing here reads the
                // completion for the file's sake — both consumers ignore it —
                // and a queue entry is the honest record that the file is still
                // there, so this stays as it was rather than changing what the
                // conductor waits for.
                completions.send(Completion::Freed { path }).is_err()
            }
            // Both are handled by the loop itself: `Stop` is what ends it, and
            // `AttachResolver` is what gives it a resolver to work with.
            Request::Stop | Request::AttachResolver(_) => true,
        }
    }

    /// Publish an answer into the cell a request was carrying.
    ///
    /// `OnceLock` cannot fail to be set twice — nothing else ever holds this
    /// cell, and the command that owns it is dropped with it — so the answer
    /// is unconditional and the conductor's next duty cycle is what picks it
    /// up.
    fn answer<T>(cell: &ResolutionCell<T>, answer: Result<T, UdpChannelError>) {
        let _ = cell.set(answer);
    }
}

/// The entry a resolver that would not start leaves in the error log
/// (`aeron_driver_native_resource_agent.c:233-251`): the reference records
/// either what the failure said or, when it said nothing, the words the agent
/// composes itself — under the negated generic code, because
/// `AERON_ERROR_CODE_GENERIC_ERROR` is what its `AERON_SET_ERR` was given.
///
/// The driver runs on either way, and that is the point of recording rather
/// than refusing: a driver whose resolver is not the one it was configured with
/// resolves *something*, and a deployment that reads its error log can see it.
fn resolver_start_fault(what: &str) -> AgentFault {
    AgentFault {
        error_code: -deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
        description: deepmsg_cnc::error_log::compose_description(
            -deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
            "aeron_driver_native_resource_agent_on_start",
            "aeron_driver_native_resource_agent.c",
            233,
            &format!("failed to start name resolver: {what}"),
        ),
    }
}

/// `AERON_NANOS_PER_MILLI` — the agent's clock is asked for milliseconds.
const NANOS_PER_MILLI: i64 = 1_000_000;

impl std::fmt::Debug for NativeResourceAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeResourceAgent")
            .field("running", &self.thread.is_some())
            .finish()
    }
}

/// The sending half of the conversation with an agent, on its own.
///
/// Everything a caller needs to ask the agent for something that is not a log
/// buffer, without holding the manager that owns the agent.
#[derive(Clone)]
pub struct AgentHandle {
    requests: Sender<Request>,
}

/// The queue, not the thread: a handle that is only printed is still a handle.
impl std::fmt::Debug for AgentHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentHandle").finish_non_exhaustive()
    }
}

impl AgentHandle {
    /// Hand the driver's resolver to the agent, which starts it and runs it
    /// from then on.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone.
    pub(crate) fn attach_resolver(&self, resolver: AgentResolver) -> io::Result<()> {
        self.requests
            .send(Request::AttachResolver(resolver))
            .map_err(|_| io::Error::other("the native resource agent has stopped"))
    }

    /// Tell the agent to stop, without waiting for it.
    ///
    /// Waiting is the driver's: in `SHARED` the agent shares its thread with
    /// the conductor, so the conductor cannot be the one to join it.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone already.
    pub(crate) fn stop(&self) -> io::Result<()> {
        self.requests
            .send(Request::Stop)
            .map_err(|_| io::Error::other("the native resource agent has stopped"))
    }

    /// Ask for a log buffer to be created and mapped, sparse or dense as
    /// `is_sparse` says — the URI's `sparse=` resolved against the driver's
    /// `term.buffer.sparse.file`.
    ///
    /// The answer is a [`Completion`] taken off the agent by the **conductor**
    /// (it drains [`AgentQueues`]) and routed to the manager that asked.
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

    /// Ask for a channel to be parsed — every name in it through the resolver.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the agent thread is gone.
    pub fn parse_channel(
        &self,
        original_uri: &[u8],
        unresolved: Unresolved,
    ) -> io::Result<ResolutionCell<ParsedChannel>> {
        self.parse(original_uri, unresolved, false)
    }

    /// The same for a **send destination**, whose address is resolved beside
    /// its channel.
    ///
    /// # Errors
    ///
    /// The same as [`AgentHandle::parse_channel`].
    pub fn parse_send_destination(
        &self,
        original_uri: &[u8],
    ) -> io::Result<ResolutionCell<ParsedChannel>> {
        self.parse(original_uri, Unresolved::Keep, true)
    }

    fn parse(
        &self,
        original_uri: &[u8],
        unresolved: Unresolved,
        send_destination: bool,
    ) -> io::Result<ResolutionCell<ParsedChannel>> {
        let result: ResolutionCell<ParsedChannel> = Arc::new(OnceLock::new());
        self.requests
            .send(Request::ParseChannel {
                original_uri: original_uri.to_vec(),
                unresolved,
                send_destination,
                result: Arc::clone(&result),
            })
            .map_err(|_| io::Error::other("the native resource agent has stopped"))?;

        Ok(result)
    }

    /// Ask for one `host:port` to be resolved again, and keep the cell.
    ///
    /// # Errors
    ///
    /// The same as [`NativeResourceAgent::parse_channel`].
    pub fn resolve_address(
        &self,
        text: &str,
        uri_param_name: &str,
    ) -> io::Result<ResolutionCell<SocketAddr>> {
        let result: ResolutionCell<SocketAddr> = Arc::new(OnceLock::new());
        self.requests
            .send(Request::ResolveAddress {
                text: text.to_owned(),
                uri_param_name: uri_param_name.to_owned(),
                result: Arc::clone(&result),
            })
            .map_err(|_| io::Error::other("the native resource agent has stopped"))?;

        Ok(result)
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

    /// One pass over the retry queue: what can be removed is, and what cannot
    /// is counted and kept.
    #[test]
    fn a_retry_pass_removes_what_it_can_and_counts_what_it_cannot() {
        let dir = TempDir::new();
        let there = dir.0.join("there.logbuffer");
        let gone = dir.0.join("gone.logbuffer");

        std::fs::write(&there, b"x").expect("a file");

        let mut pending = VecDeque::from(vec![there.clone(), gone.clone()]);
        let mut failures = 0;

        retry_frees(&mut pending, || failures += 1);

        assert!(!there.exists(), "the one that could be removed was");
        assert_eq!(1, failures, "and the one that could not was counted");
        assert_eq!(
            vec![gone],
            pending.into_iter().collect::<Vec<_>>(),
            "which is kept for the next pass, at the back"
        );
    }

    /// A removal that fails is not dropped on the floor: the file goes on the
    /// queue, which is what the reference does with the log buffer
    /// (`aeron_driver_native_resource_agent.c:405-412`).
    #[test]
    fn a_free_that_fails_is_kept_for_another_try() {
        // `queues` is held, not dropped: it owns the completion receiver, and a
        // dispatch whose send fails reports that the loop should stop.
        let AgentParts {
            mut state, queues, ..
        } = NativeResourceAgent::split(StorageChecks::new(false, 0, PathBuf::new()))
            .expect("the agent's parts");

        let dir = TempDir::new();
        let path = dir.0.join("free.logbuffer");
        let log = LogFile::create(&path, TERM_LENGTH, 4096, false).expect("a log buffer");

        // Out from under the mapping: `remove` drops the mapping and then finds
        // no file, which is the failure this is about.
        std::fs::remove_file(&path).expect("the file goes");

        assert!(
            !state.dispatch(Request::FreeLogBuffer { log: Box::new(log) }),
            "a free does not stop the loop"
        );
        assert_eq!(1, state.pending_frees.len(), "kept for another try");
        assert_eq!(Some(&path), state.pending_frees.front());

        drop(queues);
    }

    // The **counter** is not asserted anywhere, and that is a stated gap: it is
    // written through the resolver's file (`count_free_failure`), and the
    // resolver only arrives with `AttachResolver` — which needs a resolver
    // implementation and a CnC file to build. The driver always attaches one
    // before anything can be freed, so the path runs in every real driver and in
    // none of these tests.
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
