//! The recorder: every recording session in flight, and the numbers they add
//! up to.
//!
//! `ArchiveConductor.Recorder` is a `SessionWorker` of its own
//! (`ArchiveConductor.java:2699-2744`), driven on its own turn by the
//! conductor that owns it (`SharedModeArchiveConductor.java:57-66` — the
//! recorder runs *after* the conductor's own sessions). What it holds is the
//! recording sessions and three running totals; what it does not hold is
//! anything the *conductor* does when a session ends, which in the reference is
//! the `closeSession` hook a `SharedModeRecorder` overrides to reach back into
//! the conductor (`:88-96`).
//!
//! That split is kept here, and one step further: a session that has finished is
//! **handed back** by [`Recorder::take_finished`] rather than closed by the
//! recorder. The work of closing one — the catalog's stop position, the `STOP`
//! signal, the subscription's refcount — belongs to whoever holds the catalog,
//! the control sessions and the registry, and here that is one object that the
//! recorder does not own.
//!
//! # The totals, and when they are published
//!
//! Three numbers, accumulated from what the writers report:
//!
//! | counter | what it holds |
//! |---|---|
//! | 106 | the bytes every recording has written |
//! | 107 | the nanoseconds those writes took |
//! | 105 | the longest one write ever took |
//!
//! The reference accumulates all three from `RecordingWriter` calls
//! (`:2717-2730`, hung off the writer's own clock around each block,
//! `RecordingWriter.java:117`, `:143-145`) and publishes them **only on a turn
//! that did work** (`:2732-2743`). The condition is not an optimisation: what it
//! changes is *when* a reader of those counters sees them, and the reference's
//! line is the one to copy.

use deepmsg_client::client::Client;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::server::recording_events::{EventsError, RecordingEvents, RecordingEventsProxy};

use crate::recording_writer::WriteStats;
use crate::server::counters::{
    ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
    ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, ArchiveIdCounter, CounterAllocator, CounterKind,
    RECORDER_MAX_WRITE_TIME_NAME, RECORDER_TOTAL_WRITE_BYTES_NAME, RECORDER_TOTAL_WRITE_TIME_NAME,
};
use crate::server::recording_session::RecordingSession;

/// What every recording this archive has made adds up to
/// (`ArchiveConductor.java:2701-2703`).
///
/// `i64` and not `u64`, because the reference's three are `long`s and are
/// published into counters that are read as `long`s — the writer hands over a
/// byte count that cannot be negative, and the accumulator is the reference's
/// type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecorderTotals {
    total_write_bytes: i64,
    total_write_time_ns: i64,
    max_write_time_ns: i64,
}

impl WriteStats for RecorderTotals {
    fn bytes_written(&mut self, bytes: u64) {
        self.total_write_bytes += i64::try_from(bytes).unwrap_or(i64::MAX);
    }

    fn write_time_ns(&mut self, nanos: u64) {
        let nanos = i64::try_from(nanos).unwrap_or(i64::MAX);

        self.total_write_time_ns += nanos;

        // `if (nanos > maxWriteTimeNs)` (`:2726-2729`): the largest single
        // write, not the total.
        if nanos > self.max_write_time_ns {
            self.max_write_time_ns = nanos;
        }
    }
}

impl RecorderTotals {
    /// The bytes written, the nanoseconds spent, and the longest single write
    /// (`:2701-2703`).
    #[must_use]
    pub const fn totals(&self) -> (i64, i64, i64) {
        (
            self.total_write_bytes,
            self.total_write_time_ns,
            self.max_write_time_ns,
        )
    }
}

/// The three counters those totals are published to.
///
/// 105, 106 and 107, allocated lazily — which is the conductor's to do, since
/// it is the object with the client and the counters region.
#[derive(Debug)]
pub struct RecorderCounters {
    /// 105: the longest single write, whose allocation the reference refuses if
    /// one for this archive id is already there (`Archive.java:1589-1595`).
    pub max_write_time: ArchiveIdCounter,
    /// 106.
    pub total_write_bytes: ArchiveIdCounter,
    /// 107.
    pub total_write_time: ArchiveIdCounter,
}

/// The three write counters, in the order the reference allocates them
/// (`Archive.java:1587-1626`).
///
/// The order is not decoration: the reference asks for the max-write-time one
/// first and **refuses to start** if a 105 for this archive id is already there
/// (`:1589-1595`), and this build does that check where its archive starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecorderCounter {
    /// 105: the longest single write, whose duplicate is refused.
    MaxWriteTime,
    /// 106: the bytes every recording has written.
    TotalWriteBytes,
    /// 107: the nanoseconds those writes took.
    TotalWriteTime,
}

impl CounterKind for RecorderCounter {
    /// All three, in the order the reference allocates them.
    const ORDER: &'static [Self] = &[
        Self::MaxWriteTime,
        Self::TotalWriteBytes,
        Self::TotalWriteTime,
    ];

    /// The type id `ArchiveCounters.allocate` is called with (`:1598`, `:1610`,
    /// `:1622` over `AeronCounters`).
    fn type_id(self) -> i32 {
        match self {
            Self::MaxWriteTime => ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            Self::TotalWriteBytes => ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
            Self::TotalWriteTime => ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID,
        }
    }

    /// The name its label is built from (`:1600`, `:1612`, `:1624`).
    fn name(self) -> &'static str {
        match self {
            Self::MaxWriteTime => RECORDER_MAX_WRITE_TIME_NAME,
            Self::TotalWriteBytes => RECORDER_TOTAL_WRITE_BYTES_NAME,
            Self::TotalWriteTime => RECORDER_TOTAL_WRITE_TIME_NAME,
        }
    }
}

/// Every recording session, and the numbers they add up to.
#[derive(Debug)]
pub struct Recorder {
    /// The sessions still in flight, which is the reference's
    /// `SessionWorker.sessions` (`SessionWorker.java:23`).
    sessions: Vec<RecordingSession>,
    /// The ones that finished and have not been collected
    /// ([`Recorder::take_finished`]).
    finished: Vec<RecordingSession>,
    totals: RecorderTotals,
    /// `None` until the three counters have been allocated — and an archive that
    /// never allocated them still records, it just does not count.
    counters: Option<RecorderCounters>,
    /// The three, while they are being asked for and taken up.
    ///
    /// The reference allocates all three at once, in `Archive.Context.conclude`
    /// (`:1587-1626`), because its archive drives the driver; here each is a
    /// command the driver answers on a later turn, which is the loop
    /// [`CounterAllocator`] runs.
    request: RecorderCountersRequest,
    /// The recording events publication, if the deployment turned them on.
    ///
    /// Held **here** rather than by each session, which is the reference's
    /// arrangement too: its `ArchiveConductor` builds one
    /// (`ArchiveConductor.java:224-225`) and hands the same object to every
    /// session. It cannot be held by a session because a session does not own
    /// the thread it runs on and this build's client will not let two threads
    /// write one publication — so it lives with whatever runs the sessions, and
    /// a session borrows it for a turn.
    events: Option<RecordingEventsProxy>,
}

/// The three counters between the ask and the hand-over
/// (`Archive.java:1587-1626`).
///
/// It is its own type, and not simply a field of [`Recorder`], because of *where*
/// the ask can be made: naming the strategy and reading the answer is a client
/// operation, and when the recorder is on a thread of its own the client is not
/// there — a driver answers a command only on a turn, and the turn is the
/// conductor's. So the ask stays with whoever holds the client
/// ([`RecorderHalf::allocate`]) and only the result crosses, which is the
/// reference's own split: its `Recorder` is constructed inside
/// `Archive.Context.conclude` (`:1587-1626`), on the thread that owns the
/// `Aeron`, and handed to a runner afterwards.
#[derive(Debug)]
pub struct RecorderCountersRequest {
    allocator: CounterAllocator<RecorderCounter>,
}

impl RecorderCountersRequest {
    /// A request that has asked for nothing yet, for an archive with this id.
    #[must_use]
    pub const fn new(archive_id: i64) -> Self {
        Self {
            allocator: CounterAllocator::new(archive_id),
        }
    }

    /// One turn of the ask: send what has not been sent, read what has been
    /// answered, and give back all three the turn they are all in hand.
    ///
    /// A refusal is not retried — a counter the driver will not make can never
    /// become one, and a recorder with no counters still records — which is
    /// [`CounterAllocator`]'s own rule.
    pub fn drive(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: std::time::Duration,
    ) -> Option<RecorderCounters> {
        self.allocator.drive(client, counters, timeout);

        if !self.allocator.is_complete() {
            return None;
        }

        let take = |kind| self.allocator.counter(kind).expect("all three are in hand");

        Some(RecorderCounters {
            max_write_time: take(RecorderCounter::MaxWriteTime),
            total_write_bytes: take(RecorderCounter::TotalWriteBytes),
            total_write_time: take(RecorderCounter::TotalWriteTime),
        })
    }
}

/// The recorder as the conductor holds it (P2-9a).
///
/// Under `SHARED` the conductor drives every session inside its own turn
/// (`SharedModeArchiveConductor.java:57-66`) and so holds the [`Recorder`]
/// itself. Under `DEDICATED` the recorder is an agent on a thread of its own
/// (`DedicatedModeArchiveConductor.java:54-55`) and what the conductor holds is
/// a **channel** to it. The conductor asks the same six questions either way —
/// the four a request asks of the recorder, the turn, and the sessions that
/// finished — so it asks them of this.
///
/// # Why a seam rather than two conductors
///
/// `SHARED` has to stay exactly what it was. It is the mode the reference's own
/// C suite runs (306 criteria), so a queue placed in front of the recorder there
/// would change the thing being accepted rather than add a second thing beside
/// it. Everything the threaded arm does differently is therefore decided here,
/// in one place, and the inline arm is the reference's own call sequence.
///
/// # What a command cannot carry
///
/// Two of the six hand the recorder something it cannot be told about without
/// the *client*: [`Recorder::allocate`] sends the three `add_counter` commands
/// that become 105/106/107, and a driver answers a command only on a turn. So
/// allocation stays where the client is — the conductor — and what crosses to a
/// worker is the three counters it ends up with ([`Recorder::set_counters`]).
/// That is the reference's own split too: its recorder is constructed inside
/// `Archive.Context.conclude` (`:1587-1626`), on the thread that owns the
/// `Aeron`, and only afterwards handed to a runner.
pub enum RecorderHalf {
    /// `SHARED`: the conductor's turn drives every session.
    Inline(Recorder),
    /// `DEDICATED`: the recorder is on a thread of its own, and this is the
    /// conductor's end of the two channels to it.
    Threaded(RecorderThread),
}

impl RecorderHalf {
    /// A recorder for an archive with this id, in the conductor's own turn.
    #[must_use]
    pub const fn new(archive_id: i64) -> Self {
        Self::Inline(Recorder::new(archive_id))
    }

    /// A recorder for this archive running on `worker`'s thread, with this end
    /// kept for the conductor.
    #[must_use]
    pub const fn threaded(thread: RecorderThread) -> Self {
        Self::Threaded(thread)
    }

    /// `recorder.addSession` (`ArchiveConductor.java:2055`).
    pub fn add_session(&mut self, session: RecordingSession) {
        match self {
            Self::Inline(recorder) => recorder.add_session(session),
            Self::Threaded(thread) => thread.send(RecorderCommand::Add(Box::new(session))),
        }
    }

    /// Abort every session reading this subscription
    /// (`ArchiveConductor.abortRecordingSessionAndCloseSubscription`, `:1766-1772`).
    pub fn abort_sessions_for(&mut self, subscription_id: i64, reason: &str) {
        match self {
            Self::Inline(recorder) => recorder.abort_sessions_for(subscription_id, reason),
            Self::Threaded(thread) => thread.send(RecorderCommand::AbortSessionsFor {
                subscription_id,
                reason: reason.to_owned(),
            }),
        }
    }

    /// Abort **one** session, by the recording it is making
    /// (`ArchiveConductor.stopRecordingByIdentity`, `:1306-1310`).
    pub fn abort_session(&mut self, recording_id: i64, reason: &str) {
        match self {
            Self::Inline(recorder) => recorder.abort_session(recording_id, reason),
            Self::Threaded(thread) => thread.send(RecorderCommand::AbortSession {
                recording_id,
                reason: reason.to_owned(),
            }),
        }
    }

    /// Ask for the three counters and take each up when the driver answers
    /// (`Archive.java:1587-1626`).
    ///
    /// The ask is made **here** whichever arm this is: it is a client operation,
    /// and the client is the conductor's. The threaded arm sends what the three
    /// turn out to be; see [`RecorderCountersRequest`].
    pub fn allocate(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: std::time::Duration,
    ) {
        match self {
            Self::Inline(recorder) => recorder.allocate(client, counters, timeout),
            Self::Threaded(thread) => thread.allocate(client, counters, timeout),
        }
    }

    /// One turn of every session (`SessionWorker.doWork` over
    /// `RecordingSession.doWork`).
    ///
    /// The threaded arm does no work here and says so: its recorder is driven by
    /// its own thread, and what the conductor owes that turn is collecting what
    /// finished, which is [`RecorderHalf::take_finished`]. What it *can* do is
    /// hear that the thread is gone — see [`RecorderThread::failure`].
    ///
    /// # Errors
    ///
    /// [`EventsError`], from the sessions this turn or from the worker's own
    /// thread. Both mean the same thing and the reference ends both the same
    /// way: the agent driving the sessions is finished.
    pub fn drive(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
    ) -> Result<usize, EventsError> {
        match self {
            Self::Inline(recorder) => recorder.drive(counters),
            Self::Threaded(thread) => match thread.failure() {
                Some(error) => Err(error),
                None => Ok(0),
            },
        }
    }

    /// Hand the recording events publication over
    /// (`ArchiveConductor.java:224-225`).
    ///
    /// The inline arm keeps it, because its recorder is this object; the
    /// threaded arm sends it, for the reason [`RecorderCommand::SetCounters`]
    /// exists: the publication is built where the client is — a client command
    /// the driver answers on a turn — and this is how it reaches the thread that
    /// will write to it.
    pub fn set_events(&mut self, events: RecordingEventsProxy) {
        match self {
            Self::Inline(recorder) => recorder.set_events(events),
            Self::Threaded(thread) => thread.send(RecorderCommand::SetEvents(Box::new(events))),
        }
    }

    /// The sessions that finished, for the conductor to close.
    pub fn take_finished(&mut self) -> Vec<RecordingSession> {
        match self {
            Self::Inline(recorder) => recorder.take_finished(),
            Self::Threaded(thread) => thread.take_finished(),
        }
    }
}

/// What the conductor tells a recorder that is on a thread of its own.
///
/// The two `abort*` arms carry a `String` rather than a `&str` because the
/// reason has to outlive the call: the conductor writes it and the worker reads
/// it, on another thread, an unspecified time later.
#[derive(Debug)]
pub enum RecorderCommand {
    /// `recorder.addSession` (`ArchiveConductor.java:2055`).
    ///
    /// Boxed because a session is by far the largest thing on this channel —
    /// every other arm is two machine words and a `String` — and the conductor's
    /// turn would otherwise carry the biggest variant's size on every send.
    Add(Box<RecordingSession>),
    /// `recorder.abortSession` (`:1306-1310`), by the recording being made.
    AbortSession {
        /// The recording whose session is to stop.
        recording_id: i64,
        /// Why, in the reference's words.
        reason: String,
    },
    /// `recorder.abortSessionsFor` (`:1766-1772`), by the subscription read.
    AbortSessionsFor {
        /// The subscription whose sessions are to stop.
        subscription_id: i64,
        /// Why, in the reference's words.
        reason: String,
    },
    /// The three counters, once the driver has answered about all three.
    SetCounters(RecorderCounters),
    /// The recording events publication, once the conductor has built one.
    ///
    /// Boxed for the same reason [`RecorderCommand::Add`] is, if less sharply:
    /// it carries a mapped log buffer, where every other arm but `Add` is a few
    /// machine words.
    SetEvents(Box<RecordingEventsProxy>),
}

/// The conductor's end of a recorder that is on a thread of its own.
///
/// Two channels, which is the reference's shape: its `DedicatedModeArchiveConductor`
/// holds a `ManyToOneConcurrentLinkedQueue<Session> closeQueue` for what the
/// workers hand back (`:35`) and each worker reads its own commands off the same
/// queue its sessions arrived on (`ArchiveConductor.java:371-378`). What is
/// here instead is [`std::sync::mpsc`] in both directions, which ADR-0003 leaves
/// open: it forbids an async runtime, not a channel with a lock in it, and the
/// reference's own queue is lock-free for a throughput this loop does not have —
/// it carries one message per recording, not one per frame.
#[derive(Debug)]
pub struct RecorderThread {
    commands: std::sync::mpsc::Sender<RecorderCommand>,
    finished: std::sync::mpsc::Receiver<RecordingSession>,
    /// Why the recorder's thread ended, if it ended badly. See
    /// [`RecorderThread::failure`].
    failure: std::sync::mpsc::Receiver<EventsError>,
    /// The one it reported, kept so that every later turn answers the same
    /// thing: a thread that has ended does not come back, and the channel
    /// answers `Disconnected` once its sender is gone.
    failed: Option<EventsError>,
    request: RecorderCountersRequest,
    /// Whether the three have been handed over, so that a later turn does not
    /// ask again — the reference allocates them once (`Archive.java:1587-1626`).
    allocated: bool,
}

impl RecorderThread {
    /// The two ends: what the conductor keeps, and the body to run on the
    /// recorder's own thread.
    ///
    /// # Errors
    ///
    /// [`RecorderWorker::new`]'s, when the counter region will not open.
    pub fn pair(
        archive_id: i64,
        aeron_dir: &std::path::Path,
        idle: deepmsg_driver::idle::Strategy,
        timeout: std::time::Duration,
    ) -> Result<(Self, RecorderWorker), String> {
        let (commands, from_conductor) = std::sync::mpsc::channel();
        let (to_conductor, finished) = std::sync::mpsc::channel();
        // A third channel, which the reference does not have: its `AgentRunner`
        // hands the throw to the archive's error handler and the conductor is
        // never told. Here the conductor is what stops the archive, so it has to
        // hear — see `RecorderWorker::run`.
        let (failures, failure) = std::sync::mpsc::channel();

        Ok((
            Self {
                commands,
                finished,
                failure,
                failed: None,
                request: RecorderCountersRequest::new(archive_id),
                allocated: false,
            },
            RecorderWorker::new(
                archive_id,
                aeron_dir,
                idle,
                timeout,
                from_conductor,
                to_conductor,
                failures,
            )?,
        ))
    }

    /// Why the recorder's thread ended, if it ended badly.
    ///
    /// `Some` once and for ever after: the worker reports one failure and then
    /// returns, so a conductor that asks again must not be told `None` — that
    /// would read as a recorder that is merely idle.
    fn failure(&mut self) -> Option<EventsError> {
        if self.failed.is_none() {
            self.failed = self.failure.try_iter().next();
        }

        self.failed
    }

    /// Tell the recorder something. A send that fails is a recorder thread that
    /// has stopped, which is not something a control request can do anything
    /// about: the archive is on its way down.
    fn send(&mut self, command: RecorderCommand) {
        let _ = self.commands.send(command);
    }

    /// The ask for the three counters, made where the client is, and handed over
    /// the turn they are all in hand.
    fn allocate(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: std::time::Duration,
    ) {
        if self.allocated {
            return;
        }

        let Some(counters) = self.request.drive(client, counters, timeout) else {
            return;
        };

        self.allocated = true;
        self.send(RecorderCommand::SetCounters(counters));
    }

    /// What the recorder has finished with since the last turn.
    fn take_finished(&mut self) -> Vec<RecordingSession> {
        self.finished.try_iter().collect()
    }
}

/// The body a recorder runs when it is on a thread of its own.
///
/// # What it owns, and what it therefore may touch
///
/// The [`Recorder`] — so every [`RecordingSession`], and through it every
/// session's own image, writer and position counter (P2-9a's ③-1). It does
/// **not** own the client, and must not: the reference's rule is that one
/// `Aeron` has one thread driving it (`ArchiveConductor.doWork` calls
/// `invokeAeronInvoker`, `:380`, and a worker's `doWork` does not). What it
/// needs from the region instead it opens for itself — a second mapping of the
/// same files, which is what the reference's `CountersReader` makes from
/// `aeronDir` in each agent's constructor.
#[derive(Debug)]
pub struct RecorderWorker {
    recorder: Recorder,
    cnc: deepmsg_cnc::CncFile,
    idle: deepmsg_driver::idle::Strategy,
    /// What the conductor has told it, until the conductor is gone.
    commands: std::sync::mpsc::Receiver<RecorderCommand>,
    /// What it has finished with, back to the conductor.
    finished: std::sync::mpsc::Sender<RecordingSession>,
    /// Why it is giving up, back to the conductor, if it ever does.
    failures: std::sync::mpsc::Sender<EventsError>,
}

impl RecorderWorker {
    /// A worker with a recorder of its own, over a counter region it opens
    /// itself.
    ///
    /// # Errors
    ///
    /// The message from [`deepmsg_cnc::CncFile::open_writable`] when the region
    /// will not open — the same reason the conductor cannot run without one.
    pub fn new(
        archive_id: i64,
        aeron_dir: &std::path::Path,
        idle: deepmsg_driver::idle::Strategy,
        timeout: std::time::Duration,
        commands: std::sync::mpsc::Receiver<RecorderCommand>,
        finished: std::sync::mpsc::Sender<RecordingSession>,
        failures: std::sync::mpsc::Sender<EventsError>,
    ) -> Result<Self, String> {
        let cnc = deepmsg_cnc::CncFile::open_writable(aeron_dir, timeout)
            .map_err(|error| format!("the recorder could not open the counter region: {error}"))?;

        Ok(Self {
            recorder: Recorder::new(archive_id),
            cnc,
            idle,
            commands,
            finished,
            failures,
        })
    }

    /// The recorder's own loop: what the conductor said, then a turn of every
    /// session, then what finished back to the conductor.
    ///
    /// It ends when the conductor's end of the command channel is **gone** —
    /// when the archive's conductor has been dropped, which is the archive
    /// stopping. That is the whole of the stop condition: a flag would be a
    /// second thing to keep in step with the first, and the reference's workers
    /// stop the same way (`AgentRunner.run` ends when `isRunning` is cleared by
    /// the runner's close, which the conductor does on its way down,
    /// `DedicatedModeArchiveConductor.closeSessionWorkers:70-71`).
    ///
    /// ★ **And it ends the other way too**: a turn whose events publication is
    /// finished stops the loop and reports. The reference's throw out of
    /// `RecordingSession.doWork` leaves `AgentRunner.run`, which hands it to the
    /// archive's error handler and lets the thread die — so "the recorder agent
    /// is over" is the reference's own answer, and this is what that answer is
    /// here. What follows it differs, and
    /// [`ArchiveConductor::do_work`](crate::server::conductor::ArchiveConductor)
    /// is where that is written down.
    pub fn run(mut self) {
        loop {
            let mut work = 0;

            loop {
                match self.commands.try_recv() {
                    Ok(RecorderCommand::Add(session)) => {
                        self.recorder.add_session(*session);
                        work += 1;
                    }
                    Ok(RecorderCommand::AbortSession {
                        recording_id,
                        reason,
                    }) => {
                        self.recorder.abort_session(recording_id, &reason);
                        work += 1;
                    }
                    Ok(RecorderCommand::AbortSessionsFor {
                        subscription_id,
                        reason,
                    }) => {
                        self.recorder.abort_sessions_for(subscription_id, &reason);
                        work += 1;
                    }
                    Ok(RecorderCommand::SetCounters(counters)) => {
                        self.recorder.set_counters(counters);
                        work += 1;
                    }
                    Ok(RecorderCommand::SetEvents(events)) => {
                        self.recorder.set_events(*events);
                        work += 1;
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                }
            }

            // Read-only or not-yet-published CnC is not a reason to stop: the
            // region is opened when this thread starts and the conductor may
            // still be the one publishing it. What the sessions need is the
            // three write counters, which arrive as a command.
            let Some(counters) = self.cnc.counters_writable() else {
                self.idle.idle(work);
                continue;
            };

            match self.recorder.drive(&counters) {
                Ok(worked) => work += worked,
                Err(error) => {
                    // The whole of what an events failure does here: the turn
                    // stops, the thread ends, and the conductor is told so that
                    // it can stop the archive. A send that fails is a publication
                    // that will never take another event, so there is nothing to
                    // carry on for.
                    let _ = self.failures.send(error);

                    return;
                }
            }

            for session in self.recorder.take_finished() {
                // The conductor is gone: nothing will close this session, and
                // there is no one left to tell.
                if self.finished.send(session).is_err() {
                    return;
                }
            }

            self.idle.idle(work);
        }
    }
}

impl Recorder {
    /// A recorder with nothing in it, for an archive with this id.
    ///
    /// The id is what the three counters are keyed by — the reference's
    /// `ArchiveCounters.allocate` writes it into the key and into the label's
    /// suffix — so a recorder has to know it before it can count.
    #[must_use]
    pub const fn new(archive_id: i64) -> Self {
        Self {
            sessions: Vec::new(),
            finished: Vec::new(),
            totals: RecorderTotals {
                total_write_bytes: 0,
                total_write_time_ns: 0,
                max_write_time_ns: 0,
            },
            counters: None,
            request: RecorderCountersRequest::new(archive_id),
            events: None,
        }
    }

    /// The recording events publication, handed over once the conductor has one
    /// (`aeron.archive.recording.events.enabled`).
    pub fn set_events(&mut self, events: RecordingEventsProxy) {
        self.events = Some(events);
    }

    /// Ask for the three counters, one at a time, and take each up when the
    /// driver answers (`Archive.java:1587-1626`).
    ///
    /// The reference does all three in `Archive.Context.conclude` — at startup,
    /// synchronously, because its archive drives the driver. This build cannot:
    /// it runs inside the turn that drives the driver, so each add goes out on
    /// one turn and its answer is read on a later one (the same two steps the
    /// control-session counters take, `Sessions::allocate_session_counter`).
    ///
    /// One at a time rather than three at once, because that keeps the state one
    /// registration id instead of three and the order is the reference's anyway.
    /// A refusal is not retried: a counter the driver will not make can never
    /// become one, and a recorder with no counters still records.
    pub fn allocate(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: std::time::Duration,
    ) {
        if self.counters.is_some() {
            return;
        }

        self.counters = self.request.drive(client, counters, timeout);
    }

    /// `recorder.addSession` (`ArchiveConductor.java:2055`).
    pub fn add_session(&mut self, session: RecordingSession) {
        self.sessions.push(session);
    }

    /// The counters, once the conductor has allocated them.
    pub fn set_counters(&mut self, counters: RecorderCounters) {
        self.counters = counters.into();
    }

    /// How many recordings are in flight.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// What every recording has added up to, for a reader that wants the
    /// numbers before a counter has been published to.
    #[must_use]
    pub const fn totals(&self) -> RecorderTotals {
        self.totals
    }

    /// Abort every session reading this subscription
    /// (`ArchiveConductor.abortRecordingSessionAndCloseSubscription`,
    /// `:1766-1772`).
    ///
    /// The reference walks **the map of sessions**, not the subscription's own
    /// list, and aborts the ones that are reading it. A session that has
    /// finished is not one of them: its recording has already stopped, and what
    /// is left for it is the closing, which the conductor does when it collects
    /// it.
    pub fn abort_sessions_for(&mut self, subscription_id: i64, reason: &str) {
        for session in &mut self.sessions {
            if session.subscription_id() == subscription_id {
                session.abort(reason);
            }
        }
    }

    /// Abort **one** session, by the recording it is making
    /// (`ArchiveConductor.stopRecordingByIdentity`, `:1306-1310`).
    ///
    /// Not [`Recorder::abort_sessions_for`]: a subscription can carry more than
    /// one image over its life, so "the session for this recording" and "every
    /// session reading this subscription" are different sets, and a stop named
    /// by identity is about one recording.
    pub fn abort_session(&mut self, recording_id: i64, reason: &str) {
        for session in &mut self.sessions {
            if session.recording_id() == recording_id {
                session.abort(reason);
            }
        }
    }

    /// One turn of every session (`SessionWorker.doWork` over
    /// `RecordingSession.doWork`).
    ///
    /// The sessions are driven first and removed afterwards, which is the
    /// worker's own order (`SessionWorker.java:56-81`) — and the reason a
    /// session that finished this turn still gets its turn.
    ///
    /// **No client** (P2-9a): a session holds its own image and takes its
    /// answers from it, so this is the same call the reference's recorder makes
    /// — `SessionWorker.doWork`, with nothing of the `Aeron` client in it —
    /// which is what lets this run on a thread of its own.
    ///
    /// # Errors
    ///
    /// [`EventsError`] from the first session that has one, and the turn **stops
    /// there**: the reference's throw leaves `SessionWorker.doWork` at once, so
    /// the sessions after it get no turn either. See [`RecorderWorker::run`] and
    /// [`ArchiveConductor::do_work`](crate::server::conductor::ArchiveConductor)
    /// for what becomes of it — the events publication is finished and no
    /// recording will ever report again.
    pub fn drive(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
    ) -> Result<usize, EventsError> {
        let mut work = 0;

        {
            let totals = &mut self.totals;
            let events = &mut self.events;

            for session in &mut self.sessions {
                work += session.do_work(
                    counters,
                    totals,
                    events
                        .as_mut()
                        .map(|events| events as &mut dyn RecordingEvents),
                )?;
            }
        }

        let mut index = 0;
        while index < self.sessions.len() {
            if self.sessions[index].is_done() {
                self.finished.push(self.sessions.swap_remove(index));
            } else {
                index += 1;
            }
        }

        self.publish_if(counters, work);

        Ok(work)
    }

    /// The sessions that finished, for the conductor to close
    /// (`SessionWorker.closeSession`, `:73-76`, which a `SharedModeRecorder`
    /// sends to `closeRecordingSession`).
    pub fn take_finished(&mut self) -> Vec<RecordingSession> {
        self.finished.drain(..).collect()
    }

    /// Publish the three totals — but only on a turn that did work
    /// (`ArchiveConductor.java:2732-2743`).
    ///
    /// `workCount > 0` is the whole of the condition, and it is a parameter here
    /// rather than a test of the totals because that is what the reference
    /// tests. A turn that read nothing is a turn whose numbers have not moved.
    fn publish_if(&self, counters: &CountersReader<'_, ReadWrite>, work: usize) {
        if 0 == work {
            return;
        }

        let Some(published) = &self.counters else {
            return;
        };

        let (total_write_bytes, total_write_time_ns, max_write_time_ns) = self.totals.totals();

        published.total_write_bytes.set(counters, total_write_bytes);
        published
            .total_write_time
            .set(counters, total_write_time_ns);
        published.max_write_time.set(counters, max_write_time_ns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::server::recording_pos::RecordingPos;

    use deepmsg_core::buffer::AtomicBuffer;

    /// A values region with room for the three counters, leaked so it can be
    /// handed to a call.
    ///
    /// The same helper `control_session`'s and `conductor`'s tests keep their
    /// own copy of, for the same reason: it is ten lines of alignment and the
    /// alternative is a shared test module for three callers.
    fn values_region() -> CountersReader<'static, ReadWrite> {
        #[repr(align(64))]
        struct Region([u8; 64 * 64]);

        let metadata: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));
        let values: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));

        CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut metadata.0).expect("aligned region"),
            AtomicBuffer::from_slice_mut(&mut values.0).expect("aligned region"),
        )
    }

    /// A recorder with the three counters pointing at the first three slots of
    /// a region of its own.
    fn recorder_with_counters() -> (Recorder, CountersReader<'static, ReadWrite>) {
        let counters = values_region();
        let mut recorder = Recorder::new(42);

        recorder.set_counters(RecorderCounters {
            max_write_time: ArchiveIdCounter::for_test(0),
            total_write_bytes: ArchiveIdCounter::for_test(1),
            total_write_time: ArchiveIdCounter::for_test(2),
        });

        (recorder, counters)
    }

    /// The three totals, and the one of them that is a maximum: a shorter write
    /// after a longer one leaves the longest where it was
    /// (`ArchiveConductor.java:2717-2730`).
    #[test]
    fn the_totals_add_up_and_the_longest_write_is_kept() {
        let mut totals = RecorderTotals::default();

        totals.bytes_written(128);
        totals.write_time_ns(1_000);
        totals.bytes_written(64);
        totals.write_time_ns(500);

        assert_eq!((192, 1_500, 1_000), totals.totals());

        totals.write_time_ns(4_000);

        assert_eq!(
            (192, 5_500, 4_000),
            totals.totals(),
            "the max follows the longest, not the last"
        );
    }

    /// The three counters are published on a turn that did work and **not** on
    /// one that did not (`ArchiveConductor.java:2732-2743`).
    ///
    /// What the condition is about is when a reader sees the numbers, so the
    /// quiet turn is asserted *after* a busy one and against the numbers that
    /// turn wrote: a slot that still reads what it read before is a slot the
    /// quiet turn left alone. (An unwritten slot reads zero here rather than
    /// "absent", because the metadata half of this region describes no
    /// counters — see the helper.)
    #[test]
    fn the_counters_are_published_only_on_a_turn_that_did_work() {
        let (mut recorder, counters) = recorder_with_counters();

        recorder.totals.bytes_written(4_096);
        recorder.totals.write_time_ns(7_000);
        recorder.totals.write_time_ns(9_000);

        recorder.publish_if(&counters, 1);

        assert_eq!(Some(9_000), counters.value(0), "105 is the longest write");
        assert_eq!(Some(4_096), counters.value(1));
        assert_eq!(Some(16_000), counters.value(2));

        recorder.totals.bytes_written(1_024);
        recorder.publish_if(&counters, 0);

        assert_eq!(Some(9_000), counters.value(0));
        assert_eq!(
            Some(4_096),
            counters.value(1),
            "a quiet turn writes nothing"
        );
        assert_eq!(Some(16_000), counters.value(2));

        recorder.publish_if(&counters, 2);

        assert_eq!(
            Some(5_120),
            counters.value(1),
            "and the next one catches up"
        );
    }

    /// A stop **by identity** aborts the recording it names and leaves the
    /// other sessions alone, even the ones reading the same subscription
    /// (`ArchiveConductor.stopRecordingByIdentity`, `:1306-1310`).
    ///
    /// `abort_sessions_for` is the subscription's question
    /// (`abortRecordingSessionAndCloseSubscription`, `:1766-1774`); this is the
    /// recording's, and a subscription can carry more than one image over its
    /// life, so the two sets are not the same.
    #[test]
    fn a_stop_by_identity_aborts_one_recording_and_not_its_subscription() {
        let dir = crate::mark::tests::TempDir::new();
        let mut recorder = Recorder::new(42);

        // Two recordings on one subscription — the shape a recording whose
        // publisher restarted leaves behind for a turn.
        recorder.add_session(a_session(dir.path(), 0, 9));
        recorder.add_session(a_session(dir.path(), 1, 9));
        assert_eq!(2, recorder.session_count());

        recorder.abort_session(0, "stop recording by identity");

        assert!(
            recorder.sessions[0].abort_reason().is_some(),
            "the recording that was named is aborted"
        );
        assert!(
            recorder.sessions[1].abort_reason().is_none(),
            "and the other recording on the same subscription is not"
        );

        recorder.abort_sessions_for(9, "stop recording");

        assert!(
            recorder.sessions[1].abort_reason().is_some(),
            "while the subscription's own question aborts what is left on it"
        );
    }

    /// One session, made without a client: enough of one for the two aborts
    /// above, and nothing that opens a file.
    fn a_session(
        directory: &std::path::Path,
        recording_id: i64,
        subscription_id: i64,
    ) -> RecordingSession {
        RecordingSession::new(
            3,
            7,
            recording_id,
            recording_id * 1024,
            recording_id * 1024,
            128 * 1024,
            subscription_id,
            Box::new(crate::server::recording_session::FakeImage::new()),
            12,
            1001,
            "aeron:ipc".to_owned(),
            "aeron:ipc".to_owned(),
            1024 * 1024,
            0,
            false,
            RecordingPos::for_test(1),
            directory,
            None,
        )
    }

    /// A recorder with no counters still records: the numbers are accumulated
    /// and simply never published, which is what an archive that never
    /// allocated them does (`Archive.java:1586-1626` allocates on demand).
    #[test]
    fn a_recorder_with_no_counters_publishes_nothing() {
        let counters = values_region();
        let mut recorder = Recorder::new(42);

        recorder.totals.bytes_written(8);

        recorder.publish_if(&counters, 1);

        assert_eq!(Some(0), counters.value(0), "nothing was written here");
        assert_eq!(Some(0), counters.value(1));
        assert_eq!(Some(0), counters.value(2));
        assert_eq!((8, 0, 0), recorder.totals().totals(), "but it was counted");
    }

    /// The three counters are the reference's three, by **type id and label**:
    /// what `AeronStat` reads to tell a 105 from a 106, and what a comparison
    /// against the reference's own reader turns on (`Archive.java:1587-1626`).
    #[test]
    fn the_three_counters_are_the_references_three() {
        assert_eq!(
            105,
            RecorderCounter::MaxWriteTime.type_id(),
            "AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID"
        );
        assert_eq!(106, RecorderCounter::TotalWriteBytes.type_id());
        assert_eq!(107, RecorderCounter::TotalWriteTime.type_id());

        assert_eq!(
            "archive-recorder max write time in ns",
            RecorderCounter::MaxWriteTime.name()
        );
        assert_eq!(
            "archive-recorder total write bytes",
            RecorderCounter::TotalWriteBytes.name()
        );
        assert_eq!(
            "archive-recorder total write time in ns",
            RecorderCounter::TotalWriteTime.name()
        );

        // And the label the reader sees is that name with the archive id's
        // suffix, which is the one thing about these counters that a reader has
        // to agree with us about (`ArchiveCounters.java:35`, `:52-69`).
        assert_eq!(
            "archive-recorder total write bytes - archiveId=42",
            crate::server::counters::archive_id_label(RecorderCounter::TotalWriteBytes.name(), 42)
        );

        assert_eq!(
            [
                RecorderCounter::MaxWriteTime,
                RecorderCounter::TotalWriteBytes,
                RecorderCounter::TotalWriteTime
            ],
            RecorderCounter::ORDER,
            "and they are asked for in the reference's order (`:1587-1626`)"
        );
    }

    /// A recorder is empty until a session is added, and a session that has
    /// finished is handed back rather than closed
    /// (`SessionWorker.java:56-81`).
    #[test]
    fn an_empty_recorder_hands_back_nothing() {
        let mut recorder = Recorder::new(42);

        assert!(recorder.is_empty());
        assert_eq!(0, recorder.session_count());
        assert!(recorder.take_finished().is_empty());
        assert_eq!(RecorderTotals::default(), recorder.totals());
    }

    /// ★ A recorder whose thread has reported a failure reports it on **every**
    /// turn afterwards, not just the one it arrived on.
    ///
    /// The mutation this pins is `try_recv().ok()`: the worker's sender is gone
    /// by the time a conductor asks a second time, so the channel answers
    /// `Disconnected` and the failure would read as a recorder that is merely
    /// idle. What that would cost is the archive running on for ever with
    /// nothing recording.
    #[test]
    fn a_recorder_thread_reports_its_failure_once_and_for_ever() {
        let (commands, _from_conductor) = std::sync::mpsc::channel();
        let (_to_conductor, finished) = std::sync::mpsc::channel();
        let (failures, failure) = std::sync::mpsc::channel();

        let mut thread = RecorderThread {
            commands,
            finished,
            failure,
            failed: None,
            request: RecorderCountersRequest::new(42),
            allocated: false,
        };

        assert_eq!(None, thread.failure(), "nothing has gone wrong yet");

        failures.send(exhausted()).expect("the report is sent");
        drop(failures);

        assert_eq!(Some(exhausted()), thread.failure());
        assert_eq!(
            Some(exhausted()),
            thread.failure(),
            "and the same answer after the channel itself has gone"
        );
    }

    /// The conductor's end of a threaded recorder: no failure is no work, and a
    /// failure is the turn's error — which is what stops the archive.
    ///
    /// The inline arm is a [`Recorder`]'s own `drive`, whose `?` over
    /// [`RecordingSession::do_work`] is pinned where the session is
    /// (`recording_session.rs`): the failure can only come from an events
    /// publication, and a publication is not something a test can make without a
    /// mapped log.
    #[test]
    fn a_threaded_half_answers_with_its_workers_failure() {
        let (commands, _from_conductor) = std::sync::mpsc::channel();
        let (_to_conductor, finished) = std::sync::mpsc::channel();
        let (failures, failure) = std::sync::mpsc::channel();

        let mut half = RecorderHalf::Threaded(RecorderThread {
            commands,
            finished,
            failure,
            failed: None,
            request: RecorderCountersRequest::new(42),
            allocated: false,
        });

        assert_eq!(Ok(0), half.drive(&values_region()));
        assert_eq!(
            Ok(0),
            half.drive(&values_region()),
            "a worker that is idle is 0"
        );
        assert_eq!(0, half.take_finished().len());

        failures.send(exhausted()).expect("the report is sent");

        assert_eq!(Err(exhausted()), half.drive(&values_region()));
    }

    /// The one error an events publication can answer with
    /// (`RecordingEventsProxy:139-142`).
    fn exhausted() -> EventsError {
        EventsError::Exhausted {
            term_length: 64 * 1024,
        }
    }
}
