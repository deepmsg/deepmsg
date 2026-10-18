//! The replayer's counters: how many replays are open, and what the reads cost.
//!
//! Four of the archive's, and the reference allocates them together
//! (`Archive.java:1578-1659`): **112** `Archive Replay Sessions`, which the
//! conductor moves as sessions open and close (`:993`, `:1382`), and the
//! replayer's three statistics — **108** the longest single read, **109** the
//! bytes read, **110** the nanoseconds spent reading — which the `Replayer`
//! worker publishes once per turn that did work (`:2745-2791`).
//!
//! # The two halves live in different places, on purpose
//!
//! In the reference all four belong to `ArchiveConductor.Replayer`, because
//! that worker is what drives the replay sessions. In this build the sessions
//! are [`Sessions`](crate::server::conductor::Sessions)' — they are driven in
//! the conductor's own turn rather than by a worker of their own (P2-9a is
//! where a replayer thread would arrive) — so what is here is the part that is
//! the replayer's whatever drives it: the four counters, the running totals, and
//! the rule that a turn which read nothing publishes nothing.

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::mark::NULL_VALUE;
use crate::server::create_replay_publication::{LimitCounter, read_limit_counter};
use crate::server::replay_session::{Progress, Publication, ReplaySession, State};

use crate::server::counters::{
    ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
    ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
    ArchiveIdCounter, CounterAllocator, CounterKind, DEDICATED_MODE, DutyCycleActor,
    DutyCycleCounters, REPLAY_SESSIONS_NAME, REPLAYER_MAX_READ_TIME_NAME,
    REPLAYER_TOTAL_READ_BYTES_NAME, REPLAYER_TOTAL_READ_TIME_NAME,
};

/// One of the replayer's four counters.
///
/// The order is `Archive.Context.conclude`'s (`Archive.java:1578-1659`), which
/// allocates 112, then the recorder's three, then these — the recorder's own
/// block is the other [`CounterAllocator`], and the two blocks are independent
/// of each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayerCounter {
    /// 112: how many replay sessions are open right now.
    ReplaySessions,
    /// 108: the longest single read, in nanoseconds.
    MaxReadTime,
    /// 109: every byte every replay has read.
    TotalReadBytes,
    /// 110: every nanosecond those reads took.
    TotalReadTime,
}

impl CounterKind for ReplayerCounter {
    const ORDER: &'static [Self] = &[
        Self::ReplaySessions,
        Self::MaxReadTime,
        Self::TotalReadBytes,
        Self::TotalReadTime,
    ];

    /// `AeronCounters`' own ids (`:792`, `:799`, `:806`, `:818`).
    fn type_id(self) -> i32 {
        match self {
            Self::ReplaySessions => ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID,
            Self::MaxReadTime => ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
            Self::TotalReadBytes => ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID,
            Self::TotalReadTime => ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
        }
    }

    /// The names their labels are built from (`Archive.java:1581`, `:1635`,
    /// `:1646`, `:1657`).
    fn name(self) -> &'static str {
        match self {
            Self::ReplaySessions => REPLAY_SESSIONS_NAME,
            Self::MaxReadTime => REPLAYER_MAX_READ_TIME_NAME,
            Self::TotalReadBytes => REPLAYER_TOTAL_READ_BYTES_NAME,
            Self::TotalReadTime => REPLAYER_TOTAL_READ_TIME_NAME,
        }
    }
}

/// The four, in hand.
///
/// `pub(crate)` fields inside a crate-private module: nothing outside the
/// archive's own loop moves them.
#[derive(Debug)]
pub struct ReplayerCounters {
    pub(crate) replay_sessions: ArchiveIdCounter,
    pub(crate) max_read_time: ArchiveIdCounter,
    pub(crate) total_read_bytes: ArchiveIdCounter,
    pub(crate) total_read_time: ArchiveIdCounter,
}

/// What the reads have added up to (`ArchiveConductor.java:2747-2749`).
///
/// `i64` and not `u64`, for the reason [`RecorderTotals`] gives: the
/// reference's three are `long`s, and they are published into counters read as
/// `long`s.
///
/// [`RecorderTotals`]: crate::server::recorder::RecorderTotals
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadTotals {
    total_read_bytes: i64,
    total_read_time_ns: i64,
    max_read_time_ns: i64,
}

impl ReadTotals {
    /// `Replayer.bytesRead` and `Replayer.readTimeNs` (`:2764-2777`), which a
    /// session calls once per read it did.
    ///
    /// The maximum is of a **single** read, so a short read after a long one
    /// leaves it where it was (`:2769-2772`).
    pub fn read(&mut self, bytes: u64, time_ns: u64) {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        let time_ns = i64::try_from(time_ns).unwrap_or(i64::MAX);

        self.total_read_bytes = self.total_read_bytes.saturating_add(bytes);
        self.total_read_time_ns = self.total_read_time_ns.saturating_add(time_ns);

        if time_ns > self.max_read_time_ns {
            self.max_read_time_ns = time_ns;
        }
    }

    /// The bytes, the nanoseconds, and the longest single read (`:2747-2749`).
    #[must_use]
    pub const fn totals(&self) -> (i64, i64, i64) {
        (
            self.total_read_bytes,
            self.total_read_time_ns,
            self.max_read_time_ns,
        )
    }
}

/// `ArchiveConductor.Replayer` (`:2745-2791`), as far as its counters go: the
/// four, the totals, and the rule for publishing them.
#[derive(Debug)]
pub struct Replayer {
    /// `None` until the four have been allocated — and a replayer that never
    /// allocated them still replays, it just does not count.
    counters: Option<ReplayerCounters>,
    allocator: CounterAllocator<ReplayerCounter>,
    totals: ReadTotals,
}

impl Replayer {
    /// A replayer for an archive with this id, which is what the four are keyed
    /// by.
    #[must_use]
    pub const fn new(archive_id: i64) -> Self {
        Self {
            counters: None,
            allocator: CounterAllocator::new(archive_id),
            totals: ReadTotals {
                total_read_bytes: 0,
                total_read_time_ns: 0,
                max_read_time_ns: 0,
            },
        }
    }

    /// Ask for the four, one at a time, and take each up when the driver
    /// answers (`Archive.java:1578-1659`).
    pub fn allocate(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: Duration,
    ) {
        if self.counters.is_some() {
            return;
        }

        self.allocator.drive(client, counters, timeout);

        if !self.allocator.is_complete() {
            return;
        }

        let take = |kind| self.allocator.counter(kind).expect("all four are in hand");

        self.counters = Some(ReplayerCounters {
            replay_sessions: take(ReplayerCounter::ReplaySessions),
            max_read_time: take(ReplayerCounter::MaxReadTime),
            total_read_bytes: take(ReplayerCounter::TotalReadBytes),
            total_read_time: take(ReplayerCounter::TotalReadTime),
        });
    }

    /// `Replayer.bytesRead` + `readTimeNs`, for one session's read.
    pub fn read(&mut self, bytes: u64, time_ns: u64) {
        self.totals.read(bytes, time_ns);
    }

    /// Publish the three statistics — but only on a turn that did work
    /// (`:2779-2790`).
    ///
    /// `workCount > 0` is the whole of the condition, and it is a parameter here
    /// rather than a test of the totals because that is what the reference
    /// tests. A turn that read nothing is a turn whose numbers have not moved.
    pub fn publish_if(&self, counters: &CountersReader<'_, ReadWrite>, work: usize) {
        if 0 == work {
            return;
        }

        let Some(published) = &self.counters else {
            return;
        };

        let (total_read_bytes, total_read_time_ns, max_read_time_ns) = self.totals.totals();

        published.total_read_bytes.set(counters, total_read_bytes);
        published.total_read_time.set(counters, total_read_time_ns);
        published.max_read_time.set(counters, max_read_time_ns);
    }

    /// `ctx.replaySessionCounter().incrementRelease()` (`:993`), which the
    /// reference does where a replay session is put in the map.
    pub fn session_opened(&self, counters: &CountersReader<'_, ReadWrite>) {
        if let Some(published) = &self.counters {
            published.replay_sessions.increment(counters);
        }
    }

    /// `decrementRelease` (`:1382`), where one is closed.
    pub fn session_closed(&self, counters: &CountersReader<'_, ReadWrite>) {
        if let Some(published) = &self.counters {
            published.replay_sessions.decrement(counters);
        }
    }

    /// The totals, for a caller that wants to check them — the reference's own
    /// three fields, read back.
    #[must_use]
    pub const fn totals(&self) -> (i64, i64, i64) {
        self.totals.totals()
    }
}

/// What one turn reads about a replay's limit, per replay in flight.
///
/// The two arms are `ReplaySession.notExtended`'s (`ReplaySession.java:561-577`),
/// and the difference between them is the whole of how a bounded replay ends:
/// [`LimitUpdate::ExtendTo`] moves the stop position forward and nothing else,
/// and [`LimitUpdate::Gone`] clamps what the replay may still send to where it
/// has already got to — which is what takes it to `INACTIVE`.
///
/// ★ **Which one it is depends on where the number lives.** A counter in the
/// driver's region is read by the replayer itself ([`LimitUpdate::Follow`]),
/// which is the reference's shape and saves a conductor turn on every message;
/// a position the **conductor alone** can see — its own `recordingSessionByIdMap`
/// for a recording whose session has not published a counter yet — still
/// crosses as [`LimitUpdate::ExtendTo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitUpdate {
    /// The counter, or the live recording, says this.
    ExtendTo(i64),
    /// Read this counter in every turn, from here on: the reference's
    /// `limitPosition` (`ReplaySession.java:374`, `:546-578`).
    Follow(LimitCounter),
    /// There is nothing left to read: the counter was closed, or its slot has
    /// been taken by another counter.
    Gone,
}

/// A replay that is running, with everything it needs to run on its own.
///
/// This is what moves to the replayer's thread: the session, the publication it
/// writes through, and the two ids that name it.
pub struct RunningReplay {
    /// The id the client's OK carried, which is the conductor's key too.
    pub replay_session_id: i64,
    /// `ReplaySession.recordingId()`, which `abortReplaysOf` is asked by.
    pub recording_id: i64,
    /// The publication the frames go out on, taken out of the client by
    /// [`Sessions::open_replay`](crate::server::conductor::Sessions) before this
    /// was built. Giving it back is still the conductor's, **by registration
    /// id**, so it is not here.
    pub publication: Box<dyn Publication>,
    /// The replay itself.
    pub session: ReplaySession,
    /// The counter this replay follows, once the conductor has named it. Read in
    /// this thread's own turn rather than carried across from the conductor.
    pub follow: Option<LimitCounter>,
}

/// What the conductor tells a replayer that is on a thread of its own.
pub enum ReplayCommand {
    /// Run this.
    Add(Box<RunningReplay>),
    /// What the source has got to, this turn.
    Extend {
        /// Which replay.
        replay_session_id: i64,
        /// Where its stop position now is.
        update: LimitUpdate,
    },
    /// Stop one (`ArchiveConductor.stopReplay`).
    Abort {
        /// Which replay.
        replay_session_id: i64,
    },
    /// Stop every replay of one recording, or of every recording when the id is
    /// `NULL_VALUE` (`ArchiveConductor.stopAllReplays`, `:1056-1064`).
    AbortRecording {
        /// Which recording, or [`NULL_VALUE`](crate::mark::NULL_VALUE).
        recording_id: i64,
    },
}

/// What the conductor has to hear about from the replayer.
///
/// ★ **Only what changed.** A replay that sat idle sends nothing at all, which
/// is what keeps this channel off the data path: the frames go out through the
/// publication, in shared memory, and none of them pass through here.
#[derive(Debug)]
pub enum ReplayNotice {
    /// The segment opened and its header checked: the OK carrying the
    /// `replaySessionId` goes out **now**, and exactly once
    /// (`ReplaySession.java:338`).
    Started {
        /// Which replay.
        replay_session_id: i64,
        /// Where it has read up to.
        replay_position: i64,
    },
    /// The turn got somewhere: a block went out, or the publication connected.
    ///
    /// It carries the position because that is the one thing a session has that
    /// the conductor cannot ask it for once the two are on different threads —
    /// `isValidDetach` reads it (`ArchiveConductor.java:2322-2332`).
    Worked {
        /// Which replay.
        replay_session_id: i64,
        /// Where it has read up to.
        replay_position: i64,
    },
    /// It ended — cleanly, or with the error the client is owed
    /// (`ReplaySession.sendPendingError`, `:288-295`).
    Ended {
        /// Which replay.
        replay_session_id: i64,
        /// Whether its publication is to be **revoked** rather than closed: a
        /// replay that failed or was cancelled gets no linger
        /// (`ReplaySession.java:175-194`).
        revoke: bool,
        /// `code` is the response's `relevantId`, `message` what it carries.
        error: Option<(i32, String)>,
    },
    /// This turn's read, for the replayer's 108–110
    /// (`ReplaySession.readBytes`, which the reference reads back after the
    /// call, `:431-434`).
    ///
    /// Sent only when there was one: adding zero to a running total is adding
    /// nothing, and a turn that read nothing is a turn this channel stays quiet
    /// for.
    Read {
        /// The bytes this turn read.
        bytes: u64,
        /// The nanoseconds the read and the frame walk took.
        time_ns: u64,
    },
}

/// The replayer as the conductor holds it (P2-9a).
///
/// Under `SHARED` the sessions are the conductor's own and are driven in its own
/// turn (`SharedModeArchiveConductor.java:57-66`, which invokes the replayer's
/// `AgentInvoker` twice a turn). Under `DEDICATED` they are an agent on a thread
/// of their own (`DedicatedModeArchiveConductor.java:55`) and what the conductor
/// holds is a channel to it. The conductor asks the same questions either way.
pub enum ReplayerHalf {
    /// `SHARED`: the conductor's turn drives every replay.
    Inline(Replays),
    /// `DEDICATED`: the replayer is on a thread of its own.
    Threaded(ReplayerThread),
}

/// The replays a `SHARED` archive is running, driven inside the conductor's
/// turn.
pub struct Replays {
    running: std::collections::HashMap<i64, RunningReplay>,
}

impl Replays {
    /// Nothing running.
    #[must_use]
    pub fn new() -> Self {
        Self {
            running: std::collections::HashMap::new(),
        }
    }
}

impl Default for Replays {
    fn default() -> Self {
        Self::new()
    }
}

/// The conductor's end of a replayer that is on a thread of its own.
///
/// The mirror of [`RecorderThread`](crate::server::recorder::RecorderThread),
/// and the same two channels: commands down, notices up.
pub struct ReplayerThread {
    commands: std::sync::mpsc::Sender<ReplayCommand>,
    notices: std::sync::mpsc::Receiver<ReplayNotice>,
}

/// The body a replayer runs when it is on a thread of its own.
///
/// It owns the sessions and the publications they write through, and it opens
/// its own view of the counters region for the one thing a session needs from
/// it — the driver's `position_limit`, which bounds every offer
/// ([`Publication::available_window`]). It does **not** own the client: the
/// reference's rule is that one `Aeron` has one thread driving it, and what a
/// replay needs from the client — the publication — has already been handed over
/// by the time the session arrives here.
pub struct ReplayerWorker {
    running: Replays,
    /// The id its counters are keyed by, which is also what its duty-cycle pair
    /// is labelled with.
    archive_id: i64,
    /// This thread's own 103/104, once the conductor's claim has reached the
    /// region — see [`DutyCycleCounters::find`].
    duty: Option<DutyCycleCounters>,
    /// The threshold its 104 counts against (`Archive.java:1460-1468`).
    cycle_threshold_ns: i64,
    cnc: deepmsg_cnc::CncFile,
    idle: deepmsg_driver::idle::Strategy,
    commands: std::sync::mpsc::Receiver<ReplayCommand>,
    notices: std::sync::mpsc::Sender<ReplayNotice>,
    /// Reused between turns so a turn does not allocate per notice.
    scratch: Vec<ReplayNotice>,
}

impl ReplayerHalf {
    /// A replayer for an archive whose sessions the conductor drives itself.
    #[must_use]
    pub fn new() -> Self {
        Self::Inline(Replays::new())
    }

    /// A replayer for this archive running on the worker's thread.
    #[must_use]
    pub const fn threaded(thread: ReplayerThread) -> Self {
        Self::Threaded(thread)
    }

    /// Run this replay from here on.
    pub fn add(&mut self, replay: RunningReplay) {
        match self {
            Self::Inline(replays) => replays.add(replay),
            Self::Threaded(thread) => thread.send(ReplayCommand::Add(Box::new(replay))),
        }
    }

    /// Tell it where the source has got to, this turn.
    pub fn extend(&mut self, replay_session_id: i64, update: LimitUpdate) {
        match self {
            Self::Inline(replays) => replays.extend(replay_session_id, update),
            Self::Threaded(thread) => thread.send(ReplayCommand::Extend {
                replay_session_id,
                update,
            }),
        }
    }

    /// Stop one replay (`stopReplay`).
    pub fn abort(&mut self, replay_session_id: i64) {
        match self {
            Self::Inline(replays) => replays.abort(replay_session_id),
            Self::Threaded(thread) => thread.send(ReplayCommand::Abort { replay_session_id }),
        }
    }

    /// Stop every replay of a recording, or all of them (`stopAllReplays`).
    pub fn abort_recording(&mut self, recording_id: i64) {
        match self {
            Self::Inline(replays) => replays.abort_recording(recording_id),
            Self::Threaded(thread) => thread.send(ReplayCommand::AbortRecording { recording_id }),
        }
    }

    /// One turn, and what the conductor has to be told about it.
    ///
    /// The per-replay limit has already been given through
    /// [`ReplayerHalf::extend`], because the **conductor** is what works it out;
    /// see [`LimitUpdate`] for why it cannot be worked out here.
    pub fn drive(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        now_ns: u64,
    ) -> Vec<ReplayNotice> {
        match self {
            Self::Inline(replays) => replays.drive(counters, now_ns),
            // The worker drives itself; what this side owes the turn is what the
            // worker has said since the last one.
            Self::Threaded(thread) => thread.drain(),
        }
    }
}

impl Default for ReplayerHalf {
    fn default() -> Self {
        Self::new()
    }
}

impl Replays {
    /// Run this replay from here on.
    pub fn add(&mut self, replay: RunningReplay) {
        self.running.insert(replay.replay_session_id, replay);
    }

    /// Tell it where the source has got to.
    pub fn extend(&mut self, replay_session_id: i64, update: LimitUpdate) {
        let Some(replay) = self.running.get_mut(&replay_session_id) else {
            return;
        };

        apply_limit(replay, update);
    }

    /// Stop one.
    pub fn abort(&mut self, replay_session_id: i64) {
        if let Some(replay) = self.running.get_mut(&replay_session_id) {
            replay.session.abort();
        }
    }

    /// Stop every replay of a recording, or all of them.
    pub fn abort_recording(&mut self, recording_id: i64) {
        for replay in self.running.values_mut() {
            if recording_id == NULL_VALUE || replay.recording_id == recording_id {
                replay.session.abort();
            }
        }
    }

    /// One turn of every replay, in **no particular order** — and that is the
    /// reference's own, which walks a hash map's values
    /// (`ArchiveConductor.replaySessionByIdMap`).
    pub fn drive(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        now_ns: u64,
    ) -> Vec<ReplayNotice> {
        let mut notices = Vec::new();
        let mut ended = Vec::new();

        for (replay_session_id, replay) in &mut self.running {
            drive_one(replay, counters, now_ns, &mut notices);

            if replay.session.state() == State::Done {
                ended.push(*replay_session_id);
            }
        }

        for replay_session_id in ended {
            self.running.remove(&replay_session_id);
        }

        notices
    }
}

impl ReplayerThread {
    /// The two ends: what the conductor keeps, and the body to run on the
    /// replayer's own thread.
    ///
    /// # Errors
    ///
    /// [`ReplayerWorker::new`]'s, when the counter region will not open.
    pub fn pair(
        archive_id: i64,
        aeron_dir: &std::path::Path,
        idle: deepmsg_driver::idle::Strategy,
        timeout: std::time::Duration,
        cycle_threshold_ns: i64,
    ) -> Result<(Self, ReplayerWorker), String> {
        let (commands, from_conductor) = std::sync::mpsc::channel();
        let (to_conductor, notices) = std::sync::mpsc::channel();

        Ok((
            Self { commands, notices },
            ReplayerWorker::new(
                archive_id,
                aeron_dir,
                idle,
                timeout,
                cycle_threshold_ns,
                from_conductor,
                to_conductor,
            )?,
        ))
    }

    /// Tell the replayer something. A send that fails is a replayer thread that
    /// has stopped, which is not something a control request can act on.
    fn send(&mut self, command: ReplayCommand) {
        let _ = self.commands.send(command);
    }

    /// What it has said since the last turn.
    fn drain(&mut self) -> Vec<ReplayNotice> {
        self.notices.try_iter().collect()
    }
}

impl ReplayerWorker {
    /// A worker over a counter region it opens itself.
    ///
    /// # Errors
    ///
    /// The message from [`deepmsg_cnc::CncFile::open_writable`] when the region
    /// will not open.
    pub fn new(
        archive_id: i64,
        aeron_dir: &std::path::Path,
        idle: deepmsg_driver::idle::Strategy,
        timeout: std::time::Duration,
        cycle_threshold_ns: i64,
        commands: std::sync::mpsc::Receiver<ReplayCommand>,
        notices: std::sync::mpsc::Sender<ReplayNotice>,
    ) -> Result<Self, String> {
        let cnc = deepmsg_cnc::CncFile::open_writable(aeron_dir, timeout)
            .map_err(|error| format!("the replayer could not open the counter region: {error}"))?;

        Ok(Self {
            running: Replays::new(),
            archive_id,
            duty: None,
            cycle_threshold_ns,
            cnc,
            idle,
            commands,
            notices,
            scratch: Vec::new(),
        })
    }

    /// The replayer's own loop, which ends when the conductor's end of the
    /// command channel is gone — the same stop condition the recorder's worker
    /// has, and for the same reason: it is one thing to keep in step instead of
    /// two.
    pub fn run(mut self) {
        loop {
            // The cycle that just ended, before this turn does anything with it
            // (`DedicatedModeArchiveConductor.java:346`).
            self.measure_cycle();

            let mut work = 0;

            loop {
                match self.commands.try_recv() {
                    Ok(ReplayCommand::Add(replay)) => {
                        self.running.add(*replay);
                        work += 1;
                    }
                    Ok(ReplayCommand::Extend {
                        replay_session_id,
                        update,
                    }) => {
                        self.running.extend(replay_session_id, update);
                        work += 1;
                    }
                    Ok(ReplayCommand::Abort { replay_session_id }) => {
                        self.running.abort(replay_session_id);
                        work += 1;
                    }
                    Ok(ReplayCommand::AbortRecording { recording_id }) => {
                        self.running.abort_recording(recording_id);
                        work += 1;
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                }
            }

            let Some(counters) = self.cnc.counters_writable() else {
                self.idle.idle(work);

                continue;
            };

            let now_ns =
                u64::try_from(deepmsg_core::clock::monotonic_nano_time()).unwrap_or(u64::MAX);
            self.scratch.clear();
            let mut notices = std::mem::take(&mut self.scratch);
            notices.extend(self.running.drive(&counters, now_ns));

            let ended = notices
                .iter()
                .filter(|notice| matches!(notice, ReplayNotice::Ended { .. }))
                .count();

            for notice in notices.drain(..) {
                if self.notices.send(notice).is_err() {
                    return;
                }
            }

            self.scratch = notices;
            work += ended;
            self.idle.idle(work);
        }
    }

    /// One turn of this thread's duty cycle, into the pair the conductor
    /// allocated for the replayer (`Archive.java:1460-1468`).
    fn measure_cycle(&mut self) {
        let now_ns = deepmsg_core::clock::monotonic_nano_time();

        if self.duty.is_none() {
            let found = {
                let Some(counters) = self.cnc.counters_writable() else {
                    return;
                };
                DutyCycleCounters::find(
                    &counters,
                    DutyCycleActor::Replayer,
                    DEDICATED_MODE,
                    self.archive_id,
                    self.cycle_threshold_ns,
                )
            };
            if let Some(mut duty) = found {
                // `onStart`'s baseline, not the conductor's claim time.
                duty.update(now_ns);
                self.duty = Some(duty);
            }
            return;
        }

        let Some(counters) = self.cnc.counters_writable() else {
            return;
        };
        if let Some(duty) = self.duty.as_mut() {
            duty.measure(&counters, now_ns);
        }
    }
}

/// One turn of one replay, and what the conductor has to hear about it.
///
/// The shared half of both arms: `SHARED` calls it from the conductor's turn and
/// `DEDICATED` from the replayer's, which is what makes the two modes the same
/// replay.
fn drive_one(
    replay: &mut RunningReplay,
    counters: &CountersReader<'_, ReadWrite>,
    now_ns: u64,
    notices: &mut Vec<ReplayNotice>,
) {
    // ★ The reference's `notExtended` (`ReplaySession.java:374`, `:546-578`),
    // read **here**, in the turn that is about to read the recording, rather
    // than in the conductor's turn and carried over. The counter is in the
    // driver's region, which this thread has a mapping of; a turn between the
    // recorder writing and the replay reading is a turn of latency on every
    // message, and it showed as 2.6× on the cell's p50.
    if let Some(limit) = replay.follow {
        match read_limit_counter(counters, limit) {
            Some(position) => replay.session.extend_stop_position(position),
            // `notExtended`'s closed arm: the publisher is gone, so the limit
            // freezes where the replay has got to and the reader reaches it.
            None => replay.session.limit_counter_gone(),
        }
    }

    let progress = replay
        .session
        .do_work(replay.publication.as_mut(), counters, now_ns);
    let replay_session_id = replay.replay_session_id;
    let replay_position = replay.session.replay_position();

    // **One notice per turn that did anything, and none at all for a quiet
    // one.** That is what the reference's `workCount` counts
    // (`ArchiveConductor.java:2779-2790`: `!matches!(progress, Idle)`), so the
    // conductor's own count of it is exact rather than derived.
    match progress {
        Progress::Started => notices.push(ReplayNotice::Started {
            replay_session_id,
            replay_position,
        }),
        Progress::Worked => notices.push(ReplayNotice::Worked {
            replay_session_id,
            replay_position,
        }),
        Progress::Finished => notices.push(ReplayNotice::Ended {
            replay_session_id,
            revoke: replay.session.is_revoking(),
            error: None,
        }),
        Progress::Failed { code, message } => notices.push(ReplayNotice::Ended {
            replay_session_id,
            revoke: replay.session.is_revoking(),
            error: Some((code, message)),
        }),
        Progress::Idle => {}
    }

    let bytes = u64::try_from(replay.session.read_bytes()).unwrap_or(u64::MAX);

    if bytes > 0 || replay.session.read_time_ns() > 0 {
        notices.push(ReplayNotice::Read {
            bytes,
            time_ns: replay.session.read_time_ns(),
        });
    }
}

/// `ReplaySession.notExtended`'s effect on one session.
fn apply_limit(replay: &mut RunningReplay, update: LimitUpdate) {
    match update {
        LimitUpdate::ExtendTo(position) => replay.session.extend_stop_position(position),
        LimitUpdate::Follow(limit) => replay.follow = Some(limit),
        LimitUpdate::Gone => replay.session.limit_counter_gone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::AtomicBuffer;

    /// The four, over slots 0 to 3 of a region of its own.
    fn replayer_with_counters() -> (Replayer, Vec<u8>, Vec<u8>) {
        let mut meta = vec![0u8; 4 * deepmsg_cnc::layout::COUNTER_METADATA_LENGTH];
        let values = vec![0u8; 4 * deepmsg_cnc::layout::COUNTER_VALUE_LENGTH];

        // Every slot allocated, and the archive's own counting is a read of the
        // values region afterwards.
        for slot in 0..4 {
            let base = slot * deepmsg_cnc::layout::COUNTER_METADATA_LENGTH;
            meta[base + deepmsg_cnc::layout::COUNTER_STATE_OFFSET
                ..base + deepmsg_cnc::layout::COUNTER_STATE_OFFSET + 4]
                .copy_from_slice(&deepmsg_cnc::layout::COUNTER_STATE_ALLOCATED.to_le_bytes());
        }

        let mut replayer = Replayer::new(42);
        replayer.counters = Some(ReplayerCounters {
            replay_sessions: ArchiveIdCounter::for_test(0),
            max_read_time: ArchiveIdCounter::for_test(1),
            total_read_bytes: ArchiveIdCounter::for_test(2),
            total_read_time: ArchiveIdCounter::for_test(3),
        });

        (replayer, meta, values)
    }

    /// The totals add up, and the one that is a maximum stays at the longest —
    /// a short read after a long one leaves it where it was
    /// (`ArchiveConductor.java:2764-2777`).
    #[test]
    fn the_reads_add_up_and_the_longest_read_is_kept() {
        let mut totals = ReadTotals::default();

        totals.read(4_096, 1_000);
        totals.read(1_024, 500);

        assert_eq!((5_120, 1_500, 1_000), totals.totals());

        totals.read(2_048, 4_000);

        assert_eq!((7_168, 5_500, 4_000), totals.totals());
    }

    /// The three are published on a turn that did work, and not otherwise
    /// (`:2779-2790`).
    #[test]
    fn the_statistics_are_published_only_on_a_turn_that_did_work() {
        let (mut replayer, mut meta, mut values) = replayer_with_counters();
        let counters = CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut meta).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        );

        replayer.read(4_096, 1_000);

        replayer.publish_if(&counters, 0);
        assert_eq!(Some(0), counters.value(1), "an idle turn publishes nothing");
        assert_eq!(Some(0), counters.value(2));

        replayer.publish_if(&counters, 1);
        assert_eq!(Some(1_000), counters.value(1));
        assert_eq!(Some(4_096), counters.value(2));
        assert_eq!(Some(1_000), counters.value(3));
    }

    /// A replayer with no counters replays without counting: the allocation is
    /// the driver's to answer, and an archive that never got an answer still
    /// serves.
    #[test]
    fn a_replayer_with_no_counters_publishes_nothing() {
        let mut meta = vec![0u8; deepmsg_cnc::layout::COUNTER_METADATA_LENGTH];
        let mut values = vec![0u8; deepmsg_cnc::layout::COUNTER_VALUE_LENGTH];
        let counters = CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut meta).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        );

        let mut replayer = Replayer::new(42);
        replayer.read(4_096, 1_000);
        replayer.publish_if(&counters, 1);
        replayer.session_opened(&counters);
        replayer.session_closed(&counters);

        assert_eq!(Some(0), counters.value(0));
    }

    /// The session count goes up and down with the sessions that are open
    /// (`:993`, `:1382`).
    #[test]
    fn the_session_count_follows_the_sessions() {
        let (replayer, mut meta, mut values) = replayer_with_counters();
        let counters = CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut meta).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        );

        replayer.session_opened(&counters);
        replayer.session_opened(&counters);
        assert_eq!(Some(2), counters.value(0));

        replayer.session_closed(&counters);
        assert_eq!(Some(1), counters.value(0));
    }

    /// The four kinds are the reference's four, with the reference's names.
    #[test]
    fn the_four_counters_are_the_references_four() {
        assert_eq!(
            vec![
                (112, "Archive Replay Sessions"),
                (108, "archive-replayer max read time in ns"),
                (109, "archive-replayer total read bytes"),
                (110, "archive-replayer total read time in ns"),
            ],
            ReplayerCounter::ORDER
                .iter()
                .map(|kind| (kind.type_id(), kind.name()))
                .collect::<Vec<_>>()
        );
    }
}
