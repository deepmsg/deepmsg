//! One recording's session: the four states a recording moves through.
//!
//! `RecordingSession` is where an archive stops being a control plane and
//! starts being a recorder. Everything before it — the start request, the
//! subscription, the catalog row — is about *setting up* a recording; this is
//! the thing that reads the image and puts bytes in a segment file, one turn at
//! a time, until the stream ends (`RecordingSession.java:31-301`).
//!
//! # Four states, and why the two `if`s matter
//!
//! ```text
//! INIT -> RECORDING -> INACTIVE -> STOPPED      (RecordingSession.java:33-36)
//! ```
//!
//! `doWork` (`:131-163`) is **four separate `if`s, not a `switch`**: a session
//! that opens its first segment this turn records in the same turn, because
//! `init` moves it to `RECORDING` and the next `if` is then true. An aborted
//! session goes straight to `INACTIVE` and then to `STOPPED` in one turn. The
//! state machine is small, and the way it is written is what makes it move at
//! the speed the acceptance test measures — a position that lags a turn behind
//! the publisher is a position `waitUntilCaughtUp` waits for.
//!
//! # The image is held, and `isClosed` is a flag
//!
//! The reference's session holds the `Image` object (`:60`) and asks it
//! `isEndOfStream()` and `isClosed()` (`:242`), where `isClosed` is a field its
//! client conductor sets when the driver says the image is unavailable
//! (`Image.java:204-207`). This session does the same (P2-9a), through the
//! [`RecordingImage`] seam — and it has to, because a session that is not on the
//! client's thread cannot ask the client anything.
//!
//! Until P2-9a it did the opposite: the session held two ids and asked
//! [`Client::block_poll_image`] for the image every turn, and an image the
//! client had removed answered `None` — which is what stood in for
//! `image.isClosed()`. That works only while the two share a thread. What
//! replaced it is the flag the client flips on the image **it handed over**
//! ([`Client::take_image`]): an image that has left the subscription is
//! otherwise unreachable by the `UnavailableImage` arm that removes it, which
//! is why the subscription keeps a [`HandedOut`] record.
//!
//! # What is not the reference's shape
//!
//! * **The block is copied.** `Image.blockPoll` hands the reference a window
//!   onto the term and its `RecordingWriter.write` takes the window straight to
//!   the file (`RecordingWriter.java:104-121`). This build's
//!   [`RecordingWriter::write_block`] takes a slice, and a
//!   [`Block`](deepmsg_client::image::Block) is a window over a mapping, so each
//!   run is copied into a buffer the session keeps. One copy per poll, not per
//!   byte, and it is the price of the writer's signature.
//! * **A failed write is carried out of the poll rather than thrown out of it.**
//!   The reference's handler throws and `record` catches (`:262-276`); a Rust
//!   handler cannot, so the error is put aside and the same two things are done
//!   with it — the message is kept for the client, and the session stops.
//! * **The reason a state changed is not logged.** The reference logs one line
//!   per transition (`:283-298`) through its `ArchiveLog`, which this build does
//!   not have. What is kept is the transition itself.
//!
//! # The recording events, and the one thing this build reads early
//!
//! The reference's session holds the `RecordingEventsProxy` object (`:44`) and,
//! at its three landing points, asks the **image** where the recording is
//! (`image.joinPosition()`, `image.sessionId()`, `image.subscription().streamId()`,
//! `image.sourceIdentity()` — `:218-220`, `:249-254`, `:152-154`). This one
//! cannot: the image it holds is a [`RecordingImage`], whose whole job is to be
//! four answers about reading and not a window onto the client's `Image`, and
//! widening it for the events would mean a second implementation in every test
//! that builds a session. So those four facts are read **once**, where the image
//! is still the client's and the subscription that carried it is still in hand
//! (`crate::server::conductor::Sessions::start_recordings`), and travel into the
//! session as parameters. Nothing else about them is the reference's, and the
//! events they feed are the same events.

use std::path::Path;

use deepmsg_client::client::Client;
use deepmsg_client::image::{Block, Image};
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::mark::NULL_VALUE;
use crate::recording_writer::{RecordingWriter, WriteStats};
use crate::server::recording_events::{EventsError, RecordingEvents};
use crate::server::recording_pos::RecordingPos;

/// The image a recording reads, as far as a session is concerned.
///
/// The twin of [`crate::server::replay_session::Publication`], and for the same
/// reason: what the session needs is four questions, and the *client's* `Image`
/// is one answer to them while a unit test's is another. A test that made a
/// session would otherwise have to build a mapped log buffer — the client's own
/// tests keep a sixty-line `TempLog` for that — when all it is testing is which
/// state a session moves to.
///
/// It is also the seam P2-9a needs: the session **holds** its image rather than
/// naming it to the client every turn, which is the reference's shape
/// (`RecordingSession.java:45`) and the thing that lets a recorder run on a
/// thread of its own while the client stays on the conductor's. `Send` because
/// that is where the session goes.
pub trait RecordingImage: Send {
    /// `image.termBufferLength()` (`Image.java:138`) — how many bytes a term is,
    /// which bounds what one turn may read (`:81`).
    fn term_buffer_length(&self) -> i32;

    /// The **publication's** registration id, which is what tells one image of
    /// a subscription from another (`Image.correlationId()`, `:184`).
    fn registration_id(&self) -> i64;

    /// `image.blockPoll(...)` (`:237`): one run of contiguous frames, handed
    /// over as a single block, and how many bytes it was.
    ///
    /// ★ **And the reader's own position counter, which has to travel with the
    /// read.** `Image.block_poll` advances the image's reader and nothing else;
    /// it was the *client* that published where the reader had got to, right
    /// after the poll (`crate::client::Client::block_poll_image`, the
    /// `if position > position_before` store). That counter is what the driver
    /// takes `min_sub_pos` from to raise the publisher's window
    /// (`aeron_ipc_publication.c:296-313`), so a reader that stops publishing
    /// it **blocks the publisher**: the archive would sit on a stream that had
    /// gone quiet, which is exactly what moving the image out of the client
    /// does if this step does not come with it.
    ///
    /// The counters region is a parameter because the image no longer lives
    /// inside the client that owns the mapping.
    fn block_poll(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        block_length_limit: usize,
        handler: &mut dyn FnMut(&Block<'_>),
    ) -> usize;

    /// `image.isEndOfStream()` (`:249`).
    fn is_end_of_stream(&self) -> Option<bool>;

    /// `image.isClosed()` (`:204-207`) — the driver withdrew the image. See the
    /// module note for why this build has a flag here at all.
    fn is_closed(&self) -> bool;
}

impl RecordingImage for Image {
    fn term_buffer_length(&self) -> i32 {
        Image::term_buffer_length(self)
    }

    fn registration_id(&self) -> i64 {
        Image::registration_id(self)
    }

    fn block_poll(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        block_length_limit: usize,
        handler: &mut dyn FnMut(&Block<'_>),
    ) -> usize {
        let before = Image::position(self);
        let read = Image::block_poll(self, block_length_limit, handler);
        let position = Image::position(self);

        // The store the client used to make, with the same guard: a turn that
        // read nothing has not moved the position, and a store that says so
        // would be one the driver reads for nothing.
        if position > before {
            counters.set_value(self.subscriber_position_id(), position);
        }

        read
    }

    fn is_end_of_stream(&self) -> Option<bool> {
        Image::is_end_of_stream(self)
    }

    fn is_closed(&self) -> bool {
        Image::is_closed(self)
    }
}

/// Where a recording is (`RecordingSession.java:33-36`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Made, and its first segment not opened yet (`:204-231`).
    Init,
    /// Reading the image into the segment (`:233-278`).
    Recording,
    /// Finished reading, not yet closed (`:146-156`).
    Inactive,
    /// Done, and the conductor may forget it (`:98-101`).
    Stopped,
}

/// What one turn of reading the image did
/// (`RecordingSession.record`, `RecordingSession.java:233-278`).
///
/// The reference's `record` is one `blockPoll` and then three branches; this is
/// the three branches with the poll's answer handed in, so that the part which
/// can be wrong is testable without a driver — the same split `start_listing`
/// and `decide_start` make, and for the same reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Bytes were read: the position moves to where the writer got to
    /// (`:238-241`).
    Recorded(usize),
    /// Nothing was read and the stream is still open: the next turn tries again
    /// (`:242`'s missing arm — neither branch fires).
    Idle,
    /// The image is finished, either way (`:242-246`).
    Ended,
}

/// Which of the three a turn is (`RecordingSession.record`).
///
/// `read` is how many bytes the poll moved, and it is tested **first** because
/// that is the reference's order: a turn that read the last bytes of a stream
/// records them and leaves the ending to the next turn. `is_closed` and
/// `is_end_of_stream` are the two arms of the reference's `else if` (`:242`).
fn step(read: usize, is_end_of_stream: Option<bool>, is_closed: bool) -> Step {
    if read > 0 {
        return Step::Recorded(read);
    }

    // Nothing was read: either the image is finished — the stream ended, or the
    // driver took it away — or the stream simply has nothing new yet.
    if is_closed || is_end_of_stream == Some(true) {
        return Step::Ended;
    }

    Step::Idle
}

/// A reborrow of the turn's publication, so that one `Option<&mut _>` can be
/// handed to three turns in a row.
///
/// `Option<&mut dyn RecordingEvents>` will not do that on its own: a reborrow
/// through `as_deref_mut` is tied to the borrow of the `Option`, and once the
/// trait object's own lifetime is in play the borrow checker will not shorten it
/// back. Naming the two lifetimes is the whole of the fix.
fn reborrow<'events, 'turn>(
    events: &'turn mut Option<&'events mut dyn RecordingEvents>,
) -> Option<&'turn mut (dyn RecordingEvents + 'events)> {
    events.as_deref_mut()
}

/// One recording, from its first segment to its last.
///
/// The fields are the reference's (`RecordingSession.java:34-56`), less two: the
/// error handler (the conductor's, reached through the conductor) and
/// `recordingEventsProxy`, which is **not** held here but lent to each turn —
/// see [`RecordingSession::do_work`] and the module note.
pub struct RecordingSession {
    /// The control session that asked, which is where the `STOP` signal goes
    /// and where a failure is answered (`:1345-1347`).
    ///
    /// The reference holds the `ControlSession` **object** here (`:54`); this
    /// build holds the id, because the object lives in the conductor's map and
    /// a value cannot be in two places — the same substitution the registry
    /// makes for a `Subscription`.
    session_id: i64,
    /// The start request's correlation id, which is what a `START`/`STOP`
    /// signal carries and what a failure is answered on (`:2051`).
    correlation_id: i64,
    /// The catalog's id for this recording, which is also the session's own
    /// (`RecordingSession.sessionId()`, `:85-89`).
    recording_id: i64,
    /// The subscription the image arrived on, which is what a stop names it by
    /// and what the registry holds.
    subscription_id: i64,
    /// The image itself.
    ///
    /// ★ **Held, not named** (P2-9a). The reference's session holds the `Image`
    /// object (`:45`) and this build used to hold the two ids the client knew
    /// it by and ask the client for it every turn — the same thing only while
    /// the session and the client are on one thread, which is exactly what
    /// `DEDICATED` is not.
    image: Box<dyn RecordingImage>,
    /// Where the image joined the stream (`image.joinPosition()`, `:79`): what
    /// all three events name as the recording's start, and what the progress
    /// mark starts at. The writer has it too; the session needs its own copy
    /// because the events outlive any question the writer is asked.
    join_position: i64,
    /// The **image's** session id, which is what the `started` event carries
    /// (`:218-220`) and what an extension writes into the catalog row
    /// (`ArchiveConductor.java:2120`).
    image_session_id: i32,
    /// The stream id of the **subscription the image arrived on**
    /// (`image.subscription().streamId()`, `:220`) — not the events channel's.
    /// Both are `i32`, so mistaking one for the other compiles.
    stream_id: i32,
    /// `image.sourceIdentity()` (`:220`).
    source_identity: String,
    /// The channel the recording was asked for, which the `started` event
    /// carries as the recording's own (`:220`) — the reference's
    /// `RecordingSession.originalChannel` (`:46`).
    original_channel: String,
    /// `progressEventPosition` (`:42`), which belongs to **this session** and
    /// not to the publication: two recordings each have their own.
    ///
    /// It starts at the image's join position (`:79`), which is where the
    /// writer starts too — so a session that has read nothing sends no progress
    /// event, where a mark of `NULL_VALUE` would send one for a position that
    /// was never recorded.
    progress_event_position: i64,
    /// `min(image.termBufferLength(), ctx.fileIoMaxLength())` (`:81`) — how many
    /// bytes one turn may read, which is not the same as how many fit in a
    /// segment.
    block_length_limit: usize,
    /// Where the recording has got to, which is the counter the acceptance test
    /// waits on ([`RecordingPos`], `:79`).
    position: RecordingPos,
    /// The segment file being written (`:77`).
    writer: RecordingWriter,
    /// Where the reader's runs are copied to — see the module note. Sized to
    /// `block_length_limit` when the session is made, so a poll never allocates.
    scratch: Vec<u8>,
    state: State,
    /// What went wrong, if anything did: sent to the client when the session
    /// ends (`sendPendingError`, `:191-197`) and logged by the conductor.
    error_message: Option<String>,
    /// Whether the recording stops when the client that asked goes away
    /// (`:1329-1363`).
    is_auto_stop: bool,
    /// Why it was aborted, if it was (`:102-106`).
    abort_reason: Option<String>,
    is_aborted: bool,
}

impl RecordingSession {
    /// A session that has not opened its first segment yet
    /// (`RecordingSession.java:60-79`).
    ///
    /// `join_position` is where the **image** joined the stream and
    /// `start_position` where the **recording** starts; they differ for a stream
    /// that was already running when the archive subscribed, and the difference
    /// is the first segment's offset (`RecordingWriter.java:98-101`).
    ///
    /// The image's MTU is not a parameter: the reference's session carries it
    /// only to hand it to the recording events (`:229`), which do not take one.
    /// The four that *are* parameters — the image's session id, the subscription's
    /// stream id, the channel the client asked for and the image's source
    /// identity — are the four the events carry and the four this build reads
    /// where the image is still in hand; see the module note.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // one per thing the reference's constructor takes
    pub fn new(
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        start_position: i64,
        join_position: i64,
        segment_file_length: usize,
        subscription_id: i64,
        image: Box<dyn RecordingImage>,
        image_session_id: i32,
        stream_id: i32,
        original_channel: String,
        source_identity: String,
        file_io_max_length: usize,
        file_sync_level: i32,
        is_auto_stop: bool,
        position: RecordingPos,
        directory: &Path,
        checksum: Option<crate::checksum::Checksum>,
    ) -> Self {
        // `min(image.termBufferLength(), ctx.fileIoMaxLength())` (`:81`): a turn
        // reads at most a term, and at most what the archive was told a file
        // write may be. The term length is the **image's**, read here once
        // rather than passed in, because the image is what has it.
        let term_buffer_length = image.term_buffer_length();
        let file_io_max_length = i32::try_from(file_io_max_length).unwrap_or(i32::MAX);
        let block_length_limit =
            usize::try_from(term_buffer_length.min(file_io_max_length)).unwrap_or(0);

        Self {
            session_id,
            correlation_id,
            recording_id,
            subscription_id,
            image,
            join_position,
            image_session_id,
            stream_id,
            source_identity,
            original_channel,
            // `this.progressEventPosition = image.joinPosition()` (`:79`).
            progress_event_position: join_position,
            block_length_limit,
            position,
            writer: RecordingWriter::new(
                directory,
                recording_id,
                start_position,
                join_position,
                term_buffer_length,
                segment_file_length,
                file_sync_level,
                checksum,
            ),
            scratch: vec![0; block_length_limit],
            state: State::Init,
            error_message: None,
            is_auto_stop,
            abort_reason: None,
            is_aborted: false,
        }
    }

    /// The recording this session is making, which is what the conductor files
    /// it under (`ArchiveConductor.java:2054`).
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// The control session that asked (`:54`).
    #[must_use]
    pub const fn session_id(&self) -> i64 {
        self.session_id
    }

    /// The request that started it (`:2051`).
    #[must_use]
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// The subscription whose image it is reading, which is what a stop takes
    /// off (`:1766-1780`).
    #[must_use]
    pub const fn subscription_id(&self) -> i64 {
        self.subscription_id
    }

    /// Whether the recording ends with the client that asked
    /// (`ArchiveConductor.java:1358`).
    #[must_use]
    pub const fn is_auto_stop(&self) -> bool {
        self.is_auto_stop
    }

    /// Where it is.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// Whether the conductor may forget it (`RecordingSession.isDone`,
    /// `:98-101`).
    #[must_use]
    pub const fn is_done(&self) -> bool {
        matches!(self.state, State::Stopped)
    }

    /// Why it was aborted, if it was (`:102-106`).
    ///
    /// The reference logs it and nothing else reads it (`:283-298`); this build
    /// has no archive log, so it is here for the conductor and for a reader
    /// asking why a recording stopped.
    #[must_use]
    pub fn abort_reason(&self) -> Option<&str> {
        self.abort_reason.as_deref()
    }

    /// What went wrong, if anything did, in the words the client is sent
    /// (`:191-197`).
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Where the recording got to, as the counter holds it
    /// (`RecordSession.recordedPosition`, `:167-176`).
    ///
    /// `None` is the reference's `NULL_POSITION`, which is what it answers when
    /// its counter has been closed — and what the conductor writes into the
    /// catalog and the `STOP` signal for a session that never recorded
    /// anything.
    #[must_use]
    pub fn recorded_position<Access>(&self, counters: &CountersReader<'_, Access>) -> Option<i64> {
        self.position.value(counters)
    }

    /// `RecordingSession.abort` (`:102-106`): the session notices on its next
    /// turn, which is why the flag and not a state change.
    pub fn abort(&mut self, reason: &str) {
        self.abort_reason = Some(reason.to_owned());
        self.is_aborted = true;
    }

    /// Give back the segment file and the counter (`:112-120`).
    ///
    /// The reference's `CloseHelper.close` swallows what it cannot close; this
    /// does too, because a session on its way out has nothing left to do about
    /// it — the counter's removal is asynchronous either way.
    pub fn close(&mut self, client: &mut Client) {
        self.writer.close();
        let _ = self.position.release(client);
    }

    /// One turn (`RecordingSession.doWork`, `:131-163`).
    ///
    /// Four `if`s rather than a match on the state, because that is what the
    /// reference has: see the module note for what that buys.
    ///
    /// `events` is the recording events publication, **lent for the turn**:
    /// `None` is the reference's `null` (`:134`, `:146`, `:160` all test it),
    /// which is what an archive with
    /// `aeron.archive.recording.events.enabled=false` has. `Option<&mut _>`
    /// rather than `&mut Option<_>` because that is the same test: an archive
    /// that has no publication cannot send one.
    ///
    /// # Errors
    ///
    /// [`EventsError`] when the events publication is finished
    /// (`RecordingEventsProxy.checkResult`), which the reference **throws** and
    /// lets out of the agent driving the sessions — see
    /// [`crate::server::recorder`] for where that lands here.
    pub fn do_work<S: WriteStats>(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        stats: &mut S,
        mut events: Option<&mut dyn RecordingEvents>,
    ) -> Result<usize, EventsError> {
        let mut work = 0;

        if self.is_aborted {
            self.state = State::Inactive;
        }

        if self.state == State::Init {
            work += self.init(counters, reborrow(&mut events))?;
        }

        if self.state == State::Recording {
            work += self.record(counters, stats, reborrow(&mut events))?;
        }

        if self.state == State::Inactive {
            self.state = State::Stopped;
            self.writer.close();
            work += 1;

            // `:160-163`: **after** the state and the writer's close, and with
            // where the counter says the recording got to rather than where the
            // writer did (`position.getPlain()`).
            if let Some(events) = reborrow(&mut events) {
                events.stopped(
                    counters,
                    self.recording_id,
                    self.join_position,
                    self.position.value(counters).unwrap_or(NULL_VALUE),
                )?;
            }
        }

        Ok(work)
    }

    /// `INIT`: open the first segment (`RecordingSession.init`, `:204-231`).
    ///
    /// A writer that will not open is a recording that never starts, and the
    /// reference answers that by keeping the message for the client and moving
    /// straight to `STOPPED` — there is nothing to record into, so there is
    /// nothing to try again for. **No `started` event goes out for it**, which
    /// is the reference's order: its `catch` rethrows before the events block
    /// (`:206-217`).
    // The trait object's lifetime is its own parameter rather than the elided
    // `(dyn RecordingEvents + '_)` a bare `&mut dyn` means: `&mut` is invariant,
    // so a reborrow of a longer-lived object would not fit.
    fn init<'events>(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        events: Option<&mut (dyn RecordingEvents + 'events)>,
    ) -> Result<usize, EventsError> {
        if let Err(error) = self.writer.init() {
            self.error_message = Some(error.to_string());
            self.writer.close();
            self.state = State::Stopped;

            return Ok(1);
        }

        // `:218-227`, and it is **before** the state moves: a `started` event
        // that cannot go out leaves the session in `INIT` to try again, which is
        // what the reference's throw out of `doWork` leaves too.
        if let Some(events) = events {
            events.started(
                counters,
                self.recording_id,
                self.join_position,
                self.image_session_id,
                self.stream_id,
                &self.original_channel,
                &self.source_identity,
            )?;
        }

        self.state = State::Recording;

        Ok(1)
    }

    /// The reference's progress block (`:248-257`), as the decision it is.
    ///
    /// ★ **Two things, and both are load-bearing.** An event goes out only when
    /// the recording has moved past the mark, so the same position is never sent
    /// twice; and the mark moves only when the event went out, so one that was
    /// dropped is offered again rather than lost — the position it named is
    /// already behind the recording and nothing will name it again.
    ///
    /// A free function over the answers, like [`step`], so that both halves can
    /// be tested without a publication.
    fn progress_mark(mark: i64, recorded_position: i64, sent: bool) -> i64 {
        if mark < recorded_position && sent {
            recorded_position
        } else {
            mark
        }
    }

    /// `RECORDING`: read one run of frames and write it
    /// (`RecordingSession.record`, `:233-278`).
    ///
    /// The whole turn is here rather than in a helper because the poll borrows
    /// the image, the writer and the scratch buffer at once, and splitting it
    /// would mean handing three borrows across a call for no gain.
    fn record<'events, S: WriteStats>(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        stats: &mut S,
        events: Option<&mut (dyn RecordingEvents + 'events)>,
    ) -> Result<usize, EventsError> {
        let (read, failure) = {
            let Self {
                image,
                block_length_limit,
                writer,
                scratch,
                ..
            } = self;

            let mut failure = None;

            let read = image.block_poll(counters, *block_length_limit, &mut |block| {
                // Once a block has failed there is nothing to write the rest
                // of the run with: the reference throws out of its handler on
                // the first exception and the poll stops there
                // (`RecordingSession.java:262-275`, whose `catch` moves the
                // session to `INACTIVE`). A Rust handler cannot throw, so the
                // rest of the run is dropped instead — which is also what
                // keeps a **half-rolled** writer from being used: a failed
                // roll-over leaves the segment writer with its offset reset
                // and its file still the old one, and writing into it would
                // overwrite the bytes already recorded.
                if failure.is_some() {
                    return;
                }

                let length = block.length();

                let copied = match scratch.get_mut(..length) {
                    Some(bytes) => block.copy_out(bytes).is_some(),
                    // Unreachable: the buffer is sized to the limit and a
                    // run is never longer than it. A run that somehow is
                    // would be a block lost, not a block half-written.
                    None => false,
                };

                if !copied {
                    return;
                }

                if let Err(error) = writer.write_block(&scratch[..length], stats) {
                    failure = Some(error);
                }
            });

            (read, failure)
        };

        if let Some(error) = failure {
            self.error_message = Some(error.to_string());
            self.state = State::Inactive;

            // No progress event, which is the reference's shape too: its
            // handler throws out of `blockPoll` and the `catch` skips the
            // events block entirely (`:262-276`).
            return Ok(1);
        }

        // Asked only when nothing was read, which is where the reference asks it
        // too (`:242`) — and of its own image, which is now where the answer
        // lives.
        let is_end_of_stream = if 0 == read {
            self.image.is_end_of_stream()
        } else {
            None
        };

        // The reference's other arm of the same `if` (`:242`): an image the
        // driver has withdrawn ends the session whether or not the stream said
        // it was over. See the module note on why this build has a flag where
        // the reference reads a field its client conductor set.
        let is_closed = self.image.is_closed();

        let work = match step(read, is_end_of_stream, is_closed) {
            Step::Recorded(read) => {
                self.position.set_position(counters, self.writer.position());

                read
            }
            Step::Idle => 0,
            Step::Ended => {
                self.state = State::Inactive;

                1
            }
        };

        // `:248-257`, **after** the state may have moved to `INACTIVE`: the turn
        // that reads the last bytes of a stream and the turn that hears the
        // stream has ended both still tell the events where the recording got
        // to. `recordingWriter.position()` is asked for rather than the counter,
        // which is the reference's line.
        if let Some(events) = events {
            let recorded_position = self.writer.position();

            if self.progress_event_position < recorded_position {
                let sent = events.progress(
                    counters,
                    self.recording_id,
                    self.join_position,
                    recorded_position,
                )?;

                self.progress_event_position =
                    Self::progress_mark(self.progress_event_position, recorded_position, sent);
            }
        }

        Ok(work)
    }
}

impl std::fmt::Debug for RecordingSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingSession")
            .field("recording_id", &self.recording_id)
            .field("session_id", &self.session_id)
            .field("correlation_id", &self.correlation_id)
            .field("subscription_id", &self.subscription_id)
            .field("image_id", &self.image.registration_id())
            .field("state", &self.state)
            .field("position", &self.writer.position())
            .field("abort_reason", &self.abort_reason)
            .finish()
    }
}

/// An image for a session a test makes: the five questions answered from
/// fields, and no log buffer anywhere.
///
/// `pub(crate)` under `#[cfg(test)]` because two modules' tests build sessions —
/// this one's and the recorder's — and neither is testing the client's mapping.
#[cfg(test)]
pub(crate) struct FakeImage {
    pub(crate) registration_id: i64,
    pub(crate) term_buffer_length: i32,
    pub(crate) closed: bool,
    /// What a poll moves, so a test can drive a session through `RECORDING`.
    pub(crate) read: usize,
}

#[cfg(test)]
impl FakeImage {
    pub(crate) fn new() -> Self {
        Self {
            registration_id: 11,
            term_buffer_length: 64 * 1024,
            closed: false,
            read: 0,
        }
    }
}

#[cfg(test)]
impl RecordingImage for FakeImage {
    fn term_buffer_length(&self) -> i32 {
        self.term_buffer_length
    }

    fn registration_id(&self) -> i64 {
        self.registration_id
    }

    fn block_poll(
        &mut self,
        _counters: &CountersReader<'_, ReadWrite>,
        _block_length_limit: usize,
        _handler: &mut dyn FnMut(&Block<'_>),
    ) -> usize {
        self.read
    }

    fn is_end_of_stream(&self) -> Option<bool> {
        None
    }

    fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::AtomicBuffer;

    use crate::recording_writer::NoStats;
    use crate::server::recording_events::{EventCall, FakeEvents};

    /// The three branches `record` takes, and the one order that matters: a
    /// turn that read the last bytes of a stream records them (`:238-246`).
    #[test]
    fn a_turn_ends_the_recording_only_when_nothing_was_read() {
        assert_eq!(Step::Recorded(64), step(64, None, false));
        assert_eq!(
            Step::Recorded(64),
            step(64, Some(true), false),
            "the read is tested first, so its bytes are not thrown away"
        );
        assert_eq!(
            Step::Recorded(64),
            step(64, Some(true), true),
            "and a *closed* image's bytes are no different: `workCount > 0` is \
             the reference's first branch, whatever the flags say"
        );

        assert_eq!(Step::Idle, step(0, Some(false), false));
        assert_eq!(
            Step::Idle,
            step(0, None, false),
            "an image whose metadata cannot be read is not one that ended"
        );

        assert_eq!(Step::Ended, step(0, Some(true), false));
        assert_eq!(
            Step::Ended,
            step(0, None, true),
            "an image the driver has withdrawn is this build's `isClosed`"
        );
    }

    /// A recording whose segment cannot be opened never starts, and the message
    /// it will send the client is the one the writer gave it
    /// (`RecordingSession.java:206-216`).
    ///
    /// What makes it fail is a **file system** refusal — the directory it is
    /// told to write into is a file — because a segment that is already there is
    /// not one: that is an extension, and it opens it.
    #[test]
    fn a_session_that_cannot_open_its_segment_is_stopped_with_the_error() {
        let dir = crate::mark::tests::TempDir::new();
        let recording_id = 7;

        let not_a_directory = dir.path().join("not-a-directory");
        std::fs::write(&not_a_directory, b"a file where a directory should be").expect("written");

        let mut session = a_session(&not_a_directory, recording_id);

        assert_eq!(Ok(1), session.do_work_without_a_client());
        assert_eq!(State::Stopped, session.state());
        assert!(session.is_done());
        assert!(
            session.error_message().is_some(),
            "the client is owed the writer's words"
        );
    }

    /// A session is not done until it has been stopped, and `close` is safe
    /// twice over — the conductor that collects it may be closing a session the
    /// session itself already closed (`RecordingSession.java:112-120`, whose
    /// `CloseHelper` is idempotent for the same reason).
    #[test]
    fn a_fresh_session_is_not_done() {
        let dir = crate::mark::tests::TempDir::new();
        let session = a_session(dir.path(), 7);

        assert_eq!(State::Init, session.state());
        assert!(!session.is_done());
        assert_eq!(7, session.recording_id());
        assert_eq!(5, session.session_id());
        assert_eq!(3, session.correlation_id());
        assert_eq!(9, session.subscription_id());
        assert!(!session.is_auto_stop());
        assert!(session.error_message().is_none());
    }

    /// The whole of a recording's life, told to the events: `started` as the
    /// first segment opens, `progress` as bytes are read, `stopped` at the end
    /// — and each of them with the fields the reference passes
    /// (`RecordingSession.java:218-220`, `:249-254`, `:152-154`).
    ///
    /// ★ **What the `started` event carries is the whole reason the session has
    /// fields of its own**: `recordingId`, the image's join position and session
    /// id, the **subscription's** stream id, the channel the client asked for and
    /// the image's source identity — six numbers read off two different objects,
    /// and the two `i32`s among them are not interchangeable.
    #[test]
    fn a_recording_tells_the_events_what_it_did() {
        let dir = crate::mark::tests::TempDir::new();
        let counters = counters();
        let mut stats = NoStats;
        let mut events = FakeEvents::new();
        let mut session = a_session(dir.path(), 7);

        assert_eq!(
            Ok(1),
            session.do_work(&counters, &mut stats, as_events(&mut events))
        );
        assert_eq!(
            vec![EventCall::Started {
                recording_id: 7,
                start_position: 0,
                session_id: 42,
                stream_id: 1001,
                channel: "aeron:udp?endpoint=localhost:9000".to_owned(),
                source_identity: "aeron:udp?endpoint=localhost:8000".to_owned(),
            }],
            events.calls,
            "one event, and it is before the recording has read anything"
        );

        // The image goes away with nothing left to read, which is the end of the
        // recording (`:242`) — and the same turn closes it, so `stopped` is on
        // the same `doWork` as the `INACTIVE` move (`:146-163`).
        session.image = Box::new(FakeImage {
            closed: true,
            ..FakeImage::new()
        });

        assert_eq!(
            Ok(2),
            session.do_work(&counters, &mut stats, as_events(&mut events)),
            "the ending turn and the close are both work"
        );
        assert_eq!(State::Stopped, session.state());
        assert_eq!(
            EventCall::Stopped {
                recording_id: 7,
                // ★ Where the **counter** says the recording got to, not where
                // the writer did (`position.getPlain()`, `:154`) — and this
                // test's counter was never allocated, so that is the reference's
                // `NULL_POSITION`, the same answer its `recordedPosition` gives
                // for a counter that has been closed (`:171-179`).
                stop_position: NULL_VALUE,
            },
            events.calls[1],
            "and nothing was recorded in between"
        );
    }

    /// ★ **A progress event that was dropped is offered again**, and the position
    /// it named is not lost: the mark moves only when the publication took it
    /// (`:249-254`).
    ///
    /// The mutation this pins is the tempting one — advancing the mark at the
    /// same time as sending, which reads fine and loses every event a slow
    /// subscriber caused to be dropped. Nothing names that position again.
    ///
    /// The session is driven through `record` directly because that is the turn
    /// the block is in, and the writer is moved by writing to it: a poll has to
    /// come from an image with a mapped term, which is the one thing a test
    /// session has not got.
    #[test]
    fn a_progress_event_that_did_not_go_out_is_sent_again() {
        let dir = crate::mark::tests::TempDir::new();
        let counters = counters();
        let mut stats = NoStats;
        let mut events = FakeEvents::new();
        let mut session = a_session(dir.path(), 7);

        write_bytes(&mut session, 64);
        assert_eq!(64, session.writer.position());

        // Dropped: the same position is offered again next turn.
        events.sent = false;
        assert_eq!(
            Ok(0),
            session.record(&counters, &mut stats, as_events(&mut events))
        );
        assert_eq!(
            vec![EventCall::Progress {
                recording_id: 7,
                position: 64
            }],
            events.calls
        );

        assert_eq!(
            Ok(0),
            session.record(&counters, &mut stats, as_events(&mut events))
        );
        assert_eq!(
            vec![
                EventCall::Progress {
                    recording_id: 7,
                    position: 64
                };
                2
            ],
            events.calls,
            "the mark did not move, so the event is offered again"
        );

        // Taken: the mark moves, and the same position is never sent twice.
        events.sent = true;
        assert_eq!(
            Ok(0),
            session.record(&counters, &mut stats, as_events(&mut events))
        );
        write_bytes(&mut session, 64);
        assert_eq!(
            Ok(0),
            session.record(&counters, &mut stats, as_events(&mut events))
        );

        assert_eq!(
            vec![
                EventCall::Progress {
                    recording_id: 7,
                    position: 64
                },
                EventCall::Progress {
                    recording_id: 7,
                    position: 64
                },
                EventCall::Progress {
                    recording_id: 7,
                    position: 64
                },
                EventCall::Progress {
                    recording_id: 7,
                    position: 128
                }
            ],
            events.calls,
            "four sends: the two dropped ones again, the one that took, and \
             nothing for the position that had already been reported"
        );
        assert_eq!(128, session.progress_event_position);
    }

    /// The progress block's two lines as the decision they are
    /// (`:249-254`).
    ///
    /// A free function over the answers, like [`step`]: both halves are about a
    /// pair of numbers and a `bool`, and a test that had to reach them through a
    /// publication would be testing the publication.
    #[test]
    fn the_progress_mark_moves_only_for_an_event_that_went_out() {
        assert_eq!(
            0,
            RecordingSession::progress_mark(0, 0, true),
            "nothing was read, so there is nothing to report"
        );
        assert_eq!(512, RecordingSession::progress_mark(0, 512, true));
        assert_eq!(
            0,
            RecordingSession::progress_mark(0, 512, false),
            "★ a dropped event leaves the mark alone, so the next turn sends it"
        );
        assert_eq!(
            512,
            RecordingSession::progress_mark(512, 512, true),
            "and a mark that has caught up never sends the same position twice"
        );
        assert_eq!(
            512,
            RecordingSession::progress_mark(512, 256, true),
            "a recording does not go backwards"
        );
    }

    /// With no events publication — `aeron.archive.recording.events.enabled`
    /// false, which is the reference's default — the session runs exactly as it
    /// did before there were events at all.
    ///
    /// The mutation this would catch is unrepresentable rather than merely
    /// untested: the parameter is `Option<&mut _>` because the reference's three
    /// landing points test `null != recordingEventsProxy` (`:134`, `:146`,
    /// `:160`), so "send anyway" has nothing to send with.
    #[test]
    fn a_session_with_no_events_records_just_the_same() {
        let dir = crate::mark::tests::TempDir::new();
        let counters = counters();
        let mut stats = NoStats;
        let mut session = a_session(dir.path(), 7);

        assert_eq!(Ok(1), session.do_work(&counters, &mut stats, None));
        assert_eq!(State::Recording, session.state());

        session.image = Box::new(FakeImage {
            closed: true,
            ..FakeImage::new()
        });

        assert_eq!(Ok(2), session.do_work(&counters, &mut stats, None));
        assert_eq!(State::Stopped, session.state());
    }

    /// A publication at its maximum position ends the turn with the reference's
    /// error, and it ends it **before** the state moves: `started` is where the
    /// reference throws out of `init`, so a session whose first event could not
    /// go out is still in `INIT` (`RecordingSession.java:218-231`).
    ///
    /// This is the only way `do_work` fails, and what becomes of it — the
    /// recorder stops, the conductor stops the archive — is
    /// [`crate::server::conductor::ArchiveError`]'s.
    #[test]
    fn a_publication_at_its_maximum_ends_the_turn() {
        let dir = crate::mark::tests::TempDir::new();
        let counters = counters();
        let mut stats = NoStats;
        let mut events = FakeEvents {
            exhausted: true,
            ..FakeEvents::new()
        };
        let mut session = a_session(dir.path(), 7);

        assert_eq!(
            Err(EventsError::Exhausted {
                term_length: 64 * 1024
            }),
            session.do_work(&counters, &mut stats, as_events(&mut events))
        );
        assert_eq!(State::Init, session.state());
        assert!(events.calls.is_empty());
    }

    /// Move the session's writer on by writing bytes into it, which is what a
    /// poll of an image with a mapped term would do.
    fn write_bytes(session: &mut RecordingSession, bytes: usize) {
        session.writer.init().expect("the first segment opens");
        session
            .writer
            .write_block(&vec![0; bytes], &mut NoStats)
            .expect("the block is written");
    }

    /// One event publication, as the `&mut dyn` a turn is handed.
    fn as_events(events: &mut FakeEvents) -> Option<&mut dyn RecordingEvents> {
        Some(events)
    }

    /// A session with the ids the tests use, a segment directory, an image that
    /// answers without a file, and nothing else.
    fn a_session(directory: &Path, recording_id: i64) -> RecordingSession {
        RecordingSession::new(
            5,
            3,
            recording_id,
            0,
            0,
            128 * 1024,
            9,
            Box::new(FakeImage::new()),
            42,
            1001,
            "aeron:udp?endpoint=localhost:9000".to_owned(),
            "aeron:udp?endpoint=localhost:8000".to_owned(),
            1024 * 1024,
            0,
            false,
            RecordingPos::for_test(1),
            directory,
            None,
        )
    }

    /// A values region with room for the counters a turn is handed.
    ///
    /// The same helper `recorder`'s and `control_session`'s tests keep their own
    /// copy of, for the same reason: it is ten lines of alignment and the
    /// alternative is a shared test module for three callers.
    fn counters() -> CountersReader<'static, ReadWrite> {
        #[repr(align(64))]
        struct Region([u8; 64 * 64]);

        let metadata: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));
        let values: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));

        CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut metadata.0).expect("aligned region"),
            AtomicBuffer::from_slice_mut(&mut values.0).expect("aligned region"),
        )
    }

    impl RecordingSession {
        /// The one turn that needs no client: `INIT`, which opens a file and
        /// nothing else.
        fn do_work_without_a_client(&mut self) -> Result<usize, EventsError> {
            self.init(&counters(), None)
        }
    }
}
