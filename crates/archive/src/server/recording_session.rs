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
//! # The image is not held, and `isClosed` is not a flag
//!
//! The reference's session holds the `Image` object (`:60`) and asks it
//! `isEndOfStream()` and `isClosed()` (`:242`), where `isClosed` is a field its
//! client conductor sets when the driver says the image is unavailable
//! (`Image.java:204-207`).
//!
//! This build's client **removes** an unavailable image from its subscription
//! and queues an event about it (`crates/client/src/client.rs`, the
//! `UnavailableImage` arm) — there is no closed-but-present image, so there is
//! no flag to read. What there is instead is the lookup: the session names its
//! image by the two ids this client knows it by, and an image that is gone is
//! one [`Client::block_poll_image`] answers `None` for. That `None` **is**
//! `image.isClosed()` here, and it is why the reference's `||` has one arm
//! rather than two.
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

use std::path::Path;

use deepmsg_client::client::Client;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::recording_writer::{RecordingWriter, WriteStats};
use crate::server::recording_pos::RecordingPos;

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
/// `read` is `None` when the client no longer holds the image, which is this
/// build's `image.isClosed()` — see the module note.
///
/// The order matters and it is the reference's: `workCount > 0` is tested
/// **first**, so a turn that read the last bytes of a stream records them and
/// leaves the ending to the next turn.
fn step(read: Option<usize>, is_end_of_stream: Option<bool>) -> Step {
    match read {
        None => Step::Ended,
        Some(0) if is_end_of_stream == Some(true) => Step::Ended,
        Some(0) => Step::Idle,
        Some(read) => Step::Recorded(read),
    }
}

/// One recording, from its first segment to its last.
///
/// The fields are the reference's (`RecordingSession.java:34-56`), less the two
/// this build has no counterpart for: `recordingEventsProxy` (the C suite runs
/// with `recording.events.enabled=false`, and this slice refuses that setting
/// being true) and the error handler (the conductor's, reached through the
/// conductor).
pub struct RecordingSession {
    /// The start request's correlation id, which is what a `START`/`STOP`
    /// signal carries and what a failure is answered on (`:2051`).
    correlation_id: i64,
    /// The catalog's id for this recording, which is also the session's own
    /// (`RecordingSession.sessionId()`, `:85-89`).
    recording_id: i64,
    /// The subscription the image arrived on, and the image itself, named the
    /// way this client names them.
    subscription_id: i64,
    image_id: i64,
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
    /// Neither the image's MTU nor the channel the client asked for is a
    /// parameter: the reference's session carries both only to hand them to the
    /// recording events (`:220-229`), which this slice has none of, and the
    /// catalog has already been given them by the conductor that made this
    /// session.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // one per thing the reference's constructor takes
    pub fn new(
        correlation_id: i64,
        recording_id: i64,
        start_position: i64,
        join_position: i64,
        segment_file_length: usize,
        subscription_id: i64,
        image_id: i64,
        term_buffer_length: i32,
        file_io_max_length: usize,
        file_sync_level: i32,
        is_auto_stop: bool,
        position: RecordingPos,
        directory: &Path,
        checksum: Option<crate::checksum::Checksum>,
    ) -> Self {
        // `min(image.termBufferLength(), ctx.fileIoMaxLength())` (`:81`): a turn
        // reads at most a term, and at most what the archive was told a file
        // write may be.
        let file_io_max_length = i32::try_from(file_io_max_length).unwrap_or(i32::MAX);
        let block_length_limit =
            usize::try_from(term_buffer_length.min(file_io_max_length)).unwrap_or(0);

        Self {
            correlation_id,
            recording_id,
            subscription_id,
            image_id,
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
    pub fn do_work<S: WriteStats>(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        stats: &mut S,
    ) -> usize {
        let mut work = 0;

        if self.is_aborted {
            self.state = State::Inactive;
        }

        if self.state == State::Init {
            work += self.init();
        }

        if self.state == State::Recording {
            work += self.record(client, counters, stats);
        }

        if self.state == State::Inactive {
            self.state = State::Stopped;
            self.writer.close();
            work += 1;
        }

        work
    }

    /// `INIT`: open the first segment (`RecordingSession.init`, `:204-231`).
    ///
    /// A writer that will not open is a recording that never starts, and the
    /// reference answers that by keeping the message for the client and moving
    /// straight to `STOPPED` — there is nothing to record into, so there is
    /// nothing to try again for.
    fn init(&mut self) -> usize {
        if let Err(error) = self.writer.init() {
            self.error_message = Some(error.to_string());
            self.writer.close();
            self.state = State::Stopped;

            return 1;
        }

        self.state = State::Recording;

        1
    }

    /// `RECORDING`: read one run of frames and write it
    /// (`RecordingSession.record`, `:233-278`).
    ///
    /// The whole turn is here rather than in a helper because the poll borrows
    /// the client, the writer and the scratch buffer at once, and splitting it
    /// would mean handing three borrows across a call for no gain.
    fn record<S: WriteStats>(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        stats: &mut S,
    ) -> usize {
        let (read, failure) = {
            let Self {
                subscription_id,
                image_id,
                block_length_limit,
                writer,
                scratch,
                ..
            } = self;

            let mut failure = None;

            let read = client.block_poll_image(
                *subscription_id,
                *image_id,
                *block_length_limit,
                |block| {
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
                },
            );

            (read, failure)
        };

        if let Some(error) = failure {
            self.error_message = Some(error.to_string());
            self.state = State::Inactive;

            return 1;
        }

        // Asked only when nothing was read, which is where the reference asks
        // it too (`:242`) — and of the client, because the session does not hold
        // the image.
        let is_end_of_stream = if read == Some(0) {
            client
                .subscription(self.subscription_id)
                .and_then(|subscription| subscription.image(self.image_id))
                .and_then(deepmsg_client::image::Image::is_end_of_stream)
        } else {
            None
        };

        match step(read, is_end_of_stream) {
            Step::Recorded(read) => {
                self.position.set_position(counters, self.writer.position());

                read
            }
            Step::Idle => 0,
            Step::Ended => {
                self.state = State::Inactive;

                1
            }
        }
    }
}

impl std::fmt::Debug for RecordingSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingSession")
            .field("recording_id", &self.recording_id)
            .field("correlation_id", &self.correlation_id)
            .field("subscription_id", &self.subscription_id)
            .field("image_id", &self.image_id)
            .field("state", &self.state)
            .field("position", &self.writer.position())
            .field("abort_reason", &self.abort_reason)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three branches `record` takes, and the one order that matters: a
    /// turn that read the last bytes of a stream records them (`:238-246`).
    #[test]
    fn a_turn_ends_the_recording_only_when_nothing_was_read() {
        assert_eq!(Step::Recorded(64), step(Some(64), None));
        assert_eq!(
            Step::Recorded(64),
            step(Some(64), Some(true)),
            "the read is tested first, so its bytes are not thrown away"
        );

        assert_eq!(Step::Idle, step(Some(0), Some(false)));
        assert_eq!(
            Step::Idle,
            step(Some(0), None),
            "an image whose metadata cannot be read is not one that ended"
        );

        assert_eq!(Step::Ended, step(Some(0), Some(true)));
        assert_eq!(
            Step::Ended,
            step(None, None),
            "an image the client no longer holds is this build's `isClosed`"
        );
    }

    /// A recording whose first segment is already there never starts, and the
    /// message it will send the client is the one the writer gave it
    /// (`RecordingSession.java:206-216`).
    #[test]
    fn a_session_that_cannot_open_its_segment_is_stopped_with_the_error() {
        let dir = crate::mark::tests::TempDir::new();
        let recording_id = 7;

        // A segment of this recording already there is what the writer refuses
        // (`SegmentWriter.create`, over `RecordingWriter.init`).
        let existing = crate::segment::segment_file_name(recording_id, 0);
        std::fs::write(dir.path().join(existing), b"in the way").expect("written");

        let mut session = a_session(dir.path(), recording_id);

        assert_eq!(1, session.do_work_without_a_client());
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
        assert_eq!(3, session.correlation_id());
        assert_eq!(9, session.subscription_id());
        assert!(!session.is_auto_stop());
        assert!(session.error_message().is_none());
    }

    /// A session with the ids the tests use, a segment directory, and nothing
    /// else: the pieces that need a client are what `do_work` is for.
    fn a_session(directory: &Path, recording_id: i64) -> RecordingSession {
        RecordingSession::new(
            3,
            recording_id,
            0,
            0,
            128 * 1024,
            9,
            11,
            64 * 1024,
            1024 * 1024,
            0,
            false,
            RecordingPos::for_test(1),
            directory,
            None,
        )
    }

    impl RecordingSession {
        /// The one turn that needs no client: `INIT`, which opens a file and
        /// nothing else.
        fn do_work_without_a_client(&mut self) -> usize {
            self.init()
        }
    }
}
