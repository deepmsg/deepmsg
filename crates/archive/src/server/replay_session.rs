//! Replaying a recording: reading its frames back out and offering them to a
//! new stream.
//!
//! Mirrors `io.aeron.archive.ReplaySession` (`ReplaySession.java`). A replay is
//! the archive **publishing**: it opens an exclusive publication on the replay
//! channel and offers the recording's frames into it, which is the only place in
//! this server that writes to a stream it was not told to write to.
//!
//! # What a replay does to a frame on the way out
//!
//! Two header words, and the order between them and the checksum is the whole of
//! the trick (`ReplaySession.java:409-423`):
//!
//! * an archive that records with a checksum stores it **in the frame's
//!   session-id field** (`RecordingWriter.computeChecksum`, `:198-212`), so the
//!   checksum has to be read out and verified **before** the two words below
//!   overwrite it. Reverse the order and every frame of a checksummed recording
//!   fails, with nothing in the failure to say why;
//! * `SESSION_ID_FIELD_OFFSET` and `STREAM_ID_FIELD_OFFSET` are then stamped with
//!   **the replay publication's** ids, because the frames leave on a different
//!   stream than they arrived on.
//!
//! # How a turn is batched
//!
//! Whole aligned data frames accumulate into one block and go out in **one**
//! `offerBlock`; a padding frame **ends the batch** and is emitted afterwards
//! with `appendPadding` (`:329-334`, `:436-457`). A trailing frame the read did
//! not get in full ends the batch too, and an offer that is refused drops the
//! padding with it — a padding frame on its own would be a term boundary the
//! stream never had.
//!
//! # One deliberate divergence
//!
//! The reference's frame loop (`:397-430`) has a branch for `HDR_TYPE_DATA` and
//! one for `HDR_TYPE_PAD` and **no third**: a frame of any other type — an ATS
//! data frame is type 8 — matches neither, leaves `batchOffset` where it was, and
//! the `while` condition stays true. That is an infinite loop inside the
//! replayer. This build refuses such a frame instead, with the same shape of
//! error the zero-length case gets. Recorded in
//! `analysis/archive/deepmsg-p2-s4-plan-replay.md` §5.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use deepmsg_core::clock::monotonic_nano_time;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::descriptor::{FRAME_ALIGNMENT, TERM_MAX_LENGTH};
use deepmsg_core::logbuffer::frame::{
    DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, SESSION_ID_FIELD_OFFSET, STREAM_ID_FIELD_OFFSET,
    TERM_ID_FIELD_OFFSET, TERM_OFFSET_FIELD_OFFSET, TYPE_DATA, TYPE_OFFSET, TYPE_PAD,
};
use deepmsg_core::logbuffer::position::{align_up, bits_to_shift};

use crate::checksum::Checksum;
use crate::segment::{Placement, SegmentSummary, segment_file_name};

/// `ArchiveException.GENERIC` (`ArchiveException.java:29`).
pub(crate) const GENERIC: i32 = 0;

/// `ArchiveException.INVALID_POSITION` (`ArchiveException.java:109`).
pub(crate) const INVALID_POSITION: i32 = 16;

/// `ReplaySession.State` (`ReplaySession.java:76-79`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Waiting for the segment file, then for the publication to connect.
    Init,
    /// Reading and offering.
    Replay,
    /// Over; the reason, if there is one, is what the client is owed.
    Inactive,
    /// Reaped.
    Done,
}

/// The replay publication, as one session uses it.
///
/// It is a trait for the reason `walk_recordings` takes `send` as a parameter:
/// the batching, the padding and the two header words are where the fiddly part
/// is, and none of it is reachable from a test unless the publication can be
/// stood in for. The thing it stands in for is the reference's
/// `Publication`/`ExclusivePublication` (`Publication.java:326`, `:462`, `:509`).
pub trait Publication {
    /// `publication.isConnected()`.
    fn is_connected(&self) -> bool;
    /// Its Aeron session id — the low half of a `replaySessionId`.
    fn session_id(&self) -> i32;
    /// Its stream id, which every replayed frame is stamped with.
    fn stream_id(&self) -> i32;
    /// Its `positionBitsToShift()`, which names the term a position falls in.
    fn position_bits_to_shift(&self) -> u32;
    /// Its `initialTermId()`, for the same reason.
    fn initial_term_id(&self) -> i32;
    /// `publication.availableWindow()`; the whole read block is gated on it.
    fn available_window(&self) -> i64;
    /// One block of whole frames, whose headers are already final.
    ///
    /// `None` is `Publication.CLOSED` — there is no such publication any more.
    fn offer_block(&mut self, block: &[u8]) -> Option<Appended>;
    /// `publication.appendPadding(length)`.
    fn append_padding(&mut self, length: usize) -> Option<Appended>;
}

/// What one turn did, in the terms the conductor has to act on.
#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// Nothing was read and nothing went out.
    Idle,
    /// The segment opened and the header checked: the OK carrying the
    /// `replaySessionId` goes out **now**, and exactly once
    /// (`ReplaySession.java:338`). It goes out before the publication is waited
    /// for — the class comment says otherwise (`:68`), the code does not.
    Started,
    /// The turn got somewhere: a block went out, or the publication connected.
    Worked,
    /// The replay ended, cleanly; the publication may linger.
    Finished,
    /// The replay ended badly, and this is what the client is owed
    /// (`ReplaySession.sendPendingError`, `:288-295`). `code` is the response's
    /// `relevantId`.
    Failed { code: i32, message: String },
}

/// One replay session.
///
/// The range it covers is fixed at construction — `replay_position` ..
/// `replay_limit` — and the **stop position it reads to** is kept separately so
/// that a bounded or following replay can grow it. A live limit is not part of
/// this slice; the field is here so the extension lands in one place.
pub struct ReplaySession {
    recording_id: i64,
    replay_session_id: i64,
    summary: SegmentSummary,
    start_position: i64,
    stop_position: i64,
    replay_position: i64,
    replay_limit: i64,
    directory: PathBuf,
    placement: Placement,
    term_length: usize,
    file: Option<File>,
    buffer: Vec<u8>,
    checksum: Option<Checksum>,
    state: State,
    error: Option<(i32, String)>,
    revoke: bool,
    aborted: bool,
    /// What this turn's read was: the bytes `readRecording` answered with, and
    /// the nanoseconds the read and the frame walk took — the two numbers the
    /// replayer's counters 108–110 are fed (`ArchiveConductor.java:431-434`).
    ///
    /// **This turn's**, not a running total: the totals are the replayer's
    /// (`crate::server::replayer::ReadTotals`), which is where the reference
    /// keeps them too, and a session that added up its own would be a second
    /// copy of the same number.
    read_bytes: usize,
    read_time_ns: u64,
    connect_deadline_ns: u64,
}

impl ReplaySession {
    /// Open a session over `summary`'s recording.
    ///
    /// `replay_position` is where to start and `replay_length` how much to send;
    /// `stop_position` is where the recording currently ends, which is both the
    /// empty-replay check and the bound on every read.
    ///
    /// # Errors
    ///
    /// A term length that is not a power of two is a recording this build cannot
    /// place a position in at all.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        recording_id: i64,
        replay_session_id: i64,
        summary: SegmentSummary,
        replay_position: i64,
        replay_length: i64,
        start_position: i64,
        stop_position: i64,
        directory: &Path,
        buffer_capacity: usize,
        checksum: Option<Checksum>,
        connect_deadline_ns: u64,
    ) -> Result<Self, String> {
        if bits_to_shift(summary.term_buffer_length).is_none() {
            return Err(format!(
                "term buffer length {} is not a power of two",
                summary.term_buffer_length
            ));
        }

        Ok(Self {
            recording_id,
            replay_session_id,
            summary,
            start_position,
            stop_position,
            replay_position,
            replay_limit: replay_position + replay_length,
            directory: directory.to_path_buf(),
            placement: Placement::new(&summary, replay_position),
            term_length: summary.term_buffer_length as usize,
            file: None,
            buffer: vec![0_u8; buffer_capacity],
            checksum,
            state: State::Init,
            error: None,
            revoke: false,
            aborted: false,
            read_bytes: 0,
            read_time_ns: 0,
            connect_deadline_ns,
        })
    }

    /// The id the OK response carries as its `relevantId`.
    #[must_use]
    pub const fn replay_session_id(&self) -> i64 {
        self.replay_session_id
    }

    /// The recording being replayed.
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Where the session is.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// How far the replay has got.
    #[must_use]
    pub const fn replay_position(&self) -> i64 {
        self.replay_position
    }

    /// Where it stops, which a bounded or following replay may move.
    #[must_use]
    pub const fn stop_position(&self) -> i64 {
        self.stop_position
    }

    /// `notExtended`'s live half (`ReplaySession.java:544-578`): a recording
    /// that is still being written moves its stop position forward.
    ///
    /// It **only ever moves forward** — the reference's `Math.max`, and the rule
    /// that keeps a following replay from rewinding when the value it reads is
    /// momentarily behind where it has already got to.
    ///
    /// The conductor reads it, not the session: a recording's position lives in
    /// the driver's counters, which a session has no way to reach.
    pub fn extend_stop_position(&mut self, position: i64) {
        if position > self.stop_position {
            self.stop_position = position;
        }
    }

    /// `notExtended`'s **other** half (`ReplaySession.java:561-577`): the
    /// counter that was bounding this replay can no longer be read, so the
    /// replay is now bounded by where it already stood.
    ///
    /// This is the difference between a replay that ends and one that never
    /// does, and it is worth being exact about which. The reference asks the
    /// counter what it says every time the reader catches up, and when the
    /// counter has been closed or its slot **reused** the answer is the stop
    /// position it had — `replayLimit = oldStopPosition` (`:570`) — after which
    /// `replayPosition >= replayLimit` is true and the session goes `INACTIVE`
    /// (`:571-574`).
    ///
    /// Without it a bounded replay whose counter went away idles on its last
    /// stop position for the life of the archive: the session is never done, so
    /// its publication is never given back and its slot in
    /// [`ReplaySettings::max_concurrent_replays`] is never returned — the bound
    /// fills with replays that have nothing left to send and refuses every
    /// later one. A **plain** replay is not affected: it has no counter, and
    /// this is only reached for one that named a limit.
    ///
    /// [`ReplaySettings::max_concurrent_replays`]: crate::server::conductor::ReplaySettings::max_concurrent_replays
    pub fn limit_counter_gone(&mut self) {
        self.replay_limit = self.replay_limit.min(self.stop_position);
    }

    /// Whether the publication is to be torn down rather than closed.
    #[must_use]
    pub const fn is_revoking(&self) -> bool {
        self.revoke
    }

    /// The bytes this turn's read answered with, for the replayer's counters
    /// 108–110 (`ArchiveConductor.Replayer`, `:2764-2777`). Zero on a turn that
    /// read nothing.
    #[must_use]
    pub const fn read_bytes(&self) -> usize {
        self.read_bytes
    }

    /// How long this turn's read and frame walk took, in nanoseconds — the
    /// reference's `readTimeNs` (`:431`), which is measured around both
    /// (`:399` against `:431`) and not around the offer.
    #[must_use]
    pub const fn read_time_ns(&self) -> u64 {
        self.read_time_ns
    }

    /// `Session.abort` (`ReplaySession.java:249-252`): the **flag**, and
    /// nothing else.
    ///
    /// The session stops on its next turn, which is what lets `stopReplay`
    /// answer `OK` before the replay has actually stopped (`AC:1037-1054`) — the
    /// client is told its request was received, not that it has happened.
    pub const fn abort(&mut self) {
        self.aborted = true;
    }

    /// One turn: `ReplaySession.doWork` (`:209-243`).
    pub fn do_work(&mut self, publication: &mut dyn Publication, now_ns: u64) -> Progress {
        // This turn's read, which the conductor reads back after this call —
        // so it starts from nothing rather than from the last turn's.
        self.read_bytes = 0;
        self.read_time_ns = 0;

        if self.state == State::Done {
            return Progress::Idle;
        }

        if self.aborted {
            self.revoke = true;
            self.state = State::Inactive;
        }

        let progress = match self.state {
            State::Init => self.init(publication, now_ns),
            State::Replay => self.replay(publication),
            State::Inactive | State::Done => Progress::Idle,
        };

        // The reference closes the recording segment on the way out of
        // `doWork`, whichever state got it there (`:239`).
        if self.state != State::Inactive {
            return progress;
        }

        self.file = None;
        self.state = State::Done;

        match self.error.take() {
            Some((code, message)) => Progress::Failed { code, message },
            None => Progress::Finished,
        }
    }

    /// `ReplaySession.init` (`:303-356`).
    fn init(&mut self, publication: &dyn Publication, now_ns: u64) -> Progress {
        if self.file.is_none() {
            let path = self.segment_path();

            match File::open(&path) {
                Ok(file) => {
                    self.file = Some(file);

                    // A start that is not the recording's own beginning has to be
                    // a frame boundary, and the only way to know is to ask the
                    // frame there (`:328-336`). The recording's **stop** is not
                    // asked: a stop position is a boundary by construction.
                    if self.replay_position > self.start_position
                        && self.replay_position != self.stop_position
                    {
                        if let Err(message) = self.check_aligned_to_fragment(publication) {
                            return self.raise(INVALID_POSITION, message);
                        }
                    }

                    // `asyncSendOkResponse`, and it is **before** the connection
                    // is waited for (`:338`).
                    return Progress::Started;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if now_ns >= self.connect_deadline_ns {
                        return self
                            .raise(GENERIC, "recording segment file not created".to_owned());
                    }

                    return Progress::Idle;
                }
                Err(error) => return self.raise(GENERIC, error.to_string()),
            }
        }

        if !publication.is_connected() {
            if now_ns >= self.connect_deadline_ns {
                // The reference names both from the publication
                // (`ReplaySession.java:346-347`): `…for replayChannel=` +
                // `publication.channel()` + `, replayStreamId=` +
                // `publication.streamId()`. **This build can name only the
                // stream**: the channel is not on this build's publication
                // object, and plumbing it through the client's add path for one
                // message on a path only a five-second timeout reaches is not
                // worth the surface. Recorded rather than left silent.
                return self.raise(
                    GENERIC,
                    format!(
                        "no connection established for replayStreamId={}",
                        publication.stream_id()
                    ),
                );
            }

            return Progress::Idle;
        }

        self.state = State::Replay;

        Progress::Worked
    }

    /// `ReplaySession.replay` (`:358-461`).
    fn replay(&mut self, publication: &mut dyn Publication) -> Progress {
        if !publication.is_connected() {
            self.revoke = true;
            self.state = State::Inactive;

            // No message: the client sees the stream end, which is all the
            // reference tells it either (`:361-366`, and `sendPendingError`
            // sends nothing when `errorMessage` is null).
            return Progress::Idle;
        }

        if self.start_position == self.stop_position && 0 == self.replay_limit {
            self.state = State::Inactive;

            return Progress::Idle;
        }

        // The reference's next guard (`:377-381`), which is asked **here** and
        // not only where a block has just been committed: a replay whose limit
        // has stopped moving reaches its end without another block to reach it
        // by, and the commit below would never run again to notice
        // ([`ReplaySession::limit_counter_gone`]).
        //
        // For every other replay this is the same arithmetic `commit` applies
        // one turn later, and it never fires first.
        if self.replay_position >= self.replay_limit {
            self.state = State::Inactive;

            return Progress::Idle;
        }

        if self.placement.term_offset == self.term_length {
            if let Err(message) = self.next_term() {
                return self.raise(GENERIC, message);
            }
        }

        if publication.available_window() <= 0 {
            return Progress::Idle;
        }

        // The reference's `startNs` (`:399`), which is taken **before** the read
        // and read back **after** the frame walk (`:431`) — so what the
        // replayer's read-time counters measure is the file read and the
        // stamping of every frame, and not the offer that follows.
        let start_ns = monotonic_nano_time();

        let available = self.stop_position - self.replay_position;
        let bytes_read = self.read_recording(available);

        if bytes_read == 0 {
            return Progress::Idle;
        }

        let session_id = publication.session_id();
        let stream_id = publication.stream_id();
        let remaining = usize::try_from(
            (self.replay_limit - self.replay_position).clamp(0, i64::from(TERM_MAX_LENGTH)),
        )
        .unwrap_or(0);

        let mut batch_offset = 0_usize;
        let mut padding_frame_length = 0_usize;

        while batch_offset < bytes_read && batch_offset < remaining {
            let Some(frame_length) = read_i32(&self.buffer, batch_offset + FRAME_LENGTH_OFFSET)
            else {
                break;
            };

            if frame_length <= 0 {
                let position = self.replay_position + batch_offset as i64;

                return self.raise(
                    GENERIC,
                    format!("unexpected end of recording at position={position}"),
                );
            }

            let frame_type = read_i16(&self.buffer, batch_offset + TYPE_OFFSET).unwrap_or(0);
            let aligned =
                usize::try_from(align_up(frame_length, FRAME_ALIGNMENT)).unwrap_or(usize::MAX);

            if frame_type == TYPE_DATA {
                if batch_offset + aligned > bytes_read {
                    break;
                }

                if let Some(checksum) = self.checksum {
                    if let Err(message) =
                        verify_checksum(checksum, &self.buffer, batch_offset, aligned)
                    {
                        return self.raise(GENERIC, message);
                    }
                }

                put_i32(
                    &mut self.buffer,
                    batch_offset + SESSION_ID_FIELD_OFFSET,
                    session_id,
                );
                put_i32(
                    &mut self.buffer,
                    batch_offset + STREAM_ID_FIELD_OFFSET,
                    stream_id,
                );
                batch_offset += aligned;
            } else if frame_type == TYPE_PAD {
                padding_frame_length = frame_length as usize;
                break;
            } else {
                // The divergence: the reference matches neither branch, leaves
                // `batchOffset` alone and loops forever (see the module note).
                let position = self.replay_position + batch_offset as i64;

                return self.raise(
                    GENERIC,
                    format!("unexpected frame type {frame_type} at position={position}"),
                );
            }
        }

        // `replayer.bytesRead(bytesRead)` and `replayer.readTimeNs(readTimeNs)`
        // (`:431-434`), which the reference does here — after the walk, before
        // the offer.
        self.read_bytes = bytes_read;
        self.read_time_ns = monotonic_nano_time().saturating_sub(start_ns) as u64;

        if batch_offset > 0 {
            // The block is the buffer itself, offered in place — the reference
            // hands `replayBuffer` straight to `offerBlock` (`:400-403`) and
            // copies nothing. This used to `to_vec()` it, which allocated and
            // copied up to a whole buffer per batch per turn on the path every
            // replayed byte goes down, and which ADR-0003 forbids.
            //
            // The borrow ends with the call: what `offer_block` answers borrows
            // nothing, so `commit` may still take `&mut self`.
            let block = self.buffer.get(..batch_offset).unwrap_or_default();

            if !self.commit(publication.offer_block(block), batch_offset) {
                // A padding frame on its own is a term boundary the stream never
                // had, so it goes the way of the batch that did not go out.
                padding_frame_length = 0;
            }
        }

        if padding_frame_length > 0 {
            let aligned = usize::try_from(align_up(
                i32::try_from(padding_frame_length).unwrap_or(i32::MAX),
                FRAME_ALIGNMENT,
            ))
            .unwrap_or(usize::MAX);
            let length = padding_frame_length.saturating_sub(DATA_HEADER_LENGTH);

            self.commit(publication.append_padding(length), aligned);
        }

        Progress::Worked
    }

    /// What became of one offer (`hasPublicationAdvanced`, `:473-494`).
    ///
    /// A published frame moves the position on; a **closed or unconnected**
    /// publication ends the replay there and then, which is what stops a replay
    /// whose subscriber went away from spinning; anything else — a used-up
    /// window, a log mid-rotation — leaves the position alone for the next turn.
    fn commit(&mut self, outcome: Option<Appended>, aligned: usize) -> bool {
        match outcome {
            Some(Appended::Ok { .. }) => {
                self.placement.term_offset += aligned;
                self.replay_position += aligned as i64;

                if self.replay_position >= self.replay_limit {
                    self.state = State::Inactive;
                }

                true
            }
            None | Some(Appended::NotConnected) => {
                self.revoke = true;
                self.state = State::Inactive;

                false
            }
            _ => false,
        }
    }

    /// Read one block out of the current segment file (`:510-530`).
    ///
    /// The answer is **what the caller asked for**, not what the file gave — the
    /// reference returns its `limit` too, and the frame loop is what notices a
    /// short read, through a frame length of zero.
    fn read_recording(&mut self, available_replay: i64) -> usize {
        let Some(file) = self.file.as_ref() else {
            return 0;
        };

        let room = (self.term_length - self.placement.term_offset) as i64;
        let limit = available_replay
            .clamp(0, self.buffer.len() as i64)
            .min(room);
        let limit = usize::try_from(limit).unwrap_or(0);

        if limit == 0 {
            return 0;
        }

        let offset = self.placement.file_offset();
        let mut filled = 0_usize;

        while filled < limit {
            let Some(dst) = self.buffer.get_mut(filled..limit) else {
                break;
            };

            match file.read_at(dst, (offset + filled) as u64) {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(_) => break,
            }
        }

        limit
    }

    /// `nextTerm` (`:580-592`): the next term, and the next **file** when the
    /// segment runs out.
    fn next_term(&mut self) -> Result<(), String> {
        if !self.placement.next_term(&self.summary) {
            return Ok(());
        }

        // State is REPLAY by now, so `init` is not re-entered: the file is opened
        // here, and a missing one ends the replay.
        let path = self.segment_path();

        match File::open(&path) {
            Ok(file) => {
                self.file = Some(file);

                Ok(())
            }
            Err(error) => Err(format!("recording segment not found: {error}")),
        }
    }

    fn segment_path(&self) -> PathBuf {
        self.directory.join(segment_file_name(
            self.recording_id,
            self.placement.segment_file_position,
        ))
    }

    /// Whether the frame at the replay position is the one that position implies
    /// (`notHeaderAligned`, `:618-634`).
    fn check_aligned_to_fragment(&self, publication: &dyn Publication) -> Result<(), String> {
        let mut header = [0_u8; DATA_HEADER_LENGTH];
        let file = self.file.as_ref().ok_or("no segment file")?;

        file.read_exact_at(&mut header, self.placement.file_offset() as u64)
            .map_err(|error| error.to_string())?;

        let term_offset = read_i32(&header, TERM_OFFSET_FIELD_OFFSET).unwrap_or(-1);
        let term_id = read_i32(&header, TERM_ID_FIELD_OFFSET).unwrap_or(-1);
        let stream_id = read_i32(&header, STREAM_ID_FIELD_OFFSET).unwrap_or(-1);

        let expected_term_offset = i32::try_from(self.placement.term_offset).unwrap_or(-1);
        let expected_term_id =
            i32::try_from(self.replay_position >> publication.position_bits_to_shift())
                .unwrap_or(0)
                .wrapping_add(publication.initial_term_id());

        if term_offset != expected_term_offset
            || term_id != expected_term_id
            || stream_id != self.summary.stream_id
        {
            // `raiseError("replayPosition=" + framePosition(0) + " does not
            // point to a valid frame", …)` (`:332-333`), and `framePosition`
            // spells the four numbers it is made of (`:463-471`). A reference
            // test asserts this text whole (`ReplaySessionTest.java:311-313`),
            // so it is spelled the same way here.
            return Err(format!(
                "replayPosition={position} (segmentFilePosition={segment_file}, \
                 segmentOffset={segment_offset}, termOffset={header_term_offset}, frameOffset=0) \
                 does not point to a valid frame",
                position = self.replay_position,
                segment_file = self.placement.segment_file_position,
                segment_offset = self.placement.term_base_segment_offset,
                header_term_offset = self.placement.term_offset,
            ));
        }

        Ok(())
    }

    /// Record the failure and stop (`raiseError`, `:532-542`).
    ///
    /// The answer is [`Progress::Idle`] on purpose: [`ReplaySession::do_work`]
    /// turns the state into the one terminal `Progress` the client is owed, so a
    /// failure is reported exactly once however many ways got here.
    fn raise(&mut self, code: i32, message: String) -> Progress {
        self.revoke = true;
        self.state = State::Inactive;

        self.error = Some((
            code,
            format!(
                "{message}, recordingId={}, replaySessionId={}, segmentFile={}",
                self.recording_id,
                self.replay_session_id,
                self.segment_path().display()
            ),
        ));

        Progress::Idle
    }
}

/// `verifyChecksum` (`:496-508`): the recorded checksum is the frame's
/// session-id field, and the digest covers the payload only.
fn verify_checksum(
    checksum: Checksum,
    buffer: &[u8],
    frame_offset: usize,
    aligned_length: usize,
) -> Result<(), String> {
    let Some(recorded) = read_i32(buffer, frame_offset + SESSION_ID_FIELD_OFFSET) else {
        return Err("frame is shorter than its header".to_owned());
    };

    let Some(payload) =
        buffer.get(frame_offset + DATA_HEADER_LENGTH..frame_offset + aligned_length)
    else {
        return Err("frame is shorter than its header".to_owned());
    };

    let computed = checksum.compute(payload);

    if computed != recorded {
        return Err(format!(
            "CRC checksum mismatch: recorded checksum={recorded}, computed checksum={computed}"
        ));
    }

    Ok(())
}

fn read_i16(bytes: &[u8], offset: usize) -> Option<i16> {
    let raw: [u8; 2] = bytes.get(offset..offset + 2)?.try_into().ok()?;

    Some(i16::from_le_bytes(raw))
}

fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    let raw: [u8; 4] = bytes.get(offset..offset + 4)?.try_into().ok()?;

    Some(i32::from_le_bytes(raw))
}

fn put_i32(bytes: &mut [u8], offset: usize, value: i32) {
    if let Some(slot) = bytes.get_mut(offset..offset + 4) {
        slot.copy_from_slice(&value.to_le_bytes());
    }
}

/// The four things a replay stamps into every frame's header, and reads every
/// time it builds one (`ReplaySession.java:391-392`, `:320-321`).
///
/// Gathered into one call rather than four trait methods because they are
/// always wanted together and each would be a one-line delegation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationFacts {
    /// The Aeron session id the frames go out under — the low half of a
    /// `replaySessionId` (`AC:956`).
    pub session_id: i32,
    /// The stream id the frames go out under.
    pub stream_id: i32,
    /// `positionBitsToShift`, which names the term a position falls in.
    pub position_bits_to_shift: u32,
    /// `initialTermId`, for the same reason.
    pub initial_term_id: i32,
}

/// The archive's client, as far as a replay's publication goes.
///
/// Deliberately **not** `control_session::Publications`, which is the seam the
/// control session's egress uses. Past the registration id the two have nothing
/// in common — that one offers an encoded `Response` through `offer`, this one
/// offers blocks of recorded frames through `offer_block` — and keeping them
/// apart is also what keeps the control session's test doubles from having to
/// stub out calls they can never make.
pub trait ReplayPublications {
    /// `publication.isConnected()`.
    fn is_connected(&self, registration_id: i64) -> bool;

    /// The four header words. `None` when this client holds no such
    /// publication, which is where the reference's `CLOSED` lands.
    fn facts(&self, registration_id: i64) -> Option<PublicationFacts>;

    /// `Publication.availableWindow` (`:409-413`); a replay's whole read is
    /// gated on it (`:385`). `None` for no such publication.
    fn available_window(&self, registration_id: i64) -> Option<i64>;

    /// `Publication.offerBlock` (`:509`).
    fn offer_block(&mut self, registration_id: i64, block: &[u8]) -> Option<Appended>;

    /// `ExclusivePublication.appendPadding` (`:462`).
    fn append_padding(&mut self, registration_id: i64, length: usize) -> Option<Appended>;

    /// `publication.revoke()`: torn down now, with no linger — what a replay
    /// that failed or was cancelled gets (`:175-194`).
    fn release_publication(&mut self, registration_id: i64, timeout: Duration);

    /// `CloseHelper.close(publication)`: **the linger applies**, which is the
    /// whole point of it for a replay that ran to its end — the driver keeps
    /// the publication long enough to retransmit the tail.
    ///
    /// Named for the publication rather than `close` because `Client` has a
    /// `close` of its own, and a name that means two things where one of them is
    /// a whole client is a name that gets read wrong.
    fn close_publication(&mut self, registration_id: i64, timeout: Duration);
}

/// The replay publication as it actually is: a registration id at the driver,
/// reached through the client that holds it.
///
/// The four facts are read **once**, when the publication is first reached, and
/// served from then on. The reference reads them off a live object every time;
/// here the object is a registration id, and asking the client for four numbers
/// on every frame's header would be four lookups into a list for values that
/// cannot have changed — a publication's ids are settled when it is created.
pub struct Published<'a, P: ReplayPublications + ?Sized> {
    publications: &'a mut P,
    registration_id: i64,
    facts: PublicationFacts,
}

impl<'a, P: ReplayPublications + ?Sized> Published<'a, P> {
    /// Reach the publication this registration id names.
    ///
    /// `None` when the client holds none, which the conductor only reaches by
    /// holding a registration id it was never given.
    pub fn new(publications: &'a mut P, registration_id: i64) -> Option<Self> {
        let facts = publications.facts(registration_id)?;

        Some(Self {
            publications,
            registration_id,
            facts,
        })
    }
}

impl<P: ReplayPublications + ?Sized> Publication for Published<'_, P> {
    fn is_connected(&self) -> bool {
        self.publications.is_connected(self.registration_id)
    }

    fn session_id(&self) -> i32 {
        self.facts.session_id
    }

    fn stream_id(&self) -> i32 {
        self.facts.stream_id
    }

    fn position_bits_to_shift(&self) -> u32 {
        self.facts.position_bits_to_shift
    }

    fn initial_term_id(&self) -> i32 {
        self.facts.initial_term_id
    }

    fn available_window(&self) -> i64 {
        self.publications
            .available_window(self.registration_id)
            .unwrap_or(0)
    }

    fn offer_block(&mut self, block: &[u8]) -> Option<Appended> {
        self.publications.offer_block(self.registration_id, block)
    }

    fn append_padding(&mut self, length: usize) -> Option<Appended> {
        self.publications
            .append_padding(self.registration_id, length)
    }
}

impl<P: ReplayPublications + ?Sized> Published<'_, P> {
    /// `publication.revoke()`: torn down now, with no linger — what a replay
    /// that **failed or was cancelled** gets (`ReplaySession.java:175-194`).
    ///
    /// The pair below is the wrapper's own, and the **conductor** is what calls
    /// it. The reference's session closes its own publication (`:184-192`),
    /// because its conductor is the object that holds the Aeron client; here
    /// the session has no client at all, so the teardown is the conductor's and
    /// what it tears down through is this — the wrapper, which is what holds
    /// the registration id.
    pub fn revoke(&mut self) {
        self.publications
            .release_publication(self.registration_id, DEFAULT_CLOSE_TIMEOUT);
    }

    /// `publication.close()`, which honours the linger: what a replay that ran
    /// to its **end** gets, so the driver can retransmit its tail.
    pub fn close(&mut self) {
        self.publications
            .close_publication(self.registration_id, DEFAULT_CLOSE_TIMEOUT);
    }
}

/// How long the client is given to get a publication's removal to the driver.
///
/// The reference's own close waits for the driver to answer; a poll-driven
/// conductor cannot wait, and it is the client's deadline that finishes a
/// removal nobody polls for. The same one the control session's own publication
/// is closed with.
const DEFAULT_CLOSE_TIMEOUT: Duration = deepmsg_client::client::DEFAULT_TIMEOUT;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mark::tests::TempDir;
    use crate::segment::{SegmentSpec, SegmentWriter};
    use deepmsg_core::logbuffer::position::Position;

    const TERM_LENGTH: i32 = 64 * 1024;
    const SEGMENT_LENGTH: usize = 4 * TERM_LENGTH as usize;
    const BITS_TO_SHIFT: u32 = 16;
    const RECORDING_ID: i64 = 7;
    const RECORDED_STREAM_ID: i32 = 1001;
    const RECORDED_SESSION_ID: i32 = 42;
    const INITIAL_TERM_ID: i32 = 7;
    const BUFFER_CAPACITY: usize = TERM_LENGTH as usize;

    /// The replay publication's ids, which every replayed frame ends up bearing.
    const REPLAY_SESSION_ID: i32 = 99;
    const REPLAY_STREAM_ID: i32 = 55;

    /// The ATS data header type, which the reference's frame loop has no branch
    /// for (`FrameDescriptor.HDR_TYPE_ATS_DATA`).
    const TYPE_ATS_DATA: i16 = 0x08;

    /// One data frame of `payload` bytes, with the recording's own field values.
    fn frame(term_offset: i32, payload: &[u8]) -> Vec<u8> {
        frame_of(term_offset, TYPE_DATA, payload)
    }

    fn frame_of(term_offset: i32, frame_type: i16, payload: &[u8]) -> Vec<u8> {
        let length = DATA_HEADER_LENGTH + payload.len();
        let aligned = align_up(i32::try_from(length).expect("small"), FRAME_ALIGNMENT) as usize;
        let mut bytes = vec![0_u8; aligned];

        bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
        bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&frame_type.to_le_bytes());
        bytes[TERM_OFFSET_FIELD_OFFSET..TERM_OFFSET_FIELD_OFFSET + 4]
            .copy_from_slice(&term_offset.to_le_bytes());
        bytes[SESSION_ID_FIELD_OFFSET..SESSION_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&RECORDED_SESSION_ID.to_le_bytes());
        bytes[STREAM_ID_FIELD_OFFSET..STREAM_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&RECORDED_STREAM_ID.to_le_bytes());
        bytes[TERM_ID_FIELD_OFFSET..TERM_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&INITIAL_TERM_ID.to_le_bytes());
        bytes[DATA_HEADER_LENGTH..length].copy_from_slice(payload);

        bytes
    }

    /// A padding frame covering `length` bytes, as a term's tail is. The body is
    /// filled rather than left zero so that "the header alone was written" and
    /// "the whole frame was" are different bytes on disk.
    fn padding_frame(length: usize) -> Vec<u8> {
        let mut bytes = vec![0xAB_u8; length];

        bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
        bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&TYPE_PAD.to_le_bytes());

        bytes
    }

    fn spec() -> SegmentSpec {
        SegmentSpec {
            recording_id: RECORDING_ID,
            start_position: 0,
            join_position: 0,
            term_buffer_length: TERM_LENGTH,
            segment_length: SEGMENT_LENGTH,
        }
    }

    fn summary(stop: i64) -> SegmentSummary {
        SegmentSummary {
            recording_id: RECORDING_ID,
            start_position: 0,
            stop_position: Some(stop),
            initial_term_id: INITIAL_TERM_ID,
            term_buffer_length: TERM_LENGTH,
            segment_file_length: SEGMENT_LENGTH as i32,
            stream_id: RECORDED_STREAM_ID,
        }
    }

    /// Write `blocks` into a recording and answer with its summary and stop.
    fn recorded(dir: &TempDir, blocks: &[Vec<u8>]) -> (SegmentSummary, i64) {
        let mut writer = SegmentWriter::create(dir.path(), spec(), 1, None).expect("a writer");

        for block in blocks {
            writer.write_block(block).expect("written");
        }

        let stop = writer.offset() as i64;

        (summary(stop), stop)
    }

    fn reader(
        dir: &TempDir,
        summary: SegmentSummary,
        from: i64,
        length: i64,
        stop: i64,
        checksum: Option<Checksum>,
    ) -> ReplaySession {
        ReplaySession::new(
            RECORDING_ID,
            0x0000_0001_0000_0063,
            summary,
            from,
            length,
            summary.start_position,
            stop,
            dir.path(),
            BUFFER_CAPACITY,
            checksum,
            u64::MAX,
        )
        .expect("a session")
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Answer {
        Ok,
        BackPressured,
        Closed,
    }

    struct Fake {
        connected: bool,
        window: i64,
        answer: Answer,
        offers: Vec<Vec<u8>>,
        paddings: Vec<usize>,
    }

    impl Fake {
        fn connected() -> Self {
            Self {
                connected: true,
                window: TERM_LENGTH as i64,
                answer: Answer::Ok,
                offers: Vec::new(),
                paddings: Vec::new(),
            }
        }

        fn outcome(&self) -> Option<Appended> {
            match self.answer {
                Answer::Ok => Some(Appended::Ok {
                    position: Position::from_term_count(0, 0, BITS_TO_SHIFT),
                    term_offset: 0,
                }),
                Answer::BackPressured => Some(Appended::BackPressured),
                Answer::Closed => None,
            }
        }
    }

    impl Publication for Fake {
        fn is_connected(&self) -> bool {
            self.connected
        }

        fn session_id(&self) -> i32 {
            REPLAY_SESSION_ID
        }

        fn stream_id(&self) -> i32 {
            REPLAY_STREAM_ID
        }

        fn position_bits_to_shift(&self) -> u32 {
            BITS_TO_SHIFT
        }

        fn initial_term_id(&self) -> i32 {
            INITIAL_TERM_ID
        }

        fn available_window(&self) -> i64 {
            self.window
        }

        fn offer_block(&mut self, block: &[u8]) -> Option<Appended> {
            self.offers.push(block.to_vec());

            self.outcome()
        }

        fn append_padding(&mut self, length: usize) -> Option<Appended> {
            self.paddings.push(length);

            self.outcome()
        }
    }

    /// Drive until the session ends, answering every turn.
    fn run(session: &mut ReplaySession, publication: &mut Fake) -> Vec<Progress> {
        let mut seen = Vec::new();

        for _ in 0..32 {
            let progress = session.do_work(publication, 0);
            let done = matches!(progress, Progress::Finished | Progress::Failed { .. });

            seen.push(progress);

            if done {
                break;
            }
        }

        seen
    }

    /// The OK goes out **before** the publication is waited for
    /// (`ReplaySession.java:338`, and the class comment at `:68` says otherwise).
    #[test]
    fn the_ok_goes_out_before_the_publication_connects() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"one")];
        let (summary, stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        publication.connected = false;

        let mut session = reader(&dir, summary, 0, stop, stop, None);

        assert_eq!(Progress::Started, session.do_work(&mut publication, 0));
        assert!(!publication.is_connected());
        assert_eq!(
            State::Init,
            session.state(),
            "and it is still waiting to connect"
        );

        // The connection turn is what moves it on.
        publication.connected = true;
        assert_eq!(Progress::Worked, session.do_work(&mut publication, 0));
        assert_eq!(State::Replay, session.state());
    }

    /// What went in comes back out, and every frame bears the **replay
    /// publication's** ids rather than the recorded ones.
    #[test]
    fn a_recording_replays_back_with_the_publications_ids() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"the first message"), frame(64, b"second")];
        let (summary, stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 0, stop, stop, None);

        let seen = run(&mut session, &mut publication);

        assert!(seen.contains(&Progress::Finished), "{seen:?}");
        assert_eq!(1, publication.offers.len(), "one batch, one offer");

        let block = &publication.offers[0];
        assert_eq!(128, block.len(), "both whole frames, in one block");

        for offset in [0_usize, 64] {
            assert_eq!(
                REPLAY_SESSION_ID,
                read_i32(block, offset + SESSION_ID_FIELD_OFFSET).expect("stamped")
            );
            assert_eq!(
                REPLAY_STREAM_ID,
                read_i32(block, offset + STREAM_ID_FIELD_OFFSET).expect("stamped")
            );
        }

        assert_eq!(b"the first message", &block[32..49]);
        assert_eq!(b"second", &block[96..102]);
    }

    /// A bounded replay's stop position moves forward and the replay goes on
    /// with it (`notExtended`'s live half, `ReplaySession.java:561-567`), and
    /// never backwards (`:563`).
    #[test]
    fn a_limit_that_moves_takes_the_replay_with_it() {
        let dir = TempDir::new();
        let blocks = vec![
            frame(0, b"one"),
            frame(64, b"two"),
            frame(128, b"three"),
            frame(192, b"four"),
        ];
        let (summary, stop) = recorded(&dir, &blocks);
        assert_eq!(256, stop, "four 64-byte frames");

        // A following replay: `ARCHIVE_REPLAY_ALL_AND_FOLLOW` is `NULL_LENGTH`,
        // so the request's own length bounds nothing and the limit is the whole
        // of what the replay reads to.
        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 0, i64::MAX, 128, None);

        run(&mut session, &mut publication);
        assert_eq!(1, publication.offers.len(), "as far as the limit");
        assert_eq!(128, session.replay_position());
        assert_ne!(State::Done, session.state(), "and waiting for more");

        session.extend_stop_position(256);
        run(&mut session, &mut publication);

        assert_eq!(2, publication.offers.len(), "the rest went out");
        assert_eq!(256, session.replay_position());

        // And it is still **not** over, which is the whole difference between
        // this and a limit that has gone: a following replay whose limit stands
        // on the end of the recording waits there, because nothing has said
        // there will be no more.
        assert_ne!(State::Done, session.state());
        assert_eq!(128, publication.offers[1].len(), "two more frames");
    }

    /// A limit that has **gone** is a replay that is over: the counter was
    /// closed or its slot reused, and the reference clamps what the replay may
    /// send to where it already stands and takes it to `INACTIVE`
    /// (`ReplaySession.java:568-574`).
    ///
    /// Without the clamp the session waits for a counter nobody will ever write
    /// again, and it waits holding its slot in the archive's replay bound.
    #[test]
    fn a_limit_that_has_gone_ends_the_replay() {
        let dir = TempDir::new();
        let blocks = vec![
            frame(0, b"one"),
            frame(64, b"two"),
            frame(128, b"three"),
            frame(192, b"four"),
        ];
        let (summary, _stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 0, i64::MAX, 128, None);

        run(&mut session, &mut publication);
        assert_eq!(128, session.replay_position(), "caught up to the limit");
        assert_ne!(State::Done, session.state(), "and it was still live");

        session.limit_counter_gone();
        let seen = run(&mut session, &mut publication);

        assert!(seen.contains(&Progress::Finished), "{seen:?}");
        assert_eq!(State::Done, session.state());
        assert_eq!(
            1,
            publication.offers.len(),
            "and it sent nothing more than it had"
        );
        assert_eq!(128, session.replay_position());
    }

    /// A padding frame **ends the batch** and goes out on its own, as a padding
    /// frame — one term boundary, preserved (`ReplaySession.java:329-334`).
    #[test]
    fn padding_ends_the_batch_and_goes_out_on_its_own() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"one"), padding_frame(64), frame(128, b"two")];
        let (summary, stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 0, stop, stop, None);

        let seen = run(&mut session, &mut publication);

        assert!(seen.contains(&Progress::Finished), "{seen:?}");
        assert_eq!(
            2,
            publication.offers.len(),
            "the padding ended the first batch"
        );
        assert_eq!(64, publication.offers[0].len());
        assert_eq!(64, publication.offers[1].len());
        assert_eq!(
            vec![64 - DATA_HEADER_LENGTH],
            publication.paddings,
            "the padding frame less its header, which is what appendPadding takes"
        );
    }

    /// The reference's frame loop matches `DATA` and `PAD` and nothing else, so a
    /// frame of any other type leaves the batch where it was and the loop never
    /// ends. This build refuses it instead.
    #[test]
    fn a_frame_type_that_is_neither_data_nor_padding_ends_the_replay() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"one"), frame_of(64, TYPE_ATS_DATA, b"two")];
        let (summary, stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 0, stop, stop, None);

        let seen = run(&mut session, &mut publication);
        let last = seen.last().expect("a turn");

        match last {
            Progress::Failed { code, message } => {
                assert_eq!(GENERIC, *code);
                assert!(message.contains("unexpected frame type 8"), "{message}");
            }
            other => panic!("expected a failure, got {other:?}"),
        }

        assert!(
            session.is_revoking(),
            "a failed replay revokes the publication"
        );
    }

    /// An offer the driver refuses moves nothing: the frame is offered again on
    /// the next turn, from the same position.
    #[test]
    fn a_refused_offer_leaves_the_position_where_it_was() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"one"), frame(64, b"two")];
        let (summary, stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        publication.answer = Answer::BackPressured;
        let mut session = reader(&dir, summary, 0, stop, stop, None);

        let seen = run(&mut session, &mut publication);

        assert_eq!(State::Replay, session.state(), "still going");
        assert_eq!(0, session.replay_position(), "and still at the start");
        assert!(
            publication.offers.len() > 1,
            "the same frames are offered again on the next turn"
        );
        assert_eq!(
            seen.len() - 2,
            publication.offers.len(),
            "every turn after Started and the connect offered once"
        );
        assert!(
            !seen.contains(&Progress::Finished),
            "a refused offer is not an ending"
        );
    }

    /// A publication that has gone away ends the replay and is torn down rather
    /// than closed, so it does not linger for a subscriber that is not there
    /// (`ReplaySession.java:175-194`).
    #[test]
    fn a_closed_publication_ends_the_replay_and_revokes() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"one")];
        let (summary, stop) = recorded(&dir, &blocks);

        let mut publication = Fake::connected();
        publication.answer = Answer::Closed;
        let mut session = reader(&dir, summary, 0, stop, stop, None);

        let seen = run(&mut session, &mut publication);

        assert!(seen.contains(&Progress::Finished), "{seen:?}");
        assert!(session.is_revoking());
        assert_eq!(State::Done, session.state());
    }

    /// A start that is not the recording's own beginning has to be a **frame
    /// boundary**, and the frame there is what says so (`:328-336`).
    #[test]
    fn a_late_join_position_is_checked_against_the_frame_that_is_there() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"the first message"), frame(64, b"second")];
        let (summary, stop) = recorded(&dir, &blocks);

        // A real boundary: the second frame starts here.
        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 64, stop - 64, stop, None);

        let seen = run(&mut session, &mut publication);

        assert!(seen.contains(&Progress::Finished), "{seen:?}");
        assert_eq!(1, publication.offers.len());
        assert_eq!(64, publication.offers[0].len(), "just the second frame");
        assert_eq!(b"second", &publication.offers[0][32..38]);

        // Not a boundary: 32 bytes in is the middle of nothing.
        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary, 32, stop - 32, stop, None);

        let seen = run(&mut session, &mut publication);

        match seen.last().expect("a turn") {
            Progress::Failed { code, message } => {
                assert_eq!(INVALID_POSITION, *code);
                assert!(
                    message.contains("does not point to a valid frame"),
                    "{message}"
                );
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// The checksum an archive records lives **in the frame's session-id field**,
    /// so it has to be verified before that field is stamped with the replay
    /// publication's id (`:409-423`). Verified against a tampered payload, which
    /// is the only way to see that the check ran at all.
    #[test]
    fn a_checksummed_recording_is_verified_before_the_ids_are_stamped() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, b"the first message")];

        let mut writer =
            SegmentWriter::create(dir.path(), spec(), 1, Some(Checksum::Crc32)).expect("a writer");

        for block in &blocks {
            writer.write_block(block).expect("written");
        }

        let stop = writer.offset() as i64;
        drop(writer);

        // Untampered: it replays, and the id field ends up the publication's.
        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary(stop), 0, stop, stop, Some(Checksum::Crc32));

        let seen = run(&mut session, &mut publication);

        assert!(seen.contains(&Progress::Finished), "{seen:?}");
        assert_eq!(
            REPLAY_SESSION_ID,
            read_i32(&publication.offers[0], SESSION_ID_FIELD_OFFSET).expect("stamped")
        );

        // Tampered: one payload byte, and the checksum no longer matches.
        let path = dir.path().join(segment_file_name(RECORDING_ID, 0));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("the segment");
        let payload = [blocks[0][DATA_HEADER_LENGTH] ^ 0xFF];

        file.write_all_at(&payload, DATA_HEADER_LENGTH as u64)
            .expect("tampered");
        drop(file);

        let mut publication = Fake::connected();
        let mut session = reader(&dir, summary(stop), 0, stop, stop, Some(Checksum::Crc32));

        let seen = run(&mut session, &mut publication);

        match seen.last().expect("a turn") {
            Progress::Failed { code, message } => {
                assert_eq!(GENERIC, *code);
                assert!(message.contains("CRC checksum mismatch"), "{message}");
            }
            other => panic!("expected a checksum failure, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod published_tests {
    use super::*;
    use deepmsg_core::logbuffer::position::Position;

    const REGISTRATION_ID: i64 = 0x5EED;

    const FACTS: PublicationFacts = PublicationFacts {
        session_id: 99,
        stream_id: 55,
        position_bits_to_shift: 16,
        initial_term_id: 7,
    };

    #[derive(Default)]
    struct Fake {
        connected: bool,
        window: Option<i64>,
        outcome: Option<Appended>,
        counts: Vec<(i64, usize)>,
        released: Vec<i64>,
        closed: Vec<i64>,
    }

    impl ReplayPublications for Fake {
        fn is_connected(&self, _registration_id: i64) -> bool {
            self.connected
        }

        fn facts(&self, registration_id: i64) -> Option<PublicationFacts> {
            (registration_id == REGISTRATION_ID).then_some(FACTS)
        }

        fn available_window(&self, _registration_id: i64) -> Option<i64> {
            self.window
        }

        fn offer_block(&mut self, registration_id: i64, block: &[u8]) -> Option<Appended> {
            self.counts.push((registration_id, block.len()));

            self.outcome
        }

        fn append_padding(&mut self, registration_id: i64, length: usize) -> Option<Appended> {
            self.counts.push((registration_id, length));

            self.outcome
        }

        fn release_publication(&mut self, registration_id: i64, _timeout: Duration) {
            self.released.push(registration_id);
        }

        fn close_publication(&mut self, registration_id: i64, _timeout: Duration) {
            self.closed.push(registration_id);
        }
    }

    fn ok() -> Appended {
        Appended::Ok {
            position: Position::from_term_count(0, 0, FACTS.position_bits_to_shift),
            term_offset: 0,
        }
    }

    /// A registration id the client does not hold is not a publication, and
    /// saying so is the adapter's whole job at construction.
    #[test]
    fn a_registration_the_client_does_not_hold_is_not_a_publication() {
        let mut fake = Fake::default();

        assert!(Published::new(&mut fake, REGISTRATION_ID + 1).is_none());
        assert!(Published::new(&mut fake, REGISTRATION_ID).is_some());
    }

    /// The four header words come off the publication, not out of thin air.
    #[test]
    fn the_four_header_words_are_the_publications() {
        let mut fake = Fake::default();
        let published = Published::new(&mut fake, REGISTRATION_ID).expect("a publication");

        assert_eq!(99, published.session_id());
        assert_eq!(55, published.stream_id());
        assert_eq!(16, published.position_bits_to_shift());
        assert_eq!(7, published.initial_term_id());
    }

    /// Offers and paddings go out under the registration id, and a client that
    /// no longer holds the publication answers `None` — which the session reads
    /// as `CLOSED`.
    #[test]
    fn offers_go_out_under_the_registration_id() {
        let mut fake = Fake {
            outcome: Some(ok()),
            ..Fake::default()
        };
        {
            let mut published = Published::new(&mut fake, REGISTRATION_ID).expect("a publication");

            assert!(matches!(
                published.offer_block(b"frames"),
                Some(Appended::Ok { .. })
            ));
            assert!(matches!(
                published.append_padding(32),
                Some(Appended::Ok { .. })
            ));
        }

        assert_eq!(
            vec![(REGISTRATION_ID, 6), (REGISTRATION_ID, 32)],
            fake.counts
        );

        // And a publication the client has let go of says so.
        fake.outcome = None;

        let mut published = Published::new(&mut fake, REGISTRATION_ID).expect("a publication");

        assert!(published.offer_block(b"frames").is_none());
    }

    /// A window the client cannot answer for is no window at all, so the next
    /// turn reads nothing rather than reading into a full one.
    #[test]
    fn a_window_the_client_cannot_answer_for_is_zero() {
        let mut fake = Fake::default();

        {
            let published = Published::new(&mut fake, REGISTRATION_ID).expect("a publication");

            assert_eq!(0, published.available_window());
        }

        fake.window = Some(4096);

        let published = Published::new(&mut fake, REGISTRATION_ID).expect("a publication");

        assert_eq!(4096, published.available_window());
    }

    /// **Closing is not revoking**, and the difference is the linger: a replay
    /// that ran to its end is closed so the driver can retransmit its tail, and
    /// one that failed or was cancelled is torn down at once
    /// (`ReplaySession.java:175-194`).
    #[test]
    fn closing_is_not_revoking() {
        let mut fake = Fake::default();

        Published::new(&mut fake, REGISTRATION_ID)
            .expect("a publication")
            .close();

        assert_eq!(vec![REGISTRATION_ID], fake.closed);
        assert!(fake.released.is_empty(), "a clean end does not revoke");

        Published::new(&mut fake, REGISTRATION_ID)
            .expect("a publication")
            .revoke();

        assert_eq!(vec![REGISTRATION_ID], fake.released);
        assert_eq!(
            vec![REGISTRATION_ID],
            fake.closed,
            "and the close is not repeated"
        );
    }
}
