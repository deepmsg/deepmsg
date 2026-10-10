//! P2-S5's acceptance: the segment operations, end to end.
//!
//! The C suite is this slice's judgement and it is a good one — six cases, all
//! passing (`shouldRecordThenReplayThenTruncate`, `shouldPurgeStoppedRecording`,
//! `shouldPurgeSegments`, `shouldDetachAndDeleteSegments`,
//! `shouldDetachAndReattachSegments`, `shouldUpdateChannel`). This file is the
//! same claim made from inside the workspace, which is the one CI runs, aimed at
//! the four things a C case cannot reach.
//!
//! # The bytes of a segment file a truncate cut
//!
//! Every C case asserts the **catalog**: a truncate moves a recording's stop,
//! and the descriptor is read back to see that it did. What the reference does
//! to the *file* is `eraseRemainingSegment` (`ArchiveConductor.java:2535-2575`),
//! and this build does it in `segment::erase_tail`: the file is cut at the stop
//! and the cut byte is written back at `segmentLength - 1`, which on a file
//! system extends the file out again with zeros in between. So the segment the
//! truncate stopped inside is **still a whole segment** and reads as empty from
//! the cut on.
//!
//! Both halves of that are load-bearing and neither is visible in a descriptor.
//! Not erasing leaves the frames the recording no longer claims readable in a
//! file it still claims, and *truncating* instead of padding leaves a file whose
//! length is not `segmentLength` — which `attachSegments` refuses (`:1591-1596`),
//! so the segment can never be attached again. The C harness records a few
//! kilobytes into 128 MiB segments, so its truncate is to position 0 and no
//! segment is cut at all.
//!
//! # A delete that has to wait for a replay
//!
//! `DeleteSegmentsSession` has an `AWAIT_REPLAYS_STOP` state, entered for a
//! *truncate* and no other caller (`ArchiveConductor.java:1245`), which is what
//! keeps a delete from taking files out from under a reader. No C case reaches
//! it: they run no replay across a truncate.
//!
//! The state itself is one turn long and no test outside the archive can see it
//! — the replay is aborted in the same turn the session is made, and on a
//! machine where an open file can be deleted, deleting without waiting leaves
//! the same files behind. What this file can assert is the **chain**: a replay
//! left genuinely in flight, a truncate answered, files that do go, and a
//! delete signal that comes only at the end. The state is pressed directly by
//! the session's own unit test (`delete_segments.rs`,
//! `waiting_for_replays_comes_first`).
//!
//! # A replay refused because a delete has not finished
//!
//! `:880-886` refuses a replay that would read past a delete still in flight. It
//! is the guard that keeps a client from being handed a recording whose files
//! are half gone, and nothing pressed it until now.
//!
//! **It takes two requests in one turn to reach it**, which is the reason the
//! test below sends without waiting for an answer: the delete session is made by
//! the truncate and the replay is refused by it, and a client that waited for
//! the first answer would be asking about a delete that had already finished. It
//! also takes a **bounded** replay: the comparison is against a stop position
//! *above* the recording's own, and for an unbounded replay of a stopped
//! recording that position **is** the recording's stop, so the second half of the
//! guard is false by construction and the request is taken.
//!
//! # A delete that finishes one somebody else half-rolled
//!
//! A file that will not delete is renamed `…rec.del` and tried again, and the
//! listing counts both suffixes — so a `.del` with no `.rec` beside it is a
//! delete that was interrupted, and the next one has to finish it. That is
//! `DeleteSegmentsSession.java:144-168`, and this file reaches it the way the
//! archive does: `findDetachedSegments` names the `.rec` whatever is on disk, so
//! the delete is handed a path that is not there and a `.del` that is.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use deepmsg_codec::archive::WriteBuf;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::delete_detached_segments_request_codec::DeleteDetachedSegmentsRequestEncoder;
use deepmsg_codec::archive::detach_segments_request_codec::DetachSegmentsRequestEncoder;
use deepmsg_codec::archive::message_header_codec;
use deepmsg_codec::archive::recording_signal::RecordingSignal;
use deepmsg_codec::archive::truncate_recording_request_codec::TruncateRecordingRequestEncoder;
use deepmsg_tests::archive::{self, Archive, DEADLINE, Recording};

/// The connect's correlation id, which its answer echoes.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0005;

/// The archive's segment file length (`Archive.java:342`).
///
/// 64 KiB, the **power-of-two minimum** the archive accepts
/// (`Archive.java:1486-1489`) — the smallest segment a recording can be made to
/// span several of, which is what every test here needs.
const SEGMENT_FILE_LENGTH: i64 = 64 * 1024;

/// The stream every recording and every replay in this file is on.
const STREAM_ID: i32 = 33;

/// How many messages a recording carries, how big each is, and where the mark
/// is.
///
/// A frame is a 32-byte header over its payload, so this is a little over
/// 165 KiB — three segments, which is the least a truncate can cut inside one of
/// and leave another behind it.
const MESSAGE_COUNT: usize = 160;
/// See [`MESSAGE_COUNT`].
const MESSAGE_SIZE: usize = 1024;
/// See [`MESSAGE_COUNT`]: where a truncate cuts, and where the recording is
/// replayed to afterwards.
///
/// It is read **after** that many messages have been offered, so the position is
/// a frame boundary that covers exactly [`MARK`] frames.
const MARK: usize = MESSAGE_COUNT / 2;

/// How many messages the delete-guard test's recording carries.
///
/// It is longer than the others because the truncate in that test has to leave
/// **several** segments behind the cut: the delete it starts takes one file a
/// turn, and the replay request that is refused has to arrive while that delete
/// is still in flight.
const LONG_MESSAGE_COUNT: usize = 512;

/// The block size a replay reads with (`:372-374`).
const FILE_IO_MAX_LENGTH: i32 = 4096;

/// The type id of the counter the delete-guard test's replay is bounded by.
///
/// Any id would do — it is a counter nothing but these tests reads — and it is
/// the C suite's own choice (`aeron_archive_test.cpp:3515`) for the bounded
/// replay that has one.
const LIMIT_COUNTER_TYPE_ID: i32 = 10002;

/// How long the archive is given to notice something it does on its own turns.
const SETTLED: Duration = Duration::from_millis(250);

/// The channel the truncate test records on, and the one its replay goes out
/// on.
///
/// A port per test, because `cargo test` runs them at once in one process and a
/// UDP endpoint is a bound socket (`archive_recording.rs` gives the same
/// reason).
const TRUNCATE_CHANNEL: &str = "aeron:udp?endpoint=localhost:33450";
/// See [`TRUNCATE_CHANNEL`], and the reason it is not the recording's: a replay
/// that went out on the recorded channel would be a replay onto the image this
/// test is recording into.
const TRUNCATE_REPLAY_CHANNEL: &str = "aeron:udp?endpoint=localhost:33451";

/// See [`TRUNCATE_CHANNEL`]: the delete that waits for a replay.
const WAITING_CHANNEL: &str = "aeron:udp?endpoint=localhost:33452";
/// See [`TRUNCATE_REPLAY_CHANNEL`].
const WAITING_REPLAY_CHANNEL: &str = "aeron:udp?endpoint=localhost:33453";

/// See [`TRUNCATE_CHANNEL`]: the replay a delete refuses.
const GUARD_CHANNEL: &str = "aeron:udp?endpoint=localhost:33454";
/// See [`TRUNCATE_REPLAY_CHANNEL`].
const GUARD_REPLAY_CHANNEL: &str = "aeron:udp?endpoint=localhost:33455";

/// See [`TRUNCATE_CHANNEL`]: the delete that finishes a half-rolled one.
const DETACHED_CHANNEL: &str = "aeron:udp?endpoint=localhost:33456";

/// The archive's properties, plus the three every test in this file needs.
///
/// `spies.simulate.connection` is what makes a **spy** reader count as a
/// subscriber, which is the shape `ArchiveConductor.java:562-565` creates and
/// what the C harness sets it for (`TestArchive.h:53`).
///
/// **The other two are one setting, not two.** A recording's segment length is
/// `max(aeron.archive.segment.file.length, termBufferLength)`
/// (`ArchiveConductor.java:2006`, `conductor::segment_file_length`), because a
/// segment has to be able to hold one term — so the archive's 64 KiB is
/// **ignored** while the driver's term is its own default of 16 MiB, and a
/// recording of a few segments' worth lands in a single file. `max(64k, 64k)` is
/// what the pair gives, and it is the smallest segment a recording can be made
/// to span.
fn properties() -> Vec<String> {
    vec![
        archive::PROPERTIES[0].to_owned(),
        archive::PROPERTIES[1].to_owned(),
        "-Daeron.spies.simulate.connection=true".to_owned(),
        format!("-Daeron.term.buffer.length={SEGMENT_FILE_LENGTH}"),
        format!("-Daeron.archive.segment.file.length={SEGMENT_FILE_LENGTH}"),
    ]
}

/// A recording of `messages` messages on `channel`, with a mark at [`MARK`].
fn a_recording(channel: &str, messages: usize) -> Recording {
    Recording::new(channel, STREAM_ID, messages, MESSAGE_SIZE, MARK)
}

/// A truncate (13) of `recording_id` to `position`.
fn truncate_recording_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 128];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            TruncateRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// A detach (53): the recording gives up everything below `new_start_position`.
fn detach_segments_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    new_start_position: i64,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 128];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            DetachSegmentsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .new_start_position(new_start_position);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// A delete-detached (54): the segments the recording has given up are removed.
fn delete_detached_segments_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 128];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            DeleteDetachedSegmentsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// The index of the segment file a position falls inside, and the file.
///
/// The **directory's** answer rather than the arithmetic's: which file a
/// position falls in is `segmentFileBasePosition`'s question
/// (`AeronArchive.java:195-203`), and a test that worked it out for itself would
/// be asserting its own copy of the answer.
fn segment_holding(files: &[(i64, PathBuf)], position: i64) -> (usize, i64, PathBuf) {
    let index = files
        .iter()
        .rposition(|(base, _)| *base <= position)
        .expect("the position is inside the recording");

    (index, files[index].0, files[index].1.clone())
}

/// A truncate cuts the segment it stops inside and leaves it a whole segment.
///
/// The catalog half of this is `shouldRecordThenReplayThenTruncate`'s; what is
/// asserted here is the **file**: its length, the zeros from the cut on, the
/// frames before it, and a replay that reads exactly to the cut and no further.
#[test]
fn a_truncate_cuts_the_segment_it_stops_inside_and_leaves_it_a_whole_segment() {
    let Some(mut archive) = Archive::start(
        "archive-segments-truncate",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    let recording = a_recording(TRUNCATE_CHANNEL, MESSAGE_COUNT);
    let recorded = archive.record(&recording);
    let cut = recorded.first;

    let files = archive.segment_files(recorded.recording_id);
    assert!(
        files.len() >= 3,
        "the recording is {} bytes over {} segments, and this test needs three",
        recorded.stop,
        files.len()
    );

    let (index, base, path) = segment_holding(&files, cut);
    let offset = usize::try_from(cut - base).expect("the cut is inside the segment");

    // The cut has to be *inside* a segment, or the erase is not reached at all:
    // a truncate on a boundary deletes the file whole instead
    // (`ArchiveConductor.java:1219-1233`).
    assert!(
        offset > 0 && offset < usize::try_from(SEGMENT_FILE_LENGTH).expect("a length"),
        "the cut is {offset} into the segment, which is not inside it"
    );

    // Everything after the segment the cut falls in goes with the truncate.
    let removed = files.len() - index - 1;
    assert!(removed >= 1, "the truncate leaves a segment to delete");

    let correlation_id = archive.session.next_correlation_id();
    let payload = truncate_recording_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        cut,
    );
    let answer = archive.ok_answer(correlation_id, &payload);

    assert_eq!(
        i64::try_from(removed).expect("a count"),
        answer.relevant_id,
        "the OK carries the number of files the delete will take"
    );

    // The signal is the end of the delete (`DeleteSegmentsSession.java:76-80`),
    // so the directory is read after it and not before.
    let signal = archive.await_signal(correlation_id);

    assert_eq!(
        RecordingSignal::DELETE,
        signal.signal,
        "the signal a delete sends when its work is over"
    );
    assert_eq!(recorded.recording_id, signal.recording_id);

    // The catalog says the recording stops where the truncate put it.
    assert_eq!(
        cut,
        archive.stop_position(recorded.recording_id),
        "the stop moved to the truncate's position"
    );

    let after = archive.segment_files(recorded.recording_id);
    assert_eq!(
        index + 1,
        after.len(),
        "every segment after the one the cut fell in is gone"
    );

    // And the file itself. The length is the first half of the claim: a
    // truncate that *shortened* the file would leave one `attachSegments`
    // refuses (`:1591-1596`), so the recording could never be made whole again.
    let bytes = std::fs::read(&path).expect("the segment file the cut fell in");

    assert_eq!(
        usize::try_from(SEGMENT_FILE_LENGTH).expect("a length"),
        bytes.len(),
        "the cut segment is still a whole segment"
    );
    assert!(
        bytes[..offset].iter().any(|byte| *byte != 0),
        "the frames below the cut are still there"
    );
    assert!(
        bytes[offset..].iter().all(|byte| *byte == 0),
        "and everything from the cut on reads as zeros, which is what erased means to a reader"
    );

    // A reader's half of the same claim: the recording replays to the cut, with
    // the messages below it and none of the ones the truncate took away.
    let subscription = archive.subscribe(TRUNCATE_REPLAY_CHANNEL, STREAM_ID);
    let correlation_id = archive.session.next_correlation_id();
    let payload = archive::replay_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        -1,
        -2,
        FILE_IO_MAX_LENGTH,
        STREAM_ID,
        TRUNCATE_REPLAY_CHANNEL,
    );
    archive.ok_answer(correlation_id, &payload);

    let replay = archive.read_replay(subscription, cut);

    assert_eq!(
        MARK,
        replay.frames.len(),
        "the messages below the cut come back"
    );
    assert_eq!(
        cut, replay.position,
        "and the reader stops on the cut, which is where the recording now ends"
    );

    for (index, frame) in replay.frames.iter().enumerate() {
        assert_eq!(
            recording.message(index),
            frame.payload,
            "message {index} is the bytes the publication wrote"
        );
    }

    let _ = archive.stop();
}

/// A truncate of a recording a replay is reading ends, and it ends in order.
///
/// The replay is left **genuinely in flight** — subscribed and never read, so
/// its publication fills its term and the replay waits there — and the assertion
/// that it is still open is made rather than assumed: a replay that had finished
/// would make the delete below about nothing.
#[test]
fn a_truncate_of_a_recording_a_replay_is_reading_ends() {
    let Some(mut archive) = Archive::start(
        "archive-segments-delete-waits",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    let recording = a_recording(WAITING_CHANNEL, MESSAGE_COUNT);
    let recorded = archive.record(&recording);
    let cut = recorded.first;

    let files = archive.segment_files(recorded.recording_id);
    let (index, _, _) = segment_holding(&files, cut);
    assert!(files.len() > index + 1, "the truncate has files to take");

    // A replay of the whole recording, and nothing ever reads it: the
    // publication's term fills and the replay waits on it, which is the state a
    // delete has to wait for.
    let subscription = archive.subscribe(WAITING_REPLAY_CHANNEL, STREAM_ID);
    let correlation_id = archive.session.next_correlation_id();
    let payload = archive::replay_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        -1,
        -2,
        FILE_IO_MAX_LENGTH,
        STREAM_ID,
        WAITING_REPLAY_CHANNEL,
    );
    let answer = archive.ok_answer(correlation_id, &payload);
    assert!(answer.relevant_id > 0, "the replay has a session id");

    // It is announced on the CnC broadcast, so it is a poll that makes the
    // image exist — and the image is what says the replay is really running.
    let deadline = Instant::now() + DEADLINE;
    while archive::image(&archive.client, subscription).is_none() {
        assert!(
            Instant::now() < deadline,
            "the replay's publication never linked to the subscription on \
             {WAITING_REPLAY_CHANNEL}"
        );
        archive.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    std::thread::sleep(SETTLED);

    assert_eq!(
        Some(1),
        archive::counter_by_label(&archive.client, "Archive Replay Sessions"),
        "the replay is open — a recording this long would otherwise have been read by now, and \
         the delete below would be waiting for nothing"
    );

    let correlation_id = archive.session.next_correlation_id();
    let payload = truncate_recording_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        cut,
    );
    let answer = archive.ok_answer(correlation_id, &payload);
    assert!(
        answer.relevant_id > 0,
        "the OK carries the files the delete will take, which is {}",
        answer.relevant_id
    );

    // It finishes, which is the claim: the replay it waited for was aborted by
    // the truncate, so the wait ended rather than holding the files forever.
    let signal = archive.await_signal(correlation_id);

    assert_eq!(RecordingSignal::DELETE, signal.signal);

    // The three things the chain owes: the files are gone, the replay the delete
    // waited for is closed, and the recording is cut.
    let after = archive.segment_files(recorded.recording_id);
    assert_eq!(
        index + 1,
        after.len(),
        "the segments after the cut are gone"
    );
    assert_eq!(
        Some(0),
        archive::counter_by_label(&archive.client, "Archive Replay Sessions"),
        "and the replay that held the delete up is closed, so its slot is back"
    );
    assert_eq!(
        cut,
        archive.stop_position(recorded.recording_id),
        "the recording stops where the truncate put it"
    );

    let _ = archive.stop();
}

/// A replay that would read past a delete still in flight is refused.
///
/// Two requests go out before either is answered, because the refusal is about a
/// state that exists only between them: the truncate makes the delete session
/// and the replay finds it. The second replay — the same recording, once the
/// delete is over — is taken, which is the other half of the claim: what was
/// refused was refused because of the delete and not because the recording had
/// become unreadable.
#[test]
fn a_replay_that_would_read_past_a_delete_in_flight_is_refused() {
    let Some(mut archive) = Archive::start(
        "archive-segments-delete-guard",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    let recording = a_recording(GUARD_CHANNEL, LONG_MESSAGE_COUNT);
    let recorded = archive.record(&recording);
    let cut = recorded.first;

    let files = archive.segment_files(recorded.recording_id);
    let (index, _, _) = segment_holding(&files, cut);
    let removed = files.len() - index - 1;

    // The delete takes one file a turn, so the number of files behind the cut is
    // the width of the window the refused replay has to arrive in.
    assert!(
        removed >= 4,
        "the truncate leaves {removed} segments behind the cut, which is too few for the replay \
         below to find the delete still in flight"
    );

    // The replay is **bounded** because that is what makes the guard reachable
    // at all: its `stopPosition` is the counter's value, and the comparison is
    // against a number *above* the recording's own stop — a position an
    // unbounded replay of a stopped recording never has.
    let counter = archive.limit_counter(LIMIT_COUNTER_TYPE_ID, recorded.stop);

    let truncate_correlation_id = archive.session.next_correlation_id();
    let payload = truncate_recording_request(
        archive.session.control_session_id(),
        truncate_correlation_id,
        recorded.recording_id,
        cut,
    );
    archive
        .session
        .send_only(&mut archive.client, &payload, Instant::now() + DEADLINE)
        .expect("the truncate is published");

    let refused_correlation_id = archive.session.next_correlation_id();
    let payload = archive::bounded_replay_request(
        archive.session.control_session_id(),
        refused_correlation_id,
        recorded.recording_id,
        -1,
        -2,
        FILE_IO_MAX_LENGTH,
        counter.counter_id(),
        STREAM_ID,
        GUARD_REPLAY_CHANNEL,
    );
    archive
        .session
        .send_only(&mut archive.client, &payload, Instant::now() + DEADLINE)
        .expect("the replay is published");

    let answer = archive.await_answer(truncate_correlation_id);
    assert_eq!(
        ControlResponseCode::OK,
        answer.code,
        "the truncate itself is taken: {}",
        answer.message()
    );
    assert_eq!(
        i64::try_from(removed).expect("a count"),
        answer.relevant_id,
        "and it carries the files the delete will take"
    );

    let answer = archive.await_answer(refused_correlation_id);
    assert_eq!(
        ControlResponseCode::ERROR,
        answer.code,
        "the replay was taken while a delete of the same recording was in flight: {}",
        answer.message()
    );
    assert!(
        answer
            .message()
            .contains("due to an outstanding delete operation"),
        "and it was refused for the delete: {}",
        answer.message()
    );

    // Over, and the same request is taken: a replay that stops at the
    // recording's own stop is reading files no delete is holding.
    let signal = archive.await_signal(truncate_correlation_id);
    assert_eq!(RecordingSignal::DELETE, signal.signal);

    let correlation_id = archive.session.next_correlation_id();
    let payload = archive::replay_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        -1,
        -2,
        FILE_IO_MAX_LENGTH,
        STREAM_ID,
        GUARD_REPLAY_CHANNEL,
    );
    let answer = archive.ok_answer(correlation_id, &payload);

    assert!(
        answer.relevant_id > 0,
        "the recording is replayable again, with a session id of its own"
    );

    let _ = archive.stop();
}

/// A delete-detached finishes a delete that something else half-rolled.
///
/// The state is made by hand — the `.rec` renamed to `.del`, which is what a
/// delete that died between the two leaves — and the file that goes is the one
/// the listing named, not the one on disk: `findDetachedSegments` builds the
/// `.rec` name whatever is there (`ArchiveConductor.java:1715-1738`), which is
/// what makes the fallback reachable at all.
#[test]
fn a_delete_of_detached_segments_finishes_one_that_was_half_rolled() {
    let Some(mut archive) = Archive::start(
        "archive-segments-del-fallback",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    let recording = a_recording(DETACHED_CHANNEL, MESSAGE_COUNT);
    let recorded = archive.record(&recording);

    let files = archive.segment_files(recorded.recording_id);
    assert!(files.len() >= 2, "the recording has a segment to give up");

    let (_, first_path) = files[0].clone();
    let second_base = files[1].0;

    // The detach: the recording stops claiming its first segment, which is what
    // *detached* means — the file is still there and nothing has deleted it.
    let correlation_id = archive.session.next_correlation_id();
    let payload = detach_segments_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        second_base,
    );
    archive.ok_answer(correlation_id, &payload);

    assert_eq!(
        recorded.stop,
        archive.stop_position(recorded.recording_id),
        "a detach moves the start and not the stop, which is why the segment below it is detached \
         rather than cut"
    );

    // A delete that was interrupted: the `.rec` is gone, the `.del` is not.
    let mut half_rolled = first_path.clone().into_os_string();
    half_rolled.push(".del");
    let half_rolled = PathBuf::from(half_rolled);

    std::fs::rename(&first_path, &half_rolled).expect("the segment file is renamed");

    assert!(
        !first_path.exists() && half_rolled.exists(),
        "the directory is in the state a half-rolled delete leaves"
    );

    let correlation_id = archive.session.next_correlation_id();
    let payload = delete_detached_segments_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
    );
    let answer = archive.ok_answer(correlation_id, &payload);

    assert_eq!(
        1, answer.relevant_id,
        "one file was found to delete: the one below the start, named as a `.rec`"
    );

    let signal = archive.await_signal(correlation_id);

    assert_eq!(
        RecordingSignal::DELETE,
        signal.signal,
        "the delete is over only when the `.del` it was after is gone"
    );

    assert!(
        !half_rolled.exists(),
        "the `.del` name is gone: the delete found no `.rec` to remove and finished the rename \
         off under its own suffix"
    );

    let after = archive.segment_files(recorded.recording_id);
    assert_eq!(
        files.len() - 1,
        after.len(),
        "and nothing else went with it"
    );
    assert_eq!(second_base, after[0].0, "the recording keeps its start");

    let _ = archive.stop();
}
