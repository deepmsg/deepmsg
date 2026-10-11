//! P2-S4's acceptance: a recording, replayed, end to end.
//!
//! The C suite is this slice's judgement and it is a good one — eighteen cases,
//! all passing. This is the same claim made from inside the workspace, which is
//! the one CI actually runs, and it is aimed at the two things a C case cannot
//! reach.
//!
//! # A replay that crosses segment files
//!
//! The C harness records a handful of kilobytes and an archive's default segment
//! file is 128 MiB, so every replay it makes reads from one file. This one writes
//! segment files of 64 KiB — the power-of-two minimum the archive accepts
//! (`Archive.java:1486-1489`) — and records past three of them, which is the only
//! way the segment-to-segment arithmetic (`:580-592`) is reached end to end. It
//! reads in 4 KiB blocks (`:372-374`) as the C suite's own replays do, so a short
//! read **at a segment boundary** is reached too — and that combination is what
//! the C suite does not have.
//!
//! Asserted: every message the publication wrote comes back once, in order and
//! byte for byte; the frames carry the replay's own ids; and the replayed image's
//! position lands on the recording's stop position **exactly**.
//!
//! That last one is how the padding is asserted. A padding frame is not a
//! fragment — a subscriber's reader steps over it (`aeron_image.c:375-379`) — so
//! it cannot be looked for. It can be counted *through*: a dropped, doubled or
//! misplaced padding frame moves the position the reader ends on.
//!
//! # A bounded replay whose limit counter is taken away
//!
//! `shouldRecordThenBoundedReplay` presses the half where the limit **rises**.
//! The other half is a counter that has gone — closed, or its slot reused — and
//! the reference freezes there rather than following whatever took the slot
//! (`ReplaySession.java:568-574`).
//!
//! Getting it wrong is not a wrong answer but a **leak**: the replay never ends,
//! so the slot it holds in `aeron.archive.max.concurrent.replays` is never given
//! back, and the archive fills with replays that have nothing left to send. So
//! that is what the second test watches, and it watches it the way a client
//! would: against a bound of two, run two replays whose limits have gone, and ask
//! for a third. Measured — with the freeze taken out, the third is refused with
//! `max concurrent replays reached 2`.
//!
//! # What is *not* here, and why
//!
//! The **response channel** (eight of the eighteen C cases), and the
//! **frame-header rewrite**. Both are already caught: every C case watches
//! replayed frames arrive, and the driver's receiver drops frames whose session
//! or stream does not match the image it linked — measured, by taking the rewrite
//! out and then by taking only its stream id out, and both left the replay
//! delivering **nothing at all**.
//! What this file adds for the second is the positive half of the statement: the
//! ids the frames that do arrive are carrying, asserted at the frame rather than
//! left as a consequence of a read that completed.
//!
//! The **replayer's counters** are asserted in the first test and nowhere else,
//! which is the same division of labour: `AeronStat` can read them, but only a
//! test that made the replay knows what they should have counted.
//!
//! Everything this file drives the archive with is in [`archive`], shared with
//! the segments file — including the recording, which both make the same way.

use std::time::Duration;

use deepmsg_archive::client::ReplayParams;
use deepmsg_tests::archive::{self, Archive, Recording};

/// The connect's correlation id, which its answer echoes.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0004;

/// The channel the first test records, and the stream.
///
/// A port of its own, because these tests are run by `cargo test` and two
/// drivers on one host must not pick the same one (`archive_recording.rs` gives
/// the same reason) — and a **second** one below, because the two tests in this
/// file are two drivers on one host running at once.
const RECORDING_CHANNEL: &str = "aeron:udp?endpoint=localhost:33438";
/// See [`RECORDING_CHANNEL`], and the reason it is here: a UDP endpoint is a
/// bound socket, so two tests that record at once cannot share the port.
const BOUNDED_RECORDING_CHANNEL: &str = "aeron:udp?endpoint=localhost:33443";
/// See [`RECORDING_CHANNEL`].
const RECORDING_STREAM_ID: i32 = 33;

/// The channel the first test's replay goes out on, and the stream.
///
/// Both differ from the recording's, which is what makes the frame-header
/// assertion mean something: a replay that copied the recorded headers through
/// would hand the subscriber [`RECORDING_STREAM_ID`].
const REPLAY_CHANNEL: &str = "aeron:udp?endpoint=localhost:33439";
/// See [`REPLAY_CHANNEL`].
const REPLAY_STREAM_ID: i32 = 66;

/// The channels the second test's replays go out on, one each.
///
/// One channel per replay because an image is what a subscription keys a stream
/// by, and three replays of one recording onto one channel would be three images
/// of it to tell apart. The ports are [`REPLAY_CHANNEL`]'s plus one, two, three.
const BOUNDED_REPLAY_CHANNELS: [&str; 3] = [
    "aeron:udp?endpoint=localhost:33440",
    "aeron:udp?endpoint=localhost:33441",
    "aeron:udp?endpoint=localhost:33442",
];

/// How many replays the archive in the second test will run at once
/// (`aeron.archive.max.concurrent.replays`, `Archive.java:445`).
///
/// Two, and the number is the test: a bound of twenty would need twenty leaks
/// to notice, and two needs one.
const MAX_CONCURRENT_REPLAYS: usize = 2;

/// The archive's segment file length for these tests (`Archive.java:342`).
///
/// It is a property because the default is 128 MiB and a test that records
/// 128 MiB is not a test, and it is 64 KiB because that is the **power-of-two
/// minimum** the archive accepts (`Archive.java:1486-1489`) — so this is the
/// smallest segment a replay can be made to cross.
const SEGMENT_FILE_LENGTH: i64 = 64 * 1024;

/// How many messages are recorded, and how long each is.
///
/// A frame is a 32-byte header over its payload and the payload is aligned up,
/// so [`MESSAGE_COUNT`] of them is a little over 165 KiB — past the two and a
/// half segments a replay has to cross, and small enough that the whole test is
/// a second's work.
const MESSAGE_COUNT: usize = 160;
/// See [`MESSAGE_COUNT`].
const MESSAGE_SIZE: usize = 1024;

/// The message the recording is stopped at for the first test, and the one the
/// second test's replays are bounded at.
const FIRST_MARK: usize = MESSAGE_COUNT / 2;

/// The block size the replays read with (`:372-374`).
///
/// Smaller than a segment by a factor of sixteen, so every read of every segment
/// is a short one — which is the whole of what the field does.
const FILE_IO_MAX_LENGTH: i32 = 4096;

/// The type id of the counters the second test bounds its replays with. The C
/// suite's own choice (`aeron_archive_test.cpp:3515`), and any id would do: it
/// is a counter nothing but these tests reads.
const BOUNDED_COUNTER_TYPE_ID: i32 = 10001;

/// How long the archive is given to notice that a limit counter has gone.
///
/// It reads one every turn, so this is a hundred turns of slack rather than a
/// race.
const SETTLED: Duration = Duration::from_millis(250);

/// The recording both tests make: [`MESSAGE_COUNT`] messages on `channel`, with
/// a mark at [`FIRST_MARK`].
fn a_recording(channel: &str) -> Recording {
    Recording::new(
        channel,
        RECORDING_STREAM_ID,
        MESSAGE_COUNT,
        MESSAGE_SIZE,
        FIRST_MARK,
    )
}

/// The archive's properties, plus the four this file needs.
///
/// `spies.simulate.connection` is the recording's half (`archive_recording.rs`).
///
/// The other three are one setting and a bound. A recording's segment length is
/// `max(aeron.archive.segment.file.length, termBufferLength)`
/// (`ArchiveConductor.java:2006`, `conductor::segment_file_length`) — a segment
/// has to be able to hold one term — so asking for 64 KiB segments while the
/// driver's term is its 16 MiB default gets a recording of sixteen megabytes
/// that **fits in one file**, which is every claim this file makes about
/// crossing a segment. `max(64k, 64k)` is what the pair gives.
fn properties() -> Vec<String> {
    vec![
        archive::PROPERTIES[0].to_owned(),
        archive::PROPERTIES[1].to_owned(),
        "-Daeron.spies.simulate.connection=true".to_owned(),
        format!("-Daeron.term.buffer.length={SEGMENT_FILE_LENGTH}"),
        format!("-Daeron.archive.segment.file.length={SEGMENT_FILE_LENGTH}"),
        format!("-Daeron.archive.max.concurrent.replays={MAX_CONCURRENT_REPLAYS}"),
    ]
}

/// A recording replayed back, frame for frame, carrying the replay's own ids.
#[test]
fn a_recording_replays_its_own_frames_across_its_segments() {
    let Some(mut archive) = Archive::start(
        "archive-replay-frames",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    let recording = a_recording(RECORDING_CHANNEL);
    let recorded = archive.record(&recording);

    // The recording has to be long enough that reading it back crosses a
    // segment, or the assertion at the end is about a replay of one file — and
    // it is the **files** that say so, not the number of bytes. The two were not
    // the same thing until the term length came down with the segment length
    // (see [`properties`]): 168960 bytes over 64 KiB is three segments and was
    // one file of sixteen megabytes, which is the shape this test is here to
    // avoid.
    let segments = archive.segment_files(recorded.recording_id);

    assert!(
        segments.len() >= 3,
        "the recording is {} bytes over {} segment files, and this test needs three",
        recorded.stop,
        segments.len()
    );

    // The replay's own subscription, and the request. A position of `-1` is the
    // recording's beginning and a length of `-2` is "to the stop"
    // (`AeronArchive.NULL_POSITION`, `REPLAY_ALL_AND_STOP`), so neither number
    // here depends on where the recording happens to start.
    //
    // The request is `Archive::start_replay` and **not** `Archive::replay`: the
    // latter follows it with a subscription of its own and answers that
    // subscription's registration id (`archive.rs:1652-1687`). The subscription
    // this replay is read through is the one made just above, and an id the
    // product picked for a subscription is not the id asserted below.
    let subscription = archive.subscribe(REPLAY_CHANNEL, REPLAY_STREAM_ID);
    let replay_session_id = archive
        .session
        .archive_mut()
        .start_replay(
            &mut archive.client,
            recorded.recording_id,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            &ReplayParams {
                file_io_max_length: FILE_IO_MAX_LENGTH,
                position: -1,
                length: -2,
                ..ReplayParams::default()
            },
        )
        .expect("the archive starts the replay");

    // The OK's `relevantId` — what `start_replay` answers — is the
    // `replaySessionId` (`ReplaySession.java:338`), which is what a client stops
    // the replay by.
    assert!(
        replay_session_id > 0,
        "the OK carries the replay session id, which is {}",
        replay_session_id
    );
    assert_eq!(
        1,
        replay_session_id >> 32,
        "and its high half is the first replay id, which starts at 1"
    );

    let replay = archive.read_replay(subscription, recorded.stop);

    assert_eq!(
        MESSAGE_COUNT,
        replay.frames.len(),
        "every message the publication wrote comes back once"
    );

    for (index, frame) in replay.frames.iter().enumerate() {
        assert_eq!(
            recording.message(index),
            frame.payload,
            "message {index} is the bytes the publication wrote"
        );

        // The **frame's** own header words, read off the term rather than off
        // the image, and they name the replay's publication rather than the
        // recorded one. See the module note for what this does and does not
        // catch.
        assert_eq!(
            replay.publication_session_id, frame.session_id,
            "message {index} carries the replay publication's session id"
        );
        assert_eq!(
            REPLAY_STREAM_ID, frame.stream_id,
            "message {index} carries the replay stream, not the recorded one"
        );
    }

    assert_ne!(
        RECORDING_STREAM_ID, REPLAY_STREAM_ID,
        "and the two streams differ, or the assertion above says nothing"
    );

    // Where the replay's reader got to, which is the recording's stop position
    // exactly — padding and all, since padding is bytes the reader steps over
    // and a missing or misplaced one moves this number.
    assert_eq!(
        recorded.stop, replay.position,
        "the replay ran to the recording's stop position"
    );

    // The replayer's counters, which the archive allocates at startup and the
    // replay has just moved (`AeronCounters`: 108, 109, 110, 112), read by the
    // label a reader of the counters region finds them by.
    let by_label = |prefix: &str| archive::counter_by_label(&archive.client, prefix);

    assert_eq!(
        Some(0),
        by_label("Archive Replay Sessions"),
        "112 is the replays that are **open**, and this one ran to its end"
    );
    assert!(
        by_label("archive-replayer total read bytes").is_some_and(|bytes| bytes > 0),
        "109 counted the bytes the replay read"
    );
    assert!(
        by_label("archive-replayer max read time in ns").is_some_and(|ns| ns > 0),
        "and 108 the longest single read"
    );
    assert!(
        by_label("archive-replayer total read time in ns").is_some_and(|ns| ns > 0),
        "and 110 all of them together"
    );

    let _ = archive.stop();
}

/// A bounded replay ends when its limit counter goes, and the slot it held comes
/// back.
///
/// The behavioural claim is the **third** replay: with a bound of two and two
/// replays already run, an archive that gave their slots back accepts it, and
/// one that did not refuses it with `MAX_REPLAYS`.
#[test]
fn a_replay_whose_limit_has_gone_gives_its_slot_back() {
    let Some(mut archive) = Archive::start(
        "archive-replay-limit-gone",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    let recording = a_recording(BOUNDED_RECORDING_CHANNEL);
    let recorded = archive.record(&recording);

    for (index, channel) in BOUNDED_REPLAY_CHANNELS.iter().enumerate() {
        let counter = archive.limit_counter(BOUNDED_COUNTER_TYPE_ID, recorded.first);

        // Naming a limit counter that is not the null one is the whole of what
        // makes this the **bounded** form of the request
        // (`aeron_archive_replay_params_is_bounded`,
        // `aeron_archive_replay_params.c:43-46`); everything else about the two
        // is identical.
        let subscription = archive.subscribe(channel, REPLAY_STREAM_ID);
        let replay_session_id = archive
            .session
            .archive_mut()
            .start_replay(
                &mut archive.client,
                recorded.recording_id,
                channel,
                REPLAY_STREAM_ID,
                &ReplayParams {
                    bounding_limit_counter_id: counter.counter_id(),
                    file_io_max_length: FILE_IO_MAX_LENGTH,
                    position: -1,
                    length: -1,
                    ..ReplayParams::default()
                },
            )
            .expect("the archive starts the bounded replay");

        assert!(replay_session_id > 0, "replay {index} has a session id");

        // It runs up to the limit and waits there, because `-1` is
        // `REPLAY_ALL_AND_FOLLOW` and a following replay has nothing telling it
        // that there will not be more.
        let replay = archive.read_replay(subscription, recorded.first);

        assert_eq!(
            recorded.first, replay.position,
            "replay {index} stopped on the limit exactly"
        );

        // The counter goes, and the replay goes with it (`notExtended`'s other
        // half, `ReplaySession.java:568-574`).
        archive
            .client
            .remove_counter(&counter, archive::COMMAND_TIMEOUT)
            .expect("the driver gives the counter back");

        std::thread::sleep(SETTLED);
    }

    // Two replays have run against a bound of two. If either is still holding
    // its slot this is refused with `MAX_REPLAYS`, which is the leak: a replay
    // that will never send another byte, and a slot that never comes back.
    //
    // No subscription of its own: what is being asked is whether the archive
    // will take the replay, and the OK to this request goes out on the control
    // response channel whether or not anything is listening to the replay.
    let counter = archive.limit_counter(BOUNDED_COUNTER_TYPE_ID, recorded.first);
    let replay_session_id = archive
        .session
        .archive_mut()
        .start_replay(
            &mut archive.client,
            recorded.recording_id,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            &ReplayParams {
                bounding_limit_counter_id: counter.counter_id(),
                file_io_max_length: FILE_IO_MAX_LENGTH,
                position: -1,
                length: -1,
                ..ReplayParams::default()
            },
        )
        .expect("the archive starts the third replay");

    assert!(replay_session_id > 0, "the third replay was taken");

    let _ = archive.stop();
}
