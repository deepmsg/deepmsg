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

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_archive::server::conductor::ARCHIVE_ID_DEFAULT;
use deepmsg_archive::server::recording_pos::{find_counter_id_by_session, parse_key};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::WriteBuf;
use deepmsg_codec::archive::boolean_type::BooleanType;
use deepmsg_codec::archive::bounded_replay_request_codec::BoundedReplayRequestEncoder;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::message_header_codec;
use deepmsg_codec::archive::replay_request_codec::ReplayRequestEncoder;
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_codec::archive::start_recording_request_2_codec::StartRecordingRequest2Encoder;
use deepmsg_codec::archive::stop_position_request_codec::StopPositionRequestEncoder;
use deepmsg_codec::archive::stop_recording_subscription_request_codec::StopRecordingSubscriptionRequestEncoder;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The connect's correlation id, which its answer echoes.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0004;

/// The archive's id, which its rec-pos counters are keyed by.
const ARCHIVE_ID: i64 = ARCHIVE_ID_DEFAULT;

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

/// How long a command to the driver may take.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the archive is given to notice that a limit counter has gone.
///
/// It reads one every turn, so this is a hundred turns of slack rather than a
/// race.
const SETTLED: Duration = Duration::from_millis(250);

/// The archive's properties, plus the three this file needs.
///
/// `spies.simulate.connection` is the recording's half (`archive_recording.rs`);
/// the other two are the replay's.
fn properties() -> Vec<String> {
    vec![
        archive::PROPERTIES[0].to_owned(),
        archive::PROPERTIES[1].to_owned(),
        "-Daeron.spies.simulate.connection=true".to_owned(),
        format!("-Daeron.archive.segment.file.length={SEGMENT_FILE_LENGTH}"),
        format!("-Daeron.archive.max.concurrent.replays={MAX_CONCURRENT_REPLAYS}"),
    ]
}

/// A recording replayed back, frame for frame, carrying the replay's own ids.
#[test]
fn a_recording_replays_its_own_frames_across_its_segments() {
    let Some(mut archive) = Archive::start("archive-replay-frames") else {
        return;
    };

    let recorded = archive.record(RECORDING_CHANNEL);

    // The recording has to be long enough that reading it back crosses a
    // segment, or the assertion at the end is about a replay of one file.
    assert!(
        recorded.stop > 2 * SEGMENT_FILE_LENGTH,
        "the recording is {} bytes, which does not span the three segments of {} it needs to",
        recorded.stop,
        SEGMENT_FILE_LENGTH
    );

    // The replay's own subscription, and the request. A position of `-1` is the
    // recording's beginning and a length of `-2` is "to the stop"
    // (`AeronArchive.NULL_POSITION`, `REPLAY_ALL_AND_STOP`), so neither number
    // here depends on where the recording happens to start.
    let subscription = archive.subscribe(REPLAY_CHANNEL);
    let correlation_id = archive.session.next_correlation_id();
    let payload = replay_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        -1,
        -2,
        FILE_IO_MAX_LENGTH,
        REPLAY_CHANNEL,
    );

    let answer = archive.ok_answer(correlation_id, &payload);

    // The OK's `relevantId` is the `replaySessionId` (`ReplaySession.java:338`),
    // which is what a client stops the replay by.
    assert!(
        answer.relevant_id > 0,
        "the OK carries the replay session id, which is {}",
        answer.relevant_id
    );
    assert_eq!(
        1,
        answer.relevant_id >> 32,
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
            message(index),
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
    let Some(mut archive) = Archive::start("archive-replay-limit-gone") else {
        return;
    };

    let recorded = archive.record(BOUNDED_RECORDING_CHANNEL);

    for (index, channel) in BOUNDED_REPLAY_CHANNELS.iter().enumerate() {
        let counter = archive.limit_counter(BOUNDED_COUNTER_TYPE_ID, recorded.first);

        let subscription = archive.subscribe(channel);
        let correlation_id = archive.session.next_correlation_id();
        let payload = bounded_replay_request(
            archive.session.control_session_id(),
            correlation_id,
            recorded.recording_id,
            -1,
            -1,
            FILE_IO_MAX_LENGTH,
            counter.counter_id(),
            channel,
        );

        let answer = archive.ok_answer(correlation_id, &payload);
        assert!(answer.relevant_id > 0, "replay {index} has a session id");

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
            .remove_counter(&counter, COMMAND_TIMEOUT)
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
    let correlation_id = archive.session.next_correlation_id();
    let payload = bounded_replay_request(
        archive.session.control_session_id(),
        correlation_id,
        recorded.recording_id,
        -1,
        -1,
        FILE_IO_MAX_LENGTH,
        counter.counter_id(),
        REPLAY_CHANNEL,
    );

    let answer = archive.ok_answer(correlation_id, &payload);
    assert!(answer.relevant_id > 0, "the third replay was taken");

    let _ = archive.stop();
}

/// A running archiving media driver, the client connected to it, and the one
/// control session these tests use.
struct Archive {
    media_driver: OwnArchivingMediaDriver,
    client: Client,
    session: Session,
    aeron_dir: PathBuf,
    cnc: deepmsg_cnc::CncFile,
    /// Held, not merely made: a `TempDir` removes its directory when it is
    /// dropped, and the archive's own directory is one it asks `statvfs` about
    /// before every recording (`isLowStorageSpace`, `ArchiveConductor.java:2597-2617`).
    /// A dropped one is a start refused for want of room on a filesystem that is
    /// not there.
    _archive_dir: TempDir,
}

impl Archive {
    /// Start one, or `None` when our archiving media driver is not built.
    fn start(test_name: &str) -> Option<Self> {
        let archive_dir = TempDir::new(test_name);

        let properties = properties();
        let properties: Vec<&str> = properties.iter().map(String::as_str).collect();

        let Some(mut media_driver) =
            OwnArchivingMediaDriver::start(test_name, archive_dir.path(), &properties)
        else {
            archiving_driver::announce_skip();
            return None;
        };

        media_driver
            .await_ready(DEADLINE)
            .expect("the archiving media driver must come up and signal its mark file");

        let aeron_dir = media_driver.aeron_dir().to_path_buf();
        let mut client = Client::connect(&aeron_dir).expect("connect to the driver");

        let session = Session::connect(
            &mut client,
            &aeron_dir,
            CONNECT_CORRELATION_ID,
            Instant::now() + DEADLINE,
        )
        .unwrap_or_else(|reason| panic!("{reason};\n{}", media_driver.log_tail(40)));

        // A writable mapping of the counters region, which is how an application
        // that owns a counter writes it: the client hands out a reader, because
        // a counter is written by whoever asked for it through the address the
        // driver gave them (`aeron_counter_set_release`).
        let cnc = deepmsg_cnc::CncFile::try_open_writable(&aeron_dir)
            .expect("the CnC file this test's driver wrote");

        Some(Self {
            media_driver,
            client,
            session,
            aeron_dir,
            cnc,
            _archive_dir: archive_dir,
        })
    }

    fn stop(&mut self) -> std::process::ExitStatus {
        self.media_driver.stop().expect("the driver stops")
    }

    /// A counters slot the archive will read as a limit, holding `value`.
    fn limit_counter(&mut self, type_id: i32, value: i64) -> deepmsg_client::counter::Counter {
        let counter = self
            .client
            .add_counter(
                type_id,
                &value.to_be_bytes(),
                "the replay acceptance tests' limit counter",
                COMMAND_TIMEOUT,
            )
            .expect("the driver must allocate the counter");

        let counters = self.cnc.counters_writable().expect("a writable region");
        assert!(counter.set_value(&counters, value), "the limit is set");

        counter
    }

    /// A subscription on `channel`, which a replay's publication links to.
    fn subscribe(&mut self, channel: &str) -> i64 {
        self.client
            .add_subscription(channel, REPLAY_STREAM_ID, COMMAND_TIMEOUT)
            .expect("the driver must accept the replay's subscription")
    }

    /// Send one request that is answered with an `OK`, and assert that it was.
    fn ok_answer(&mut self, correlation_id: i64, payload: &[u8]) -> archive::Response {
        let answer = self
            .session
            .send(
                &mut self.client,
                correlation_id,
                payload,
                Instant::now() + DEADLINE,
            )
            .expect("the archive answers");

        assert_eq!(
            ControlResponseCode::OK,
            answer.code,
            "the archive refused: {}",
            answer.message()
        );

        answer
    }

    /// Read a replay until its reader reaches `position`.
    fn read_replay(&mut self, subscription: i64, position: i64) -> Replay {
        read_replay(&mut self.client, subscription, position)
    }

    /// The one recording a test makes: [`MESSAGE_COUNT`] messages on `channel`,
    /// with a mark at [`FIRST_MARK`], stopped.
    fn record(&mut self, channel: &str) -> Recorded {
        let publication = self
            .client
            .add_exclusive_publication(channel, RECORDING_STREAM_ID, COMMAND_TIMEOUT)
            .expect("the driver must accept the publication");
        let reader = self
            .client
            .add_subscription(channel, RECORDING_STREAM_ID, COMMAND_TIMEOUT)
            .expect("the driver must accept a reader");

        let publisher_session = self
            .client
            .exclusive_publication(publication)
            .map(|publication| publication.session_id())
            .expect("the publication has a session");

        // The start (63), whose answer is the **subscription's** registration
        // id.
        let correlation_id = self.session.next_correlation_id();
        let payload =
            start_recording_request(self.session.control_session_id(), correlation_id, channel);
        let subscription_id = self.ok_answer(correlation_id, &payload).relevant_id;

        let (mark, end) = publish(&mut self.client, publication, reader);

        // The recording follows the publication, and this is where the recording
        // id comes from: nothing else names it until a listing or a stop does.
        let recording_id = wait_for_the_counter(
            &mut self.client,
            publication,
            reader,
            publisher_session,
            &self.aeron_dir,
        );

        let correlation_id = self.session.next_correlation_id();
        let payload = stop_recording_request(
            self.session.control_session_id(),
            correlation_id,
            subscription_id,
        );
        self.ok_answer(correlation_id, &payload);

        let stop = recorded_stop(&mut self.client, &mut self.session, recording_id);

        assert_eq!(
            end, stop,
            "a recording stops where its publication did, which is what makes a mark read off the \
             publication a position the recording is bounded at"
        );

        Recorded {
            recording_id,
            stop,
            first: mark,
        }
    }
}

/// What a recorded run leaves behind.
struct Recorded {
    /// The recording, as the catalog allocated it.
    recording_id: i64,
    /// Where the publication stood when the recording was stopped, which is
    /// where the recording stopped.
    stop: i64,
    /// Where the recording stood at [`FIRST_MARK`] messages.
    ///
    /// It is read off the **publication**, whose position and the recording's
    /// are the same number: the archive's spy subscription joins before anything
    /// is written, so both count from the same place — which [`Archive::record`]
    /// asserts rather than assumes.
    first: i64,
}

/// One replayed frame, kept for the assertions.
struct Frame {
    /// The **frame's** session id, read off its own header.
    session_id: i32,
    /// The frame's stream id.
    stream_id: i32,
    /// Its payload.
    payload: Vec<u8>,
}

/// What a replay left: where it stopped, whose frames it wrote, and the frames.
struct Replay {
    /// The replay publication's session id, which every frame must carry.
    publication_session_id: i32,
    /// Where the reader got to.
    position: i64,
    /// The frames it read.
    frames: Vec<Frame>,
}

/// Publish every message, answering with the publication's position at
/// [`FIRST_MARK`] and at the end.
fn publish(client: &mut Client, publication: i64, reader: i64) -> (i64, i64) {
    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0;
    let mut mark = 0;

    while offered < MESSAGE_COUNT {
        match client.offer_exclusive(publication, &message(offered)) {
            Some(Appended::Ok { .. }) => {
                offered += 1;
                read_whatever_there_is(client, reader);

                // The mark is where the publication stood **after** the message
                // before it: a frame boundary, with whatever padding the term
                // before it needed already counted in.
                if offered == FIRST_MARK {
                    mark = publication_position(client, publication);
                }
            }
            Some(Appended::NotConnected) => {
                assert!(
                    Instant::now() < deadline,
                    "the publication never linked after {offered} messages"
                );
            }
            other => panic!("the message could not be published ({other:?})"),
        }

        client.poll();
    }

    (mark, publication_position(client, publication))
}

/// The `message {index}` bytes, padded to [`MESSAGE_SIZE`].
///
/// A fixed length on purpose: a frame is its payload plus a 32-byte header, so a
/// fixed payload makes every frame the same size and the marks above land where
/// the arithmetic says they will.
fn message(index: usize) -> Vec<u8> {
    let mut payload = format!("message {index:06}").into_bytes();
    payload.resize(MESSAGE_SIZE, b'.');

    payload
}

/// Read whatever the reader has, which is what keeps the publisher going: a UDP
/// publication nobody reads is one the driver stops sending on.
fn read_whatever_there_is(client: &mut Client, reader: i64) {
    let Some(image) = image(client, reader) else {
        return;
    };

    client.poll_image(reader, image.registration_id, 10, |_| {});
}

/// Where a publication has got to.
fn publication_position(client: &Client, publication: i64) -> i64 {
    client
        .exclusive_publication(publication)
        .and_then(|publication| publication.position())
        .expect("the publication is still held")
}

/// Wait for the recording's position counter to catch up with the publication,
/// answering with the recording id it names
/// (`aeron_archive_test.cpp:267-286`).
fn wait_for_the_counter(
    client: &mut Client,
    publication: i64,
    reader: i64,
    session_id: i32,
    aeron_dir: &Path,
) -> i64 {
    let deadline = Instant::now() + DEADLINE;
    let position = publication_position(client, publication);

    while Instant::now() < deadline {
        if let Some((recording_id, value)) = recording_counter(client, session_id) {
            if value >= position {
                return recording_id;
            }
        }

        client.poll();
        read_whatever_there_is(client, reader);
        std::thread::sleep(Duration::from_millis(1));
    }

    panic!(
        "the recording never caught up to {position};\n{}",
        archive::counters(aeron_dir)
    );
}

/// The recording's position counter — its recording id and value — if the driver
/// has one for this session.
fn recording_counter(client: &Client, session_id: i32) -> Option<(i64, i64)> {
    let counters = client.counters_reader()?;
    let counter_id = find_counter_id_by_session(&counters, session_id, ARCHIVE_ID)?;
    let recording_id = parse_key(&counters.key(counter_id)?)?.recording_id;

    Some((recording_id, counters.value(counter_id)?))
}

/// Where a recording stopped (15), waiting for the catalog to say it did.
fn recorded_stop(client: &mut Client, session: &mut Session, recording_id: i64) -> i64 {
    let deadline = Instant::now() + DEADLINE;

    while Instant::now() < deadline {
        let correlation_id = session.next_correlation_id();
        let control_session_id = session.control_session_id();

        let mut buffer = vec![0u8; 64];
        let length = {
            let body = message_header_codec::ENCODED_LENGTH;
            let encoder =
                StopPositionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);

            body + encoder.encoded_length()
        };
        buffer.truncate(length);

        let answer = session
            .send(client, correlation_id, &buffer, Instant::now() + DEADLINE)
            .expect("the archive answers a stop position");
        assert_eq!(
            ControlResponseCode::OK,
            answer.code,
            "the archive refused: {}",
            answer.message()
        );

        if answer.relevant_id >= 0 {
            return answer.relevant_id;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("the recording's catalog row never got a stop position");
}

/// What an image says about itself, copied out of the client so it can be polled
/// again.
struct Image {
    registration_id: i64,
    session_id: i32,
    position: i64,
}

/// The image a subscription holds, if there is one yet.
fn image(client: &Client, subscription: i64) -> Option<Image> {
    let image = client.subscription(subscription)?.images().first()?;

    Some(Image {
        registration_id: image.registration_id(),
        session_id: image.session_id(),
        position: image.position(),
    })
}

/// Read a replay until its reader reaches `position`, answering with every data
/// frame it carried on the way.
///
/// The position is the image's own, which is where the *reader* has got to —
/// padding included, since the reader steps over it. That is what makes "it
/// reached the recording's stop position" a claim about the padding too.
fn read_replay(client: &mut Client, subscription: i64, position: i64) -> Replay {
    let deadline = Instant::now() + DEADLINE;
    let mut frames = Vec::new();
    let mut publication_session_id = None;
    let mut reached_from = None;

    loop {
        // The image is announced on the CnC broadcast, so it is a poll that
        // makes it exist — and the publication behind it is made several turns
        // after the OK, because the OK does not wait for it
        // (`ReplaySession.java:338`).
        client.poll();

        if let Some(current) = image(client, subscription) {
            publication_session_id.get_or_insert(current.session_id);
            read_frames(client, subscription, &mut frames);

            // Read again: the poll above may have taken the image away as well
            // as moved it, and what the reader got to is the last thing it was
            // seen at.
            if let Some(current) = image(client, subscription) {
                reached_from = Some(current.position);

                if current.position >= position {
                    return Replay {
                        publication_session_id: publication_session_id
                            .expect("an image was seen before this could be returned"),
                        position: current.position,
                        frames,
                    };
                }
            }
        }

        assert!(
            Instant::now() < deadline,
            "the replay never reached {position}; it is at {reached_from:?} with {} frames",
            frames.len()
        );

        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Read every data frame the subscription has, appending them to `frames`.
fn read_frames(client: &mut Client, subscription: i64, frames: &mut Vec<Frame>) {
    let Some(current) = image(client, subscription) else {
        return;
    };

    client.poll_image(subscription, current.registration_id, 10, |fragment| {
        // A replay hands over whole frames — it writes blocks of them, not
        // messages split across frames — so a fragment that is not whole is a
        // different claim about how the frames were written, and not this one.
        assert!(
            fragment.is_unfragmented(),
            "a replayed frame is a whole frame"
        );

        let mut payload = vec![0u8; fragment.payload_length()];
        assert!(
            fragment.copy_payload(&mut payload).is_some(),
            "the frame fits"
        );

        frames.push(Frame {
            session_id: fragment.session_id().expect("a frame has a header"),
            stream_id: fragment.stream_id().expect("a frame has a header"),
            payload,
        });
    });
}

/// A `ReplayRequest` (6).
#[allow(clippy::too_many_arguments)] // one per field the request carries
fn replay_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    file_io_max_length: i32,
    replay_channel: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 512];

    let written = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = ReplayRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position)
            .length(length)
            .replay_stream_id(REPLAY_STREAM_ID)
            .file_io_max_length(file_io_max_length)
            // `Aeron.NULL_VALUE`: this replay is asked for on the session's own
            // image, so it carries no token — and the two are told apart by
            // exactly this comparison, which is why the guard on the field
            // matters.
            .replay_token(-1)
            .replay_channel(replay_channel.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(written);
    buffer
}

/// A `BoundedReplayRequest` (18): the same request with the counter that bounds
/// it.
#[allow(clippy::too_many_arguments)] // one per field the request carries
fn bounded_replay_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    file_io_max_length: i32,
    limit_counter_id: i32,
    replay_channel: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 512];

    let written = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = BoundedReplayRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position)
            .length(length)
            .limit_counter_id(limit_counter_id)
            .replay_stream_id(REPLAY_STREAM_ID)
            .file_io_max_length(file_io_max_length)
            .replay_token(-1)
            .replay_channel(replay_channel.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(written);
    buffer
}

/// The `StartRecordingRequest2` (63) for the channel a test records.
fn start_recording_request(control_session_id: i64, correlation_id: i64, channel: &str) -> Vec<u8> {
    let mut buffer = vec![0u8; 256];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            StartRecordingRequest2Encoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .stream_id(RECORDING_STREAM_ID)
            .source_location(SourceLocation::LOCAL)
            .auto_stop(BooleanType::FALSE)
            .channel(channel.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// The `StopRecordingSubscriptionRequest` (14) for one subscription.
fn stop_recording_request(
    control_session_id: i64,
    correlation_id: i64,
    subscription_id: i64,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 64];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = StopRecordingSubscriptionRequestEncoder::default()
            .wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .subscription_id(subscription_id);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}
