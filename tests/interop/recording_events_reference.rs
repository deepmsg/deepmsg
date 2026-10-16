//! ★★ **The three recording events, decoded by the reference's own adapter.**
//!
//! Every other criterion P2-13 has is this build's encoder read back by this
//! build's decoder, and two copies of one implementation agreeing about a wire
//! format is not yet a wire format. `RecordingEventsAdapter` is the reader the
//! events exist for — the reference's `LiveRecordingMessageTransceiver` uses it
//! as a **pacemaker**, consuming only what the archive has recorded — and it is
//! generated code over the schema's own field order, reading bytes we wrote.
//!
//! What it can fail on that nothing else in this slice can:
//!
//! * the three **template ids**, which a reader dispatches on. A body encoded
//!   under the wrong id is a body nobody reads, and the byte-level unit tests
//!   assert the ids against the schema's constants rather than against a reader;
//! * the **variable part** of `RecordingStarted` — its two length-prefixed
//!   strings — which is the only reason the message needs a kilobyte of buffer;
//! * the **fields the archive has to fetch from somewhere else**: the recorded
//!   subscription's stream id and the channel the client asked for are not the
//!   events channel's, and `image.sessionId()` is not the archive's;
//! * and that the events go out **at all**, from the threading mode the C suite
//!   runs, through a publication built in one process and written in another.
//!
//! Run with `cargo test -p deepmsg-tests --features interop`. When the reference
//! build is not there this prints `SKIPPED` and passes, like the rest of this
//! directory.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::aeron_tests;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::driver;
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

/// The channel recorded, and the stream: a port of its own, because two drivers
/// on one host must not pick the same one.
const RECORDING_CHANNEL: &str = "aeron:udp?endpoint=localhost:33533";
/// The same, for the `DEDICATED` criterion: two tests in one binary run at
/// once, and a UDP endpoint is one process's at a time.
const RECORDING_CHANNEL_DEDICATED: &str = "aeron:udp?endpoint=localhost:33534";
const RECORDING_STREAM_ID: i32 = 35;

/// The events channel and stream, which the archive is configured with and the
/// probe subscribes to. `aeron:ipc` is what the reference's own benchmark
/// harness sets (`live-recording-client:27`), and it is a **shape** constraint
/// rather than a performance one: an events channel has to reach a reader in the
/// same driver.
const EVENTS_CHANNEL: &str = "aeron:ipc";
const EVENTS_STREAM_ID: i32 = 30;

/// How many messages the recording is asked to carry: more than one frame, so
/// the archive reads the image on more than one turn and reports progress on
/// more than one.
const MESSAGE_COUNT: usize = 16;

/// How long the probe stays up after subscribing. The test is what ends it, by
/// stopping the recording; this is the backstop that keeps a reader that never
/// sees a stop from hanging the suite.
const PROBE_DEADLINE_MS: u64 = 30_000;

/// How long a command to the driver may take.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// The connect's correlation id, which its answer echoes.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0003;

/// The archive's properties plus the three that turn the events on, and the one
/// the spy subscription needs (`ArchiveConductor.java:562-565`; the C harness
/// sets it for the same reason).
///
/// The events channel is `aeron:ipc` and the stream id is the reference's
/// default, written out rather than left to be: the probe has to subscribe to
/// the same pair, and a default is a thing two programs can disagree about.
const PROPERTIES: [&str; 7] = [
    archive::PROPERTIES[0],
    archive::PROPERTIES[1],
    archive::PROPERTIES[2],
    "-Daeron.spies.simulate.connection=true",
    "-Daeron.archive.recording.events.enabled=true",
    "-Daeron.archive.recording.events.channel=aeron:ipc",
    "-Daeron.archive.recording.events.stream.id=30",
];

/// One event, as the probe printed it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Started {
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: String,
        source_identity: String,
    },
    Progress {
        recording_id: i64,
        position: i64,
    },
    Stopped {
        recording_id: i64,
        stop_position: i64,
    },
}

impl Event {
    /// One line of the probe's output, or `None` for a line that is not an
    /// event.
    fn parse(line: &str) -> Option<Self> {
        let fields: Vec<&str> = line.split(' ').collect();
        let number = |at: usize| fields[at].parse::<i64>().ok();

        match *fields.first()? {
            "STARTED" => Some(Self::Started {
                recording_id: number(1)?,
                start_position: number(2)?,
                session_id: i32::try_from(number(3)?).ok()?,
                stream_id: i32::try_from(number(4)?).ok()?,
                channel: fields.get(5)?.to_string(),
                source_identity: fields.get(6)?.to_string(),
            }),
            "PROGRESS" => Some(Self::Progress {
                recording_id: number(1)?,
                position: number(3)?,
            }),
            "STOPPED" => Some(Self::Stopped {
                recording_id: number(1)?,
                stop_position: number(3)?,
            }),
            _ => None,
        }
    }
}

/// The probe, running and being read by a thread of its own.
///
/// A thread rather than a blocking `read_line` because the test has work to do
/// between the lines: the recording must not start until the reader is
/// subscribed, and the stop must not be asked for until the events have been
/// heard — and a reader that is not being drained is a probe blocked on a full
/// pipe rather than a probe that failed.
struct Probe {
    child: Child,
    lines: Arc<Mutex<Vec<String>>>,
}

impl Probe {
    /// Start it against `aeron_dir`, subscribed to the events channel.
    fn spawn(aeron_dir: &Path, classes: &Path) -> Self {
        let jar = driver::locate_aeron_all().expect("the reference build is there");
        let classpath = std::env::join_paths([jar, classes.to_path_buf()]).expect("two paths join");

        let mut child = Command::new("java")
            .args(driver::AGRONA_JVM_ARGS)
            .arg("-cp")
            .arg(classpath)
            .arg("RecordingEventsProbe")
            .arg(aeron_dir)
            .arg(EVENTS_CHANNEL)
            .arg(EVENTS_STREAM_ID.to_string())
            .arg(PROBE_DEADLINE_MS.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("java runs");

        let stdout = child.stdout.take().expect("piped");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let into = Arc::clone(&lines);

        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                into.lock().expect("no one panics holding this").push(line);
            }
        });

        Self { child, lines }
    }

    /// What it has printed so far.
    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .expect("no one panics holding this")
            .clone()
    }

    /// Wait for a line beginning with `want`.
    ///
    /// # Panics
    ///
    /// When the deadline passes, with everything printed so far — a probe that
    /// died on the way up has said why on stderr, and a probe that is merely
    /// quiet has not.
    fn await_line(&mut self, want: &str, deadline: Instant) -> String {
        loop {
            if let Some(line) = self.lines().into_iter().find(|line| line.starts_with(want)) {
                return line;
            }

            assert!(
                Instant::now() < deadline,
                "the probe never printed a line beginning {want:?}; it printed {:?}",
                self.lines()
            );

            assert!(
                self.child.try_wait().expect("waitable").is_none(),
                "the probe exited before {want:?}; it printed {:?}",
                self.lines()
            );

            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Wait for it to end, and hand back everything it printed.
    fn finish(&mut self, deadline: Instant) -> Vec<String> {
        while self.child.try_wait().expect("waitable").is_none() {
            assert!(
                Instant::now() < deadline,
                "the probe is still running with {:?}",
                self.lines()
            );

            std::thread::sleep(Duration::from_millis(20));
        }

        self.lines()
    }
}

/// Compile the probe against the reference jar, once per process.
///
/// Once, and under a shared lock, for the reason `destination_probe.rs` gives:
/// two tests `exec`ing a class directory another thread is writing is how a
/// suite fails one run in six.
fn build_probe() -> Option<PathBuf> {
    static BUILT: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

    BUILT
        .get_or_init(|| {
            let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let into = manifest.join("../target/recording-events-probe-classes");
            let _ = std::fs::create_dir_all(&into);

            java::compile_probe(&aeron_tests::java("RecordingEventsProbe.java")?, &into)?;

            Some(into)
        })
        .clone()
}

/// ★★ The reference's own adapter, reading the three events of one recording
/// made and stopped under whichever archive `properties` names.
///
/// The two criteria below are the same claim about two different **places the
/// publication is held**, which is the one structural decision P2-13 makes: a
/// session borrows the publication from the half that runs it, and under
/// `DEDICATED` that half is a thread of its own that had to be *told* about it
/// ([`RecorderCommand::SetEvents`], the shape `SetCounters` already had). A
/// publication that never reached the worker would record perfectly and publish
/// nothing, which is what a criterion that only ran `SHARED` would not see.
///
/// [`RecorderCommand::SetEvents`]: deepmsg_archive::server::recorder::RecorderCommand
fn a_recording_whose_events_the_reference_reads(
    test_name: &str,
    channel: &str,
    properties: &[&str],
) {
    let Some(classes) = build_probe() else {
        driver::announce_tool_skip("javac");
        return;
    };

    let archive_dir = TempDir::new(test_name);

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start(test_name, archive_dir.path(), properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    let aeron_dir = media_driver.aeron_dir().to_path_buf();
    let mut client = Client::connect(&aeron_dir).expect("connect to the driver");

    // ★ **The reader first.** An `aeron:ipc` publication with no subscriber
    // throws its offers away, and `started` goes out once, at the moment the
    // first segment opens — so a reader that arrives after the recording starts
    // has missed the one event nothing can repeat. `READY` is printed once
    // `addSubscription` has been answered, which for IPC is also the moment the
    // driver has linked the probe to the archive's publication.
    let mut probe = Probe::spawn(&aeron_dir, &classes);
    probe.await_line("READY", Instant::now() + DEADLINE);

    let mut session = Session::connect(
        &mut client,
        &aeron_dir,
        CONNECT_CORRELATION_ID,
        Instant::now() + DEADLINE,
    )
    .unwrap_or_else(|reason| panic!("{reason};\n{}", media_driver.log_tail(40)));

    // The channel that is recorded, and a reader of it: a UDP publication nobody
    // reads is one the driver stops sending on, and the archive reaches it
    // through a **spy** subscription (`ArchiveConductor.java:559-560`).
    let publication = client
        .add_exclusive_publication(channel, RECORDING_STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept the publication");
    let reader = client
        .add_subscription(channel, RECORDING_STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept a reader");

    let publisher_session = client
        .exclusive_publication(publication)
        .map(|publication| publication.session_id())
        .expect("the publication has a session");

    // ★ **Half the messages go out before the archive is asked to record.** The
    // recording's start position is the **image's join position**
    // (`ArchiveConductor.java:2005`), so a stream the archive joined late is the
    // only way this test can tell that field from a zero the writer happened to
    // start at.
    let before = publish_messages(&mut client, publication, reader, 0, MESSAGE_COUNT / 2);

    let subscription_id = session
        .archive_mut()
        .start_recording(&mut client, channel, RECORDING_STREAM_ID, true, false)
        .expect("the archive answers a start");

    let published = publish_messages(
        &mut client,
        publication,
        reader,
        MESSAGE_COUNT / 2,
        MESSAGE_COUNT,
    );

    // The recording id is the probe's, and it is an input rather than an
    // assertion: everything else this test asserts is asked of the archive about
    // **that** recording, so an id the archive does not know is a refusal rather
    // than a pass.
    let Event::Started { recording_id, .. } =
        Event::parse(&probe.await_line("STARTED", Instant::now() + DEADLINE))
            .expect("the line the probe printed is a started event")
    else {
        unreachable!("`await_line` found a line beginning STARTED")
    };

    session
        .archive_mut()
        .stop_recording_subscription(&mut client, subscription_id)
        .expect("the archive answers a stop");

    // ★ **The wait is the reference's own order, not tolerance for a race.**
    // `stopped` goes out in the session's last turn, off the position
    // **counter**; the catalog's stop position is written afterwards, when the
    // conductor closes the session it collected. So the event arrives first,
    // and the claim is that the two numbers agree once both are there.
    let stopped = await_stop_position(&mut session, &mut client, recording_id);

    let printed = probe.finish(Instant::now() + DEADLINE);
    let events: Vec<Event> = printed
        .iter()
        .filter_map(|line| Event::parse(line))
        .collect();

    assert!(
        printed.iter().any(|line| line.starts_with("DONE")),
        "the probe ran to the end of its own loop; it printed {printed:?}"
    );

    match events.as_slice() {
        [Event::Started { .. }, progress @ .., Event::Stopped { .. }] => assert!(
            !progress.is_empty(),
            "the recording moved, so at least one progress event went out; it printed {printed:?}"
        ),
        other => panic!("expected one started, progress, and one stopped: {other:?}"),
    }

    // ★ **`started` first, and its fields.** These are the ones no test of ours
    // could check against another: the stream id and the channel belong to the
    // **recorded** subscription, the session id to the publication being
    // recorded, and the identity to the image's source — three different
    // objects, and two of them are `i32`s a reader would swap without noticing.
    let Event::Started {
        start_position,
        session_id,
        stream_id,
        channel: recorded_channel,
        source_identity,
        ..
    } = events[0].clone()
    else {
        unreachable!("the match above put a Started first")
    };

    assert_eq!(
        RECORDING_STREAM_ID, stream_id,
        "the recorded stream, not the events stream"
    );
    assert_eq!(
        channel, recorded_channel,
        "the channel the client asked for, verbatim"
    );
    assert_eq!(
        publisher_session, session_id,
        "the session of the publication being recorded, not the archive's"
    );
    assert!(
        start_position >= before,
        "the start is the **image's join position** ({start_position}), and the \
         archive joined a stream that was already {before} bytes along"
    );
    assert_eq!(
        "aeron:ipc", source_identity,
        "★ and this one is the reference's own constant for a **spy** image — a \
         network publication links its spies with `AERON_IPC_CHANNEL` as the \
         source identity (`aeron_driver_conductor.c:4205-4220`), whatever the \
         channel it is being spied on"
    );

    // ★ **`stopped` last, and where the catalog says.** The event's number and
    // the archive's own answer are two different questions — one read off the
    // counter in the session's last turn, the other out of the catalog row the
    // stop wrote — and they agree only if both halves did.
    let Event::Stopped {
        stop_position,
        recording_id: stopped_recording,
    } = *events.last().expect("the match above left one")
    else {
        unreachable!("the match above put a Stopped last")
    };

    assert_eq!(recording_id, stopped_recording);
    assert_eq!(
        stopped, stop_position,
        "the stop the event reported is the stop the archive wrote down"
    );
    assert!(
        stop_position >= published,
        "and it is at least as far as the publication got ({published})"
    );

    // Every progress event belongs to this recording, and none of them is past
    // the stop: the mark the session keeps is a position that has been recorded.
    for event in &events[1..events.len() - 1] {
        let Event::Progress {
            recording_id: progress_recording,
            position,
        } = *event
        else {
            unreachable!("the match above put only Progress between the two")
        };

        assert_eq!(recording_id, progress_recording);
        assert!(
            position <= stop_position,
            "a progress event reported {position}, past the stop at {stop_position}"
        );
    }

    let _ = media_driver.stop();
}

/// Where the catalog says the recording stopped, waiting for it to be written
/// (`ArchiveConductor.closeRecordingSession`, `:1345-1352`).
///
/// A row with no stop is answered `-1` rather than refused
/// (`ArchiveConductor.java:1169-1175`), which is the state this waits out.
///
/// # Panics
///
/// When the archive refuses — an id it does not know is a refusal, which is what
/// makes the id the event named an input rather than a tautology — or when the
/// stop never appears.
fn await_stop_position(session: &mut Session, client: &mut Client, recording_id: i64) -> i64 {
    let deadline = Instant::now() + DEADLINE;

    loop {
        match session
            .archive_mut()
            .get_stop_position(client, recording_id)
        {
            Ok(position) if position >= 0 => return position,
            Ok(_) => {}
            Err(error) => panic!("the archive refused a stop position for {recording_id}: {error}"),
        }

        assert!(
            Instant::now() < deadline,
            "the catalog never carried a stop position for {recording_id}"
        );

        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Publish messages `from` up to `to` and read them back, answering with where
/// the publication got to.
///
/// The reader is not politeness: a UDP publication nobody reads is one the
/// driver stops sending on, and the archive's spy subscription is what is being
/// recorded.
fn publish_messages(
    client: &mut Client,
    publication: i64,
    reader: i64,
    from: usize,
    to: usize,
) -> i64 {
    let deadline = Instant::now() + DEADLINE;
    let mut offered = from;

    while offered < to {
        let message = format!("message {offered}");

        match client.offer_exclusive(publication, message.as_bytes()) {
            Some(Appended::Ok { .. }) => {
                offered += 1;

                if let Some(image) = client.subscription(reader).and_then(|subscription| {
                    subscription
                        .images()
                        .first()
                        .map(deepmsg_client::image::Image::registration_id)
                }) {
                    client.poll_image(reader, image, 10, |_| {});
                }
            }
            // Both are "the window is not open yet", which is a moment rather
            // than a failure: `NotConnected` is a reader that has not linked,
            // and `BackPressured` is one that has but whose position the driver
            // has not taken up. The first message of the first half meets one of
            // them — the publication exists a turn before its reader's endpoint
            // does — and the reference's publishers wait them out the same way.
            Some(Appended::NotConnected | Appended::BackPressured) => assert!(
                Instant::now() < deadline,
                "the publication never linked after {offered} messages"
            ),
            other => panic!("the message could not be published ({other:?})"),
        }

        client.poll();
    }

    let position = client
        .exclusive_publication(publication)
        .and_then(|publication| publication.position())
        .expect("the publication is still held");

    assert!(position > 0, "the publication wrote something");

    position
}

/// ★★ The `SHARED` archive: the recorder is the conductor's own object, and the
/// publication is written in the turn that drives the sessions.
#[test]
fn the_references_own_adapter_decodes_the_three_events() {
    a_recording_whose_events_the_reference_reads(
        "recording-events-reference",
        RECORDING_CHANNEL,
        &PROPERTIES,
    );
}

/// ★★ The `DEDICATED` archive: the sessions run on the recorder's own thread,
/// so the publication had to cross a **command channel** to get there
/// (`RecorderCommand::SetEvents`) — and it is the same publication, with the
/// same three events, because it is the same archive underneath. See
/// [`a_recording_whose_events_the_reference_reads`].
///
/// Its own channel because a UDP endpoint belongs to one process at a time and
/// the two criteria above run at once in one binary.
#[test]
fn the_references_own_adapter_decodes_them_from_a_dedicated_recorder_too() {
    let mut properties: Vec<&str> = PROPERTIES.to_vec();
    properties.push("-Daeron.archive.threading.mode=DEDICATED");

    a_recording_whose_events_the_reference_reads(
        "recording-events-reference-dedicated",
        RECORDING_CHANNEL_DEDICATED,
        &properties,
    );
}
