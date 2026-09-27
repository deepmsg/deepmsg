//! The reference client against **our** driver.
//!
//! Every other interop test in this tree runs our client against the reference
//! driver. This one turns the pair around, and it is the only arrangement in
//! which the driver-side byte contracts can be *falsified* rather than
//! restated: `BasicPublisher` and `BasicSubscriber` share no code with this
//! build, so a log buffer whose metadata is subtly wrong, an image whose path
//! is not where the reference puts it, a session id that never reaches the
//! frame header, or a publisher limit that never opens all show up here — as a
//! wrong payload, a "not connected" message, or a hang.
//!
//! What the pair exercises, in the reference's own terms:
//!
//! 1. `ADD_PUBLICATION` and `ON_PUBLICATION_READY` — the sample maps the log
//!    buffer this driver created, at the path this driver named.
//! 2. `ADD_SUBSCRIPTION` and `ON_AVAILABLE_IMAGE` — the sample maps the same
//!    file as a reader and joins at the position the driver chose.
//! 3. The **publisher limit**, which is the assertion no single-sided test can
//!    reach: `BasicPublisher` sleeps a second between messages and does not
//!    retry one, so a window that never opens loses messages rather than
//!    failing to start.
//! 4. The subscriber's position counter coming back — the driver recomputes
//!    the limit from it every duty cycle, so a driver that never reads it
//!    stops the publisher after one message's worth of window.
//!
//! # What the reference samples cannot cover
//!
//! Fragmentation and an MTU-sized payload are **not** reachable through these
//! binaries: `BasicPublisher`'s `-f` is not in the installed build (the samples
//! are built from whatever revision last built them, not from the checkout's
//! source), and `Throughput` writes through `tryClaim`, which refuses anything
//! over `maxPayloadLength` — it says so out loud. Both are covered from our own
//! side instead: a client offering more than one frame is our client's own
//! test, and the driver sees the same frames either way.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};

/// The stream both samples use.
const STREAM_ID: i32 = 3001;

/// How long a sample gets to finish. The publisher sleeps a second per message,
/// and this has to cover its messages, the driver's start-up and the map of a
/// log buffer.
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Where the reference's sample binaries are built to, beside the `aeronmd` and
/// `AeronStat` this suite already looks for.
const SAMPLES_DIR: &str = "../../aeron/cppbuild/Release/binaries";

/// Find a reference sample, or skip the test that needs it.
fn sample(name: &str) -> Option<PathBuf> {
    let env = format!("DEEPMSG_REF_{}", name.to_uppercase());
    let default = format!("{SAMPLES_DIR}/{name}");

    driver::locate_tool(&env, &default)
}

/// A reference sample process, with its output in a file so that the test can
/// watch it *while* it runs — the subscriber runs until it is told to stop, and
/// what it has received so far is the assertion.
struct Sample {
    name: String,
    child: Child,
    output: PathBuf,
}

impl Sample {
    fn start(binary: &Path, name: &str, dir: &Path, args: &[&str]) -> Self {
        let output = dir.with_extension(format!("{name}.out"));
        let log = std::fs::File::create(&output).expect("the sample's log file");
        let log_err = log.try_clone().expect("a second handle");

        let child = Command::new(binary)
            // `-p` is the aeron directory for every sample, and it is the only
            // thing that points them at this driver rather than at whichever
            // one happens to be running.
            .arg("-p")
            .arg(dir)
            .args(args)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()
            .expect("start the sample");

        Self {
            name: name.to_string(),
            child,
            output,
        }
    }

    /// Everything the sample has written so far.
    fn output(&self) -> String {
        std::fs::read_to_string(&self.output).unwrap_or_default()
    }

    /// Wait for the sample to exit on its own.
    fn await_exit(&mut self, within: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + within;

        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Ask the sample to stop, and wait for it.
    ///
    /// `Child::kill` is SIGKILL, and a sample that is killed does not exit
    /// cleanly — which is half of what this test asserts. The signal is
    /// **SIGINT**, not the SIGTERM a driver gets: the samples install a handler
    /// for SIGINT only (`aeron-samples/src/main/c/basic_subscriber.c:141`), so
    /// SIGTERM would end the process by its default disposition and the test
    /// would be asserting on the kernel rather than on the sample.
    fn terminate(&mut self, within: Duration) -> Option<ExitStatus> {
        let _ = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status();

        let deadline = Instant::now() + within;

        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The sample's output once `predicate` holds, or a panic naming what was
    /// waited for.
    fn await_output(
        &self,
        within: Duration,
        what: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> String {
        let deadline = Instant::now() + within;

        loop {
            let output = self.output();
            if predicate(&output) {
                return output;
            }
            assert!(
                Instant::now() < deadline,
                "{} never showed {what}:\n{output}",
                self.name
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Start our driver, or skip the test.
///
/// A one-megabyte term buffer rather than the default sixty-four: the samples'
/// messages are tiny, and a log buffer is *three* terms — a test that used the
/// default would write 192 MiB per publication to prove something that 3 MiB
/// proves as well. The cost is that the publisher's window is half a megabyte,
/// which is still two orders of magnitude more than these tests send.
fn start(case: &str) -> Option<OwnDriver> {
    // The case is part of the directory's name because a test that panics
    // leaves its driver running until the harness drops it: two cases in one
    // process must not want the same directory.
    let name = format!("own-driver-ref-client-{case}");
    let driver = OwnDriver::start_with(&name, &["-Daeron.ipc.term.buffer.length=1m"]);

    if driver.is_none() {
        driver::announce_own_skip();
    }

    driver
}

#[test]
fn the_reference_client_publishes_to_our_driver_and_reads_it_back() {
    let Some(publisher_binary) = sample("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };
    let Some(subscriber_binary) = sample("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };
    let Some(mut driver) = start("pubsub") else {
        return;
    };

    driver
        .await_cnc(READY_TIMEOUT)
        .expect("our driver publishes a readable CnC file");
    let dir = driver.aeron_dir().to_owned();

    // The subscriber first. A publication with no reader cannot be offered to
    // — the driver holds its limit at the producer's position — and the sample
    // does not retry a message it could not send.
    let mut subscriber = Sample::start(
        &subscriber_binary,
        "subscriber",
        &dir,
        &["-c", "aeron:ipc", "-s", &STREAM_ID.to_string()],
    );
    subscriber.await_output(Duration::from_secs(20), "its subscription", |output| {
        output.contains("Subscription channel status")
    });

    let mut publisher = Sample::start(
        &publisher_binary,
        "publisher",
        &dir,
        &["-c", "aeron:ipc", "-s", &STREAM_ID.to_string(), "-m", "4"],
    );

    // While it is publishing, the counters this driver is supposed to publish
    // are readable by the reference's own tool — one row per position, with
    // the labels the reference gives them. This is the only place they are
    // checked against a reader that is not ours.
    publisher.await_output(Duration::from_secs(30), "its first offer", |output| {
        output.contains("yay!")
    });

    if let Some(aeron_stat) = driver::locate_aeron_stat() {
        let output = Command::new(aeron_stat)
            .arg("-d")
            .arg(&dir)
            .arg("-w")
            .arg("false")
            .output()
            .expect("run AeronStat");

        let text = String::from_utf8_lossy(&output.stdout);
        for row in ["pub-pos (concurrent)", "pub-lmt", "sub-pos"] {
            assert!(
                text.contains(row),
                "AeronStat does not see `{row}` while a publication and a subscription are live:\n{text}"
            );
        }
    } else {
        driver::announce_tool_skip("AeronStat");
    }

    // The publisher finishes on its own; four messages is a window that has to
    // open four times, since the sample does not retry.
    let status = publisher
        .await_exit(SAMPLE_TIMEOUT)
        .expect("the publisher finishes");
    let output = publisher.output();
    assert!(status.success(), "the publisher failed:\n{output}");
    assert_eq!(
        4,
        output.matches("yay!").count(),
        "every message has to be offered into an open window:\n{output}"
    );
    assert!(
        !output.contains("not connected") && !output.contains("back pressure"),
        "the driver never opened the window:\n{output}"
    );

    // And the reader has them, in the frames this driver's publication carried.
    let received = subscriber.await_output(Duration::from_secs(20), "every message", |output| {
        output.matches("Message to stream").count() >= 4
    });

    for index in 0..4 {
        assert!(
            received.contains(&format!("<<Hello World! {index}>>")),
            "message {index} is missing from:\n{received}"
        );
    }
    assert!(
        received.contains(&format!("Message to stream {STREAM_ID} from session ")),
        "the frame header carries the stream:\n{received}"
    );

    // The session id the subscriber reads out of a *frame* is the one the
    // driver put in the log's metadata template and in `ON_AVAILABLE_IMAGE` —
    // three places that have to agree for this line to be well formed.
    // The session id the subscriber reads out of a *frame* is the one the
    // driver wrote into the log's metadata template and sent in
    // `ON_AVAILABLE_IMAGE` — three places that have to agree for this line to
    // be well formed at all. The digits are taken rather than the whole field:
    // the installed sample's format string is not the one in the 1.53.2 source
    // (`from session 123(14@0)` against `from session 123 (14 bytes)`), which
    // is itself worth knowing — the reference's sample binaries are built from
    // whatever revision last built them, and `docs/reference.md` says so.
    let rest = received
        .split("from session ")
        .nth(1)
        .expect("a session id in the subscriber's output");
    let digits: String = rest
        .char_indices()
        .take_while(|(index, character)| {
            character.is_ascii_digit() || (*index == 0 && *character == '-')
        })
        .map(|(_, character)| character)
        .collect();
    let session_id: i32 = digits.parse().expect("a session id");

    // A speculated session id starts from a random `i32`, so it is outside the
    // range the driver keeps for itself on **both** sides of zero.
    assert!(
        !(-1..=1000).contains(&session_id),
        "the driver speculates session ids outside its reserved range: {session_id}"
    );

    // When the publisher's client goes, the driver drains the publication and
    // tells every reader its image is gone — with the position the stream ended
    // at, which is where the producer got to: four frames of sixty-four bytes.
    // This is our side of the life cycle (`REMOVE`/client death) read by the
    // reference's own client, and the position is the end-of-stream byte this
    // driver wrote into the log reaching a reader that is not ours.
    let ended =
        subscriber.await_output(Duration::from_secs(30), "its image to go away", |output| {
            output.contains("Unavailable image on correlationId=")
        });
    assert!(
        ended.contains("Unavailable image on correlationId=4"),
        "the unavailable image names the publication:\n{ended}"
    );
    assert!(
        ended.contains(&format!("sessionId={session_id}")),
        "and the session this reader was reading:\n{ended}"
    );
    assert!(
        ended.contains("from aeron:ipc"),
        "the source identity is the constant the driver sends:\n{ended}"
    );

    // The position in that line is the *reader's* own, and the reference
    // reports it whenever its handler runs — so it is not asserted to be the
    // end of the stream. What is asserted is that the client was never told the
    // driver had died: a force-close produces the same handler call, and this
    // is what tells the two apart.
    for complaint in ["keepalive", "driver timeout", "shutdown"] {
        assert!(
            !ended.to_lowercase().contains(complaint),
            "the client must not have timed the driver out (`{complaint}`):\n{ended}"
        );
    }

    // A sample stopped the way the reference stops one: a signal, a clean exit.
    let status = subscriber
        .terminate(Duration::from_secs(10))
        .expect("the subscriber stops");
    assert!(
        status.success(),
        "the subscriber did not shut down cleanly:\n{}",
        subscriber.output()
    );

    assert!(
        driver.stop().is_ok(),
        "the driver stops on a signal:\n{}",
        driver.log_tail(20)
    );
}
