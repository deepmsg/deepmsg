//! P2-C4's eighth commit, first half: the driver's **receive-side loss
//! injection**, judged where it acts.
//!
//! # What is judged here
//!
//! That a rule written into the driver's settings **withholds frames as they
//! arrive**, and only the ones it names. Each case is one run of the same
//! exchange — one publication on a UDP channel, one subscription on it, eight
//! messages — under a different rule:
//!
//! | rule | what must happen |
//! |---|---|
//! | none | all eight arrive, and the driver never asks for anything again |
//! | `stream.id=<this stream>` | **none** arrive — the rule is total, so the retransmissions are lost too |
//! | `stream.id=<another stream>` | all eight arrive — the rule is about the stream it names |
//! | `stream.id=…` + `every=2` | all eight arrive, **and the driver sent a NAK** |
//!
//! # Why the last two are the point, and why the NAK is what says so
//!
//! A receiver that loses a frame does not simply do without it: it notices the
//! gap, asks for it again, and the sender sends it. So "loss was injected" and
//! "everything arrived" are not in tension — the second is what *proves the
//! first was survivable*. What they are is **indistinguishable from a run that
//! lost nothing**, which is why the NAK counter is asserted beside them: without
//! it, a rule that quietly dropped nothing would pass every case in this file.
//!
//! The total-loss case is the mirror image. If its own recovery had worked, the
//! messages would have arrived; that none of them does — while the driver is
//! asking for them — is what shows the rule is a rule and not a hiccup.
//!
//! # What is not here
//!
//! The two **positioned** rules (`setup.at.position`, `data.in.range`) are
//! judged at the unit level instead — each generator by its own test, and the
//! rule a set of settings builds by `the_rule_a_spec_builds_is_the_one_its_settings_name`
//! (`crates/driver/src/media/receive_loss.rs`). Naming a term id from out here
//! means observing one, and what a test would be asserting is then its own
//! guess about where a publication's first term is rather than the rule under
//! it. Their end-to-end criterion is the case they exist for — *the recording
//! stays whole while the reader's copy is missing a range* — which needs a
//! subscriber and an archive together, and belongs with the criteria that have
//! both.
//!
//! Everything here is our own — our driver, our client — so it runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream that is published and subscribed to.
const STREAM_ID: i32 = 5101;

/// A stream nothing in these tests uses, for the rule that must not fire.
const OTHER_STREAM_ID: i32 = 5102;

/// How many messages one run publishes.
const MESSAGES: usize = 8;

/// How long anything here may take.
const DEADLINE: Duration = Duration::from_secs(30);

/// How long a run that expects **nothing** waits before it is sure.
///
/// Long enough that a delivery which was going to happen has happened: the
/// exchange below takes single-digit milliseconds when it works at all.
const QUIET: Duration = Duration::from_secs(2);

/// A UDP port this process owns: above `ip_local_port_range`, one slot per
/// process, `offset` ports in ([`deepmsg_tests::ports`]).
fn free_port(offset: u16) -> u16 {
    deepmsg_tests::ports::port(offset)
}

/// What a subscription was handed.
#[derive(Default)]
struct Collector {
    messages: Vec<Vec<u8>>,
}

impl ControlledHandler for Collector {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        self.messages.push(message.payload.to_vec());

        Action::Continue
    }
}

/// What one run came back with.
struct Run {
    /// The messages the subscription was handed, in order.
    messages: Vec<Vec<u8>>,
    /// How many NAKs the driver had sent by the end — the witness that the
    /// frames it did not deliver were **missed** rather than never sent.
    naks_sent: i64,
    /// How many images the subscription had at the end — the witness that the
    /// channel itself is up, which is what makes a missing message a *drop*.
    images: usize,
}

/// Run the exchange once under `properties`, or [`None`] when our driver is not
/// built.
fn run(test_name: &str, port_offset: u16, properties: &[&str]) -> Option<Run> {
    let Some(own) = OwnDriver::start_with(test_name, properties) else {
        driver::announce_own_skip();
        return None;
    };

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!("aeron:udp?endpoint=127.0.0.1:{}", free_port(port_offset));

    let publication = client
        .add_exclusive_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver takes a publication");

    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver takes a subscription");

    // Both ends have to be joined before anything is published, and the
    // subscription needs an image — a message offered before there is one is
    // one the publication holds, not one that was lost.
    until(&mut client, "the publication to link", |client| {
        client
            .exclusive_publication(publication)
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false)
    });

    until(
        &mut client,
        "the subscription to see the publication",
        |client| {
            client
                .subscription(subscription)
                .is_some_and(|subscription| !subscription.images().is_empty())
        },
    );

    for index in 0..MESSAGES {
        let payload = format!("message {index}").into_bytes();

        until(&mut client, "the message to be taken", |client| {
            client
                .offer_exclusive(publication, &payload)
                .is_some_and(|outcome| matches!(outcome, Appended::Ok { .. }))
        });
    }

    let mut collector = Collector::default();

    // Read until everything is there, or until a run that expects nothing has
    // waited long enough to be sure.
    let deadline = Instant::now() + DEADLINE;
    let quiet_until = Instant::now() + QUIET;

    while collector.messages.len() < MESSAGES && Instant::now() < deadline {
        client.poll_subscription_controlled(subscription, MESSAGES, &mut collector);

        if Instant::now() >= quiet_until && collector.messages.is_empty() {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    // A last look, so that a message delivered during the wait above is not
    // counted as missing.
    client.poll_subscription_controlled(subscription, MESSAGES, &mut collector);

    Some(Run {
        messages: collector.messages,
        naks_sent: naks_sent(&client),
        images: client
            .subscription(subscription)
            .map_or(0, |subscription| subscription.images().len()),
    })
}

/// What the driver's `NAKs sent` system counter says.
fn naks_sent(client: &Client) -> i64 {
    let Some(reader) = client.counters_reader() else {
        return 0;
    };

    let mut naks = 0;

    let _ = reader.for_each(|descriptor| {
        if descriptor.label == "NAKs sent" {
            naks = descriptor.value;
        }
    });

    naks
}

/// Drive a client until `done`, or fail.
fn until(client: &mut Client, what: &str, mut done: impl FnMut(&mut Client) -> bool) {
    let deadline = Instant::now() + DEADLINE;

    while !done(client) {
        assert!(Instant::now() < deadline, "{what} never happened");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The eight messages, in the order they were published.
fn expected() -> Vec<Vec<u8>> {
    (0..MESSAGES)
        .map(|index| format!("message {index}").into_bytes())
        .collect()
}

/// With no rule at all, everything arrives and the driver asks for nothing:
/// the control for every case below.
#[test]
fn nothing_is_lost_when_no_rule_is_configured() {
    let Some(run) = run("receive-loss-none", 0, &[]) else {
        return;
    };

    assert_eq!(expected(), run.messages, "the exchange itself is broken");
    assert_eq!(
        0, run.naks_sent,
        "a driver that lost nothing should never have asked for a frame again",
    );
}

/// ★ A rule that names a stream takes **all** of it: the frame is dropped on
/// its way in and so is every retransmission of it, which is what makes the
/// loss real rather than reported.
#[test]
fn a_stream_rule_takes_everything_on_that_stream() {
    let property = format!("-Ddeepmsg.debug.receive.data.loss.stream.id={STREAM_ID}");

    let Some(run) = run("receive-loss-stream", 10, &[property.as_str()]) else {
        return;
    };

    assert!(
        run.messages.is_empty(),
        "the rule named this stream and {} message(s) still arrived",
        run.messages.len(),
    );
    assert!(
        run.images > 0,
        "the subscription has no image at all, so what this case shows is a \
         channel that never came up rather than a stream that was dropped",
    );
}

/// The same rule, naming a stream nothing is published on: proof that what the
/// case above measures is the *rule* and not a broken exchange.
#[test]
fn a_stream_rule_leaves_other_streams_alone() {
    let property = format!("-Ddeepmsg.debug.receive.data.loss.stream.id={OTHER_STREAM_ID}");

    let Some(run) = run("receive-loss-other-stream", 20, &[property.as_str()]) else {
        return;
    };

    assert_eq!(expected(), run.messages);
}

/// ★ A rule that takes **one frame in every two** of a stream: every message
/// still arrives, and the NAK counter is what says the loss was there to
/// recover from.
///
/// Without that counter this case would pass against a rule that dropped
/// nothing at all — which is the shape of a green test that proves nothing, and
/// the reason the counter is asserted rather than merely available.
#[test]
fn a_rate_rule_loses_frames_that_the_driver_then_recovers() {
    let properties = [
        format!("-Ddeepmsg.debug.receive.data.loss.stream.id={STREAM_ID}"),
        "-Ddeepmsg.debug.receive.data.loss.every=2".to_owned(),
    ];
    let properties: Vec<&str> = properties.iter().map(String::as_str).collect();

    let Some(run) = run("receive-loss-rate", 30, &properties) else {
        return;
    };

    assert_eq!(
        expected(),
        run.messages,
        "every message should have been recovered",
    );
    assert!(
        run.naks_sent > 0,
        "nothing was dropped, so this case says nothing about recovery",
    );
}

/// A rule with no stream named takes a frame in every two of **anything** —
/// the reference's `aeron_frame_data_loss_generator`, without its predicate.
#[test]
fn a_rate_rule_with_no_stream_takes_from_any_stream() {
    let Some(run) = run(
        "receive-loss-rate-any",
        40,
        &["-Ddeepmsg.debug.receive.data.loss.every=2"],
    ) else {
        return;
    };

    assert_eq!(expected(), run.messages);
    assert!(
        run.naks_sent > 0,
        "nothing was dropped, so this case says nothing about recovery",
    );
}

/// ★ A **window** has two ends, and this is the one `pass.first` could not
/// name: a blackout that opens *later* leaves alone everything that came before
/// it. The reference's response-dropping cases need exactly that — its handshake
/// and its listing go through and one response is held back — and counting
/// frames cannot express it, because the handshake's frame count is not a
/// constant here (measured at three *and* four).
///
/// The window named here opens far beyond the run, so the exchange that finishes
/// in milliseconds is untouched: the same reading as the case with no rule at
/// all, from a rule that is configured. Where the window's *middle* and its
/// `until` fall is the generator's own tests' business, because those need real
/// time (`crates/driver/src/media/receive_loss.rs`).
#[test]
fn a_window_that_opens_later_leaves_what_came_before_it_alone() {
    let Some(run) = run(
        "receive-loss-window",
        50,
        &[
            "-Ddeepmsg.debug.receive.data.loss.stream.id=5101",
            "-Ddeepmsg.debug.receive.data.loss.from.millis=20000",
            "-Ddeepmsg.debug.receive.data.loss.until.millis=25000",
        ],
    ) else {
        return;
    };

    assert_eq!(expected(), run.messages);
    assert_eq!(
        0, run.naks_sent,
        "a window that had not opened yet dropped something",
    );
}

/// Two rules at once are refused where the settings are read: which of them
/// would win is not something a reader of the settings could tell.
#[test]
fn two_rules_at_once_are_refused() {
    let Some(mut own) = OwnDriver::start_with(
        "receive-loss-two-rules",
        &[
            "-Ddeepmsg.debug.receive.data.loss.stream.id=5101",
            "-Ddeepmsg.debug.receive.data.loss.data.in.range=5101,0,0,4096",
        ],
    ) else {
        driver::announce_own_skip();
        return;
    };

    // The driver refuses to start, so there is no CnC file to connect to and
    // what it said is in its log.
    assert!(
        own.await_cnc(Duration::from_secs(5)).is_err(),
        "a driver with two loss rules should not have come up",
    );

    let log = own.log_tail(20);
    assert!(
        log.contains("only one can be in force"),
        "the driver should have said why it refused: {log}",
    );
}
