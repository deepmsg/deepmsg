//! The archive client's control-response poller, on a subscription it reads.
//!
//! Every message here is published for real and read back by the poller, because
//! what this type does is *decide* — which of three templates it understands,
//! what to do with one it does not, and when to stop. None of that is visible
//! from the slot alone, and all of it is what the layer above depends on:
//!
//! * **One message per poll.** The poll stops at the first message it
//!   understands and leaves the rest, so a caller reads one answer at a time
//!   without ever losing one.
//! * **A template it does not read is stepped over**, not treated as an answer
//!   and not treated as an error.
//! * **A recording signal is not an answer** — it fills the slot and completes
//!   the poll exactly as a response does, and the flag is the only thing that
//!   says so.
//! * **A malformed message fails the poll**, which is the one case where the
//!   reason cannot travel in the slot.

use std::time::{Duration, Instant};

use deepmsg_archive::client::poller::{ControlResponseError, ControlResponsePoller};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::challenge_codec::ChallengeEncoder;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::ControlResponseEncoder;
use deepmsg_codec::archive::message_header_codec;
use deepmsg_codec::archive::recording_signal::RecordingSignal;
use deepmsg_codec::archive::recording_signal_event_codec::RecordingSignalEventEncoder;
use deepmsg_codec::archive::{SBE_SCHEMA_ID, WriteBuf};
use deepmsg_tests::driver::{self, OwnDriver};

/// Where the archive's answers would arrive. IPC, so the reader is in this
/// process.
const CHANNEL: &str = "aeron:ipc";

/// A stream of its own — no real archive uses it, so a failure here reads as
/// this test's and not as some other stream's.
const STREAM_ID: i32 = 4212;

const TIMEOUT: Duration = Duration::from_secs(5);

/// The session the archive would have named, chosen so that a slot left at the
/// `-1` default could not pass for it.
const CONTROL_SESSION_ID: i64 = 4_212_000;

/// The template id of a request this poller has no branch for. Any id that is
/// not one of its three will do; this one is not in the schema at all.
const UNKNOWN_TEMPLATE_ID: u16 = 999;

/// A driver, a client, and a publication with a reader on it.
struct Rig {
    /// The driver, which has to outlive the client: dropping it kills it.
    _own: OwnDriver,
    client: Client,
    publication: i64,
    subscription: i64,
}

/// Start a rig, or answer `None` when our driver is not built.
fn rig(test_name: &str) -> Option<Rig> {
    let Some(mut own) = OwnDriver::start(test_name) else {
        driver::announce_own_skip();
        return None;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_exclusive_publication(CHANNEL, STREAM_ID, TIMEOUT)
        .expect("the driver must accept the publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, TIMEOUT)
        .expect("the driver must accept a reader");

    let deadline = Instant::now() + TIMEOUT;
    loop {
        client.poll();

        let linked = client
            .exclusive_publication(publication)
            .and_then(|publication| publication.is_connected())
            .unwrap_or(false);
        let has_image = client
            .subscription(subscription)
            .is_some_and(|subscription| !subscription.images().is_empty());

        if linked && has_image {
            break;
        }

        assert!(
            Instant::now() < deadline,
            "the reader never met the publication (linked={linked}, image={has_image})"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    Some(Rig {
        _own: own,
        client,
        publication,
        subscription,
    })
}

/// Publish one message and wait until it is in the log.
fn publish(rig: &mut Rig, payload: &[u8]) {
    let deadline = Instant::now() + TIMEOUT;

    loop {
        match rig.client.offer_exclusive(rig.publication, payload) {
            Some(deepmsg_core::logbuffer::append::Appended::Ok { .. }) => return,
            Some(deepmsg_core::logbuffer::append::Appended::EndOfLog)
            | Some(deepmsg_core::logbuffer::append::Appended::MidRotation) => {}
            Some(other) => panic!("the message was not published: {other:?}"),
            None => panic!("the publication is gone"),
        }

        assert!(Instant::now() < deadline, "the message never landed");
        rig.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Poll once, which is one read of the slot.
fn poll(rig: &mut Rig, poller: &mut ControlResponsePoller) -> Result<usize, ControlResponseError> {
    rig.client.poll();
    poller.poll(&mut rig.client)
}

// ---------------------------------------------------------------------------
// The three templates, as an archive would write them
// ---------------------------------------------------------------------------

/// A `ControlResponse` (1) — the answer to some request.
fn control_response(
    correlation_id: i64,
    relevant_id: i64,
    code: ControlResponseCode,
    error_message: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 1024];

    let length = {
        let encoder = ControlResponseEncoder::default().wrap(
            WriteBuf::new(&mut buffer),
            message_header_codec::ENCODED_LENGTH,
        );
        let mut header = encoder.header(0);
        let mut encoder = header.parent().expect("the encoder the header wrapped");

        encoder
            .control_session_id(CONTROL_SESSION_ID)
            .correlation_id(correlation_id)
            .relevant_id(relevant_id)
            .code(code)
            .version(7)
            .error_message(error_message.as_bytes());

        message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// A `Challenge` (59) — the archive asking the client to prove itself.
fn challenge(correlation_id: i64, encoded_challenge: &[u8]) -> Vec<u8> {
    let mut buffer = vec![0u8; 1024];

    let length = {
        let encoder = ChallengeEncoder::default().wrap(
            WriteBuf::new(&mut buffer),
            message_header_codec::ENCODED_LENGTH,
        );
        let mut header = encoder.header(0);
        let mut encoder = header.parent().expect("the encoder the header wrapped");

        encoder
            .control_session_id(CONTROL_SESSION_ID)
            .correlation_id(correlation_id)
            .version(7)
            .encoded_challenge(encoded_challenge);

        message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// A `RecordingSignalEvent` (24) — a lifecycle signal, which is not an answer.
fn recording_signal_event(
    correlation_id: i64,
    recording_id: i64,
    subscription_id: i64,
    position: i64,
    signal: RecordingSignal,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 1024];

    let length = {
        let encoder = RecordingSignalEventEncoder::default().wrap(
            WriteBuf::new(&mut buffer),
            message_header_codec::ENCODED_LENGTH,
        );
        let mut header = encoder.header(0);
        let mut encoder = header.parent().expect("the encoder the header wrapped");

        encoder
            .control_session_id(CONTROL_SESSION_ID)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .subscription_id(subscription_id)
            .position(position)
            .signal(signal);

        message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// The same message, claiming to be another template.
///
/// The header is `block_length`, `template_id`, `schema_id`, `version`, each two
/// bytes — so the id is at offset 2 and the schema's at 4
/// (`message_header_codec.rs:60-109`).
fn claiming(payload: &[u8], template_id: u16) -> Vec<u8> {
    let mut payload = payload.to_vec();
    payload[2..4].copy_from_slice(&template_id.to_le_bytes());
    payload
}

/// The same message, claiming to come from another schema.
fn from_another_schema(payload: &[u8]) -> Vec<u8> {
    let mut payload = payload.to_vec();
    payload[4..6].copy_from_slice(&SBE_SCHEMA_ID.wrapping_add(1).to_le_bytes());
    payload
}

// ---------------------------------------------------------------------------
// The criteria
// ---------------------------------------------------------------------------

/// A `ControlResponse` fills the slot, code and reason and all.
#[test]
fn a_control_response_fills_the_slot() {
    let Some(mut rig) = rig("archive-poller-response") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 10);

    publish(
        &mut rig,
        &control_response(11, 42, ControlResponseCode::OK, ""),
    );
    assert_eq!(1, poll(&mut rig, &mut poller).expect("the poll reads it"));

    assert!(poller.is_poll_complete());
    assert!(poller.is_control_response(), "and it is a response");
    assert!(!poller.was_challenged());
    assert!(!poller.is_recording_signal());
    assert_eq!(CONTROL_SESSION_ID, poller.control_session_id());
    assert_eq!(11, poller.correlation_id());
    assert_eq!(42, poller.relevant_id(), "what the answer is about");
    assert_eq!(Some(7), poller.version());
    assert!(poller.is_code_ok());
    assert!(!poller.is_code_error());
    assert_eq!(b"", poller.error_message(), "OK carries no reason");

    // An `ERROR` is the same message with a reason in it, and the reason is the
    // one thing a caller acts on.
    publish(
        &mut rig,
        &control_response(
            12,
            43,
            ControlResponseCode::ERROR,
            "unknown recording id: 99",
        ),
    );
    // The poll resets the slot itself, because the last one completed.
    assert_eq!(1, poll(&mut rig, &mut poller).expect("the poll reads it"));

    assert_eq!(12, poller.correlation_id());
    assert_eq!(43, poller.relevant_id());
    assert!(poller.is_code_error());
    assert!(!poller.is_code_ok());
    assert_eq!(b"unknown recording id: 99", poller.error_message());
}

/// A challenge fills the slot too, and carries **no** code.
///
/// Which is what tells it apart from an `ERROR` response: a caller that tested
/// `is_code_error` and nothing else would take a challenge for a refusal.
#[test]
fn a_challenge_fills_the_slot_and_is_not_a_response() {
    let Some(mut rig) = rig("archive-poller-challenge") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 10);

    publish(&mut rig, &challenge(21, b"encoded-challenge"));
    assert_eq!(1, poll(&mut rig, &mut poller).expect("the poll reads it"));

    assert!(poller.is_poll_complete());
    assert!(poller.was_challenged());
    assert!(
        !poller.is_control_response(),
        "a challenge is not an answer"
    );
    assert!(!poller.is_recording_signal());
    assert_eq!(CONTROL_SESSION_ID, poller.control_session_id());
    assert_eq!(21, poller.correlation_id());
    assert_eq!(-1, poller.relevant_id(), "a challenge is about nothing");
    assert_eq!(Some(7), poller.version());
    assert_eq!(None, poller.code(), "and it has no code at all");
    assert!(!poller.is_code_ok());
    assert!(!poller.is_code_error());
    assert_eq!(b"encoded-challenge", poller.encoded_challenge());
}

/// A recording signal fills the slot and says what it is.
#[test]
fn a_recording_signal_is_not_an_answer() {
    let Some(mut rig) = rig("archive-poller-signal") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 10);

    publish(
        &mut rig,
        &recording_signal_event(31, 7, 8, 4096, RecordingSignal::STOP),
    );
    assert_eq!(1, poll(&mut rig, &mut poller).expect("the poll reads it"));

    assert!(poller.is_poll_complete());
    assert!(poller.is_recording_signal());
    assert!(!poller.is_control_response());
    assert!(!poller.was_challenged());
    assert_eq!(CONTROL_SESSION_ID, poller.control_session_id());
    assert_eq!(31, poller.correlation_id());
    assert_eq!(7, poller.recording_id());
    assert_eq!(8, poller.subscription_id());
    assert_eq!(4096, poller.position());
    assert_eq!(Some(RecordingSignal::STOP), poller.recording_signal());
    assert_eq!(None, poller.code(), "a signal is not a response either");
}

/// **The slot is one message wide.** Two answers, two polls, in order.
///
/// This is the property the reset exists for, and the one that would be lost by
/// a poll that read everything it was allowed to: the second poll *resets* — it
/// does not carry on where the last one stopped — so a caller that reads one
/// message per poll sees both and sees them in order.
#[test]
fn one_poll_reads_one_message() {
    let Some(mut rig) = rig("archive-poller-single-slot") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 10);

    publish(
        &mut rig,
        &control_response(41, 1, ControlResponseCode::OK, ""),
    );
    publish(
        &mut rig,
        &control_response(42, 2, ControlResponseCode::OK, ""),
    );

    // One poll, ten allowed, one read: the scan aborts at the message after the
    // one that completed the slot, so the second is *not* consumed.
    assert_eq!(1, poll(&mut rig, &mut poller).expect("the first poll"));
    assert_eq!(41, poller.correlation_id());

    assert_eq!(1, poll(&mut rig, &mut poller).expect("the second poll"));
    assert_eq!(42, poller.correlation_id(), "the one left for it");
    assert_eq!(2, poller.relevant_id());
}

/// **A template this poller does not read is stepped over.**
///
/// Not an answer and not an error: the message is consumed and the poll carries
/// on to the next — which is what makes a control subscription shared with
/// anything else usable at all. Two of them, so one `Continue` would not be
/// enough to pass.
#[test]
fn a_template_the_poller_does_not_read_is_stepped_over() {
    let Some(mut rig) = rig("archive-poller-unknown-template") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 10);

    let known = control_response(51, 9, ControlResponseCode::OK, "");
    publish(&mut rig, &claiming(&known, UNKNOWN_TEMPLATE_ID));
    publish(&mut rig, &claiming(&known, UNKNOWN_TEMPLATE_ID + 1));
    publish(&mut rig, &known);

    // Three messages in, one poll: the two it does not read are consumed on the
    // way to the one it does.
    assert_eq!(
        3,
        poll(&mut rig, &mut poller).expect("the poll reads all three")
    );
    assert!(poller.is_poll_complete());
    assert_eq!(51, poller.correlation_id());
}

/// **The fragment limit is a budget, and running out is not an answer.**
///
/// With two messages allowed per poll and three unreadable ones in front of the
/// answer, the first poll ends with an empty slot — which is a caller's cue to
/// poll again, not to give up. The next poll picks up where the reader is.
#[test]
fn a_poll_that_runs_out_of_fragments_reports_nothing() {
    let Some(mut rig) = rig("archive-poller-fragment-limit") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 2);

    let known = control_response(61, 4, ControlResponseCode::OK, "");
    for offset in 0..3 {
        publish(&mut rig, &claiming(&known, UNKNOWN_TEMPLATE_ID + offset));
    }
    publish(&mut rig, &known);

    assert_eq!(2, poll(&mut rig, &mut poller).expect("the budget"));
    assert!(
        !poller.is_poll_complete(),
        "two messages it does not read are not an answer"
    );
    assert!(!poller.is_control_response());

    // The reader did not lose its place, so the next poll finds the rest — and
    // the third unreadable one is stepped over on the way.
    assert_eq!(2, poll(&mut rig, &mut poller).expect("the rest"));
    assert!(poller.is_poll_complete());
    assert_eq!(61, poller.correlation_id());
}

/// **A message that is not this build's fails the poll.**
///
/// Two ways, and both are `Err` rather than a flag in the slot: there is no
/// message to describe, so there is nothing to put in it.
///
/// The last part of this — that the poller *recovers*, and the message after the
/// bad one is read normally — is where this build deliberately leaves the
/// reference. Its flag is sticky, so the good message would be reported as an
/// error and then reset away unread
/// (`aeron_archive_control_response_poller.c:120-133`); the reasoning is at
/// `ControlResponsePoller::poll`.
#[test]
fn a_malformed_message_fails_the_poll() {
    let Some(mut rig) = rig("archive-poller-malformed") else {
        return;
    };
    let mut poller = ControlResponsePoller::new(rig.subscription, 10);

    let known = control_response(71, 5, ControlResponseCode::OK, "");
    publish(&mut rig, &from_another_schema(&known));

    assert_eq!(
        Err(ControlResponseError::MalformedMessage),
        poll(&mut rig, &mut poller)
    );
    assert!(!poller.is_poll_complete(), "nothing was read");

    // Too short to hold its own header, which the reference reports as an
    // unwrappable buffer rather than as a schema it does not know.
    publish(&mut rig, &[0u8; 4]);

    assert_eq!(
        Err(ControlResponseError::MalformedMessage),
        poll(&mut rig, &mut poller)
    );

    // And the poller recovers: the next message is read normally.
    publish(&mut rig, &known);

    assert_eq!(1, poll(&mut rig, &mut poller).expect("the poll reads it"));
    assert_eq!(71, poller.correlation_id());
    assert!(poller.is_code_ok());
}
