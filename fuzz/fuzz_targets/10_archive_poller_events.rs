#![no_main]

//! Target 10: the archive's client-side pollers and the recording-event
//! dispatcher.
//!
//! A poller is a one-slot state machine fed whole messages. The fuzzer plays
//! the archive: any bytes, any header, and a message that fails to decode must
//! leave the slot usable — the next good message still has to land. The golden
//! control response is the witness for that second half.

use deepmsg_archive::client::descriptor_poller::RecordingDescriptorPoller;
use deepmsg_archive::client::poller::ControlResponsePoller;
use deepmsg_archive::client::recording_events::{RecordingEventsListener, dispatch};
use deepmsg_archive::client::subscription_descriptor_poller::RecordingSubscriptionDescriptorPoller;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message, MessageHeader};
use libfuzzer_sys::fuzz_target;

/// A control response the reference's generator wrote — the bytes a poller has
/// to accept, whatever the fuzzer just fed it.
const CONTROL_RESPONSE_OK: &[u8] =
    include_bytes!("../../tests/fixtures/sbe/101-001-control-response-ok.bin");

/// The golden control response with its code replaced by `code` — the two
/// pollers' "there are no more" markers (`RECORDING_UNKNOWN` is 2,
/// `SUBSCRIPTION_UNKNOWN` is 3).
fn golden_ending(code: i32) -> Vec<u8> {
    let mut response = CONTROL_RESPONSE_OK.to_vec();
    response[32..36].copy_from_slice(&code.to_le_bytes());
    response
}

fn message(payload: &[u8]) -> Message<'_> {
    Message {
        header: MessageHeader {
            session_id: 1,
            stream_id: 2,
            term_offset: 0,
            flags: 0xC0,
            position: 0,
            frame_length: i32::try_from(payload.len()).unwrap_or(i32::MAX) + 32,
            fragmented_frame_length: i32::try_from(payload.len()).unwrap_or(i32::MAX) + 32,
            image_registration_id: 0,
        },
        payload,
    }
}

#[derive(Default)]
struct CountingListener {
    started: usize,
    progressed: usize,
    stopped: usize,
}

impl RecordingEventsListener for CountingListener {
    fn on_start(
        &mut self,
        _recording_id: i64,
        _start_position: i64,
        _session_id: i32,
        _stream_id: i32,
        _channel: &str,
        _source_identity: &str,
    ) {
        self.started += 1;
    }

    fn on_progress(&mut self, _recording_id: i64, _start_position: i64, _position: i64) {
        self.progressed += 1;
    }

    fn on_stop(&mut self, _recording_id: i64, _start_position: i64, _stop_position: i64) {
        self.stopped += 1;
    }
}

fuzz_target!(|data: &[u8]| {
    let mut poller = ControlResponsePoller::new(1, 10);
    let _ = poller.on_message(message(data));

    // Whatever the fuzz message did, a golden response still decodes.
    poller.reset();
    let _ = poller.on_message(message(CONTROL_RESPONSE_OK));
    assert!(
        poller.is_control_response(),
        "a good response after arbitrary bytes did not decode"
    );

    // The two descriptor pollers decode the same response family. Whatever the
    // fuzz message did, a reset plus a "there are no more" response still ends
    // the dispatch. The fixture's session is 56 and its correlation 59; only
    // the code differs between the two pollers' end markers.
    let mut descriptors = RecordingDescriptorPoller::new(1, 56, 10, |_| {});
    let _ = descriptors.on_message(message(data));
    descriptors.reset(59, 10);
    assert_eq!(
        Action::Break,
        descriptors.on_message(message(&golden_ending(2))),
        "a good response after arbitrary bytes did not decode"
    );

    let mut subscriptions = RecordingSubscriptionDescriptorPoller::new(1, 56, 10, |_| {});
    let _ = subscriptions.on_message(message(data));
    subscriptions.reset(59, 10);
    assert_eq!(
        Action::Break,
        subscriptions.on_message(message(&golden_ending(3))),
        "a good response after arbitrary bytes did not decode"
    );

    let mut listener = CountingListener::default();
    let _ = dispatch(data, &mut listener);
    assert!(listener.started + listener.progressed + listener.stopped <= data.len());

    // The golden recording-started signal must reach the listener.
    let mut listener = CountingListener::default();
    let started = include_bytes!("../../tests/fixtures/sbe/101-101-recording-started.bin");
    let _ = dispatch(started, &mut listener);
    assert_eq!(
        1, listener.started,
        "the golden started signal was not delivered"
    );
});
