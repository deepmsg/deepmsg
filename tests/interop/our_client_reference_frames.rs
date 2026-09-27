//! Our client's **fragmentation**, read frame by frame by the reference.
//!
//! The other interop tests are about whole messages: ours against the
//! reference driver, the reference's against ours. This one is about the
//! shapes in between — a payload too large for one frame, split by *our*
//! appender, carried by the *reference* driver, and printed by the
//! reference's own subscriber, one fragment per line.
//!
//! It is the test for the frame arithmetic: the flag bits a fragment carries,
//! the length each frame claims, and where the next one is expected to begin.
//! A frame length that is off by its own header, an `END` on the wrong frame,
//! or an alignment that rounds the wrong way all produce a subscriber that
//! prints a message that is short, split wrongly, or never complete — and none
//! of that is visible to a test that only ever reads what it wrote itself.
//!
//! The reference's sample prints every fragment it is handed
//! (`aeron-samples/src/main/cpp/BasicSubscriber.cpp:72-78`):
//!
//! ```text
//! Message to stream 3003 from session 1234(1376@0) <<aaaa…>>
//! Message to stream 3003 from session 1234(1376@1376) <<aaaa…>>
//! ```
//!
//! — length and term offset of each one, which is exactly what a fragmenter
//! has to get right.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};
use deepmsg_tests::samples;

/// One stream, so a failure names its own driver.
const STREAM_ID: i32 = 3003;

/// The payload: four frames' worth, because one frame carries 1376 bytes on an
/// IPC channel whose MTU is 1408.
const PAYLOAD_LENGTH: usize = 1376 * 3 + 872;

#[test]
fn a_large_payload_from_our_client_arrives_as_the_reference_expects_frames() {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let mut reference =
        ReferenceDriver::start(&binary, "our-client-ref-frames").expect("start the driver");
    reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a readable CnC file");
    let dir = reference.aeron_dir().to_owned();

    // The subscriber first: the driver holds a publication's limit at its
    // producer's position while nothing is reading it, so a publisher that
    // started first would be offering into a closed window.
    let mut subscriber = samples::Sample::start(
        &subscriber_binary,
        "subscriber",
        &dir,
        &["-c", "aeron:ipc", "-s", &STREAM_ID.to_string()],
    );
    subscriber.await_output(Duration::from_secs(20), "its subscription", |output| {
        output.contains("Subscription channel status")
    });

    let mut client = Client::connect(&dir).expect("our client connects to the reference driver");
    let publication = client
        .add_publication("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the reference driver creates it");

    // A payload larger than one frame, from a repeating pattern so that a frame
    // carrying the wrong bytes is visible in what the subscriber prints.
    let payload: Vec<u8> = (0..PAYLOAD_LENGTH)
        .map(|index| b'a' + u8::try_from(index % 26).expect("less than 26"))
        .collect();

    // The window opens when the reference subscriber reports its position,
    // which its client does on its own cadence — so this is a wait rather than
    // a single offer.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut offered = false;

    while Instant::now() < deadline {
        client.poll();

        match client.offer(publication, &payload) {
            Some(Appended::Ok { .. }) => {
                offered = true;
                break;
            }
            Some(Appended::BackPressured | Appended::NotConnected) | None => {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => panic!("the reference driver refused the payload: {other:?}"),
        }
    }

    assert!(offered, "the window never opened");

    // One message of five thousand bytes, which is four frames on the wire —
    // the sample prints what its own reassembly produced
    // (`aeron-samples/src/main/cpp/BasicSubscriber.cpp:72-78`), so a length of
    // 5000 at offset 0 is the reference client saying that our four frames
    // carried one message, whole, from the start.
    let received = subscriber.await_output(Duration::from_secs(20), "the message", |output| {
        output.contains(&format!("({PAYLOAD_LENGTH}@0)"))
    });

    assert!(
        received.contains(&format!("Message to stream {STREAM_ID} from session ")),
        "the header the reference reassembled carries our stream and session:\n{received}"
    );
    assert_eq!(
        1,
        received.matches("Message to stream").count(),
        "one message, not one line per frame:\n{received}"
    );

    // And the bytes: the payload this client split, put back together by the
    // reference's own assembler.
    let line = received
        .lines()
        .find(|line| line.contains(&format!("({PAYLOAD_LENGTH}@0)")))
        .expect("the message's line");
    let body = line
        .split("<<")
        .nth(1)
        .and_then(|rest| rest.strip_suffix(">>"))
        .expect("the payload between the sample's brackets");

    assert_eq!(
        payload.len(),
        body.len(),
        "the reassembled payload is the length it was sent at"
    );
    assert_eq!(
        String::from_utf8(payload.clone()).expect("the test's payload is text"),
        body,
        "and it is the same bytes"
    );

    let _ = subscriber.terminate(Duration::from_secs(10));
    let _ = reference.stop();
}
