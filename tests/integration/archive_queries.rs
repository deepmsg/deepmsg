//! P2-S3's acceptance: a **page** of recordings, walked from a cursor.
//!
//! The reference's C suite is this slice's judgement and it needs the reference
//! checkout to run (`shouldFindMultipleRecordingDescriptors`,
//! `aeron_archive_test.cpp:2394-2465`, and `shouldFindRecordingDescriptorForUri`,
//! `:2468-2561`). This is the same claim made from inside the workspace, and it
//! is the one CI actually executes: start our archiving media driver, make two
//! recordings, and ask for them a page at a time.
//!
//! # What it watches, and why each thing
//!
//! A page is a **walk** (`AbstractListRecordingsSession.doWork`, `:79-142`), and
//! the C suite's catalog is two recordings and two calls wide. So what is
//! asserted here is the parts of the walk that a catalog that small cannot show:
//!
//! * **The cursor.** A page is asked for from an id and not from a page number
//!   (`:90-102`), so a request that names the second recording answers with it
//!   and not with the first page again.
//! * **The two ways a page ends.** One is the catalog running out, which the
//!   client is told about with `RECORDING_UNKNOWN` naming an id
//!   (`:107-113`); the other is the page filling up, which it is **not**
//!   (`:136-139`). The second is what the C suite's `count=1` call is about, and
//!   getting it wrong is what makes the *next* request look like the previous
//!   page is unfinished.
//! * **That a finished listing lets go of its session.** A listing holds the
//!   session's `activeListing` slot, and the next request is refused with
//!   `ACTIVE_LISTING` while it does (`ArchiveConductor.java:640-645`). So a
//!   request that follows one is the evidence that the one before it ended —
//!   which is the only thing a client can observe about a page it was never
//!   told the end of.
//! * **The two filters.** Template 9 takes a recording only when **both** its
//!   stream and a piece of its channel match (`ListRecordingsForUriSession
//!   .acceptDescriptor`, `:52-61`), which is the difference between the C
//!   suite's `"333"` and `"3334"` calls.
//!
//! # The channels are the C suite's shape
//!
//! A `LOCAL` UDP publication is recorded through a **spy** subscription
//! (`ArchiveConductor.java:559-560`), and a page is answered over the control
//! session's response channel — the same one a descriptor and a control
//! response come back on, which is why the reads here filter by correlation id
//! rather than by message type.
//!
//! # Why the listing is still read off the channel by hand
//!
//! Plan §2.3's third step is where this file's request sites were to become the
//! product's named methods, and the listing half of it cannot: what the criteria
//! here are about is the **answer that ends a page**, and the product keeps no
//! door onto it. `Archive::list_recordings` hands each descriptor to a consumer
//! and answers the count (`archive.rs:911-931`); the `RECORDING_UNKNOWN` that
//! ends the listing is read by the poller, which takes only its existence
//! (`is_dispatch_complete`, `descriptor_poller.rs:264-268`) — **the reference's
//! own poller does the same thing** (`aeron_archive_recording_descriptor_poller.c
//! :194-200`), so this is the product being faithful rather than a gap in it.
//! The `relevantId` and the code asserted below are on the wire and only there.
//!
//! The one call that could not be made either way is ⑤: a listing asked for
//! **zero** recordings is one the archive ends without answering at all
//! (`conductor.rs:1489-1493`), and the product's wait is for an answer — so
//! `Archive::list_recordings(from, 0, …)` would sit out its whole timeout. One
//! ruler (the fixture reads what the archive sends) covers all the calls here.
//!
//! What the product's listing methods are is a criterion of their own, and they
//! have one: `archive_client_api.rs` and `archive_proxy.rs` drive
//! `list_recordings` and `list_recordings_for_uri` against this same archive.
//! Nothing goes uncovered by this file staying on the channel.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::list_recordings_for_uri_request_codec::ListRecordingsForUriRequestEncoder;
use deepmsg_codec::archive::list_recordings_request_codec::ListRecordingsRequestEncoder;
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::recording_descriptor_codec::{self, RecordingDescriptorDecoder};
use deepmsg_codec::archive::{ReadBuf, WriteBuf};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The connect's correlation id, which its answer echoes.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0003;

/// The stream both recordings are on, which is what makes the uri filter's two
/// halves distinguishable: the stream matches and only the channel does not.
const STREAM_ID: i32 = 33;

/// The two channels recorded, and what the uri filter is asked for. Their ports
/// are this test's own rather than the C suite's 3333 and 3334: those tests are
/// run by hand and these by `cargo test`, and two drivers on one host must not
/// pick the same one.
const CHANNEL_A: &str = "aeron:udp?endpoint=localhost:33433";
/// See [`CHANNEL_A`].
const CHANNEL_B: &str = "aeron:udp?endpoint=localhost:33434";

/// A fragment of both channels, and one of the second alone.
const BOTH_CHANNELS: &str = "3343";
/// See [`BOTH_CHANNELS`].
const SECOND_CHANNEL: &str = "33434";

/// How many messages each recording is asked to carry: enough that the
/// publication has a position, which is what makes the recording a live one
/// rather than a row that was written and never read.
const MESSAGE_COUNT: usize = 8;

/// How long a command to the driver may take.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// The archive's properties, plus the one these channels need.
///
/// `spies.simulate.connection` is what makes a **spy** reader count as a
/// subscriber, which is the shape `ArchiveConductor.java:562-565` creates; the C
/// harness sets it for the same reason (`TestArchive.h:53`).
const PROPERTIES: [&str; 3] = [
    archive::PROPERTIES[0],
    archive::PROPERTIES[1],
    "-Daeron.spies.simulate.connection=true",
];

/// A page of recordings, asked for from the lowest id there is and from one of
/// the recordings' own ids, is walked the way the reference walks it.
#[test]
fn a_page_of_recordings_is_walked_from_a_cursor() {
    let archive_dir = TempDir::new("archive-queries-archive");

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("archive-queries", archive_dir.path(), &PROPERTIES)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    let aeron_dir = media_driver.aeron_dir().to_path_buf();
    let mut client = Client::connect(&aeron_dir).expect("connect to the driver");

    let mut session = Session::connect(
        &mut client,
        &aeron_dir,
        CONNECT_CORRELATION_ID,
        Instant::now() + DEADLINE,
    )
    .unwrap_or_else(|reason| panic!("{reason};\n{}", media_driver.log_tail(40)));

    let control_session_id = session.control_session_id();

    // Two recordings, on one stream and two channels — what the reference's own
    // two cases build (`aeron_archive_test.cpp:2394-2465`).
    let first_session = record(&mut session, &mut client, CHANNEL_A, 1);
    let second_session = record(&mut session, &mut client, CHANNEL_B, 2);

    let all = |from: i64, count: i32| {
        move |correlation_id: i64| {
            list_recordings_request(control_session_id, correlation_id, from, count)
        }
    };
    let for_uri = |count: i32, stream_id: i32, fragment: &'static str| {
        move |correlation_id: i64| {
            list_recordings_for_uri_request(
                control_session_id,
                correlation_id,
                i64::MIN,
                count,
                stream_id,
                fragment,
            )
        }
    };

    // ① A page bigger than the catalog: both recordings, in id order, and then
    // the answer that says there is nothing more. That answer names an id and
    // not a count, and the id is the one past the last recording the walk
    // answered about (`AbstractListRecordingsSession.java:110`, `:129-132`).
    let (page, end) = ask(&mut session, &mut client, 2, true, all(i64::MIN, 10));
    let ids: Vec<i64> = page
        .iter()
        .map(|descriptor| descriptor.recording_id)
        .collect();
    assert_eq!(2, page.len(), "both recordings are in the catalog");
    assert_eq!(
        vec![first_session, second_session],
        page.iter()
            .map(|descriptor| descriptor.session_id)
            .collect::<Vec<_>>(),
        "and they are the two publications that were recorded"
    );
    let end = end.expect("a page that did not fill ends with an answer");
    assert_eq!(ControlResponseCode::RECORDING_UNKNOWN, end.code);
    assert_eq!(
        ids[1] + 1,
        end.relevant_id,
        "the answer names the cursor, which is one past the last recording"
    );

    // ② The cursor is an **id**: a page asked for from the second recording's
    // own id starts there rather than at the first page again (`:90-102`).
    let (page, end) = ask(&mut session, &mut client, 1, true, all(ids[1], 10));
    assert_eq!(
        vec![ids[1]],
        page.iter().map(|d| d.recording_id).collect::<Vec<_>>()
    );
    assert_eq!(
        ids[1] + 1,
        end.expect("one recording is not a page of ten").relevant_id
    );

    // ③ A page of **one** over a catalog of two: the page fills and there is no
    // answer at all — and the listing is over all the same, which is what the
    // request in ④ shows. A listing still in flight would be refused with
    // `ACTIVE_LISTING` (`ArchiveConductor.java:640-645`).
    let (page, end) = ask(&mut session, &mut client, 1, false, all(i64::MIN, 1));
    assert_eq!(
        vec![ids[0]],
        page.iter().map(|d| d.recording_id).collect::<Vec<_>>()
    );
    assert!(end.is_none(), "a page that filled is not answered about");

    // ④ The next request is served rather than refused: the slot is free.
    let (page, end) = ask(&mut session, &mut client, 2, false, all(i64::MIN, 2));
    assert_eq!(2, page.len());
    assert!(end.is_none());

    // ⑤ A count of zero asks for nothing and is answered with nothing — the
    // reference's own quiet end, which sends no answer even though the walk
    // never ran (`:105`, `:136-139`).
    let (page, end) = ask(&mut session, &mut client, 0, false, all(i64::MIN, 0));
    assert!(page.is_empty());
    assert!(end.is_none());

    // And that listing ended too, which is what the next page being served
    // rather than refused says.
    let (page, _) = ask(&mut session, &mut client, 2, true, all(i64::MIN, 10));
    assert_eq!(2, page.len());

    // ⑥ Template 9 takes the **intersection**: the stream and a piece of the
    // channel both have to hold. One channel of the two.
    let (page, end) = ask(
        &mut session,
        &mut client,
        1,
        true,
        for_uri(10, STREAM_ID, SECOND_CHANNEL),
    );
    assert_eq!(
        vec![ids[1]],
        page.iter().map(|d| d.recording_id).collect::<Vec<_>>()
    );
    assert_eq!(
        ids[1] + 1,
        end.expect("one is not a page of ten").relevant_id
    );

    // A piece both channels have: two, and the page is still not full.
    let (page, end) = ask(
        &mut session,
        &mut client,
        2,
        true,
        for_uri(10, STREAM_ID, BOTH_CHANNELS),
    );
    assert_eq!(2, page.len());
    assert_eq!(
        ids[1] + 1,
        end.expect("two is not a page of ten").relevant_id
    );

    // A fragment no channel has: an empty page, which the client is still told
    // about — that answer is what makes it stop waiting rather than hang.
    let (page, end) = ask(
        &mut session,
        &mut client,
        0,
        true,
        for_uri(10, STREAM_ID, "no-match"),
    );
    assert!(page.is_empty());
    assert_eq!(
        ControlResponseCode::RECORDING_UNKNOWN,
        end.expect("an empty page is answered").code
    );

    // And the other half of the intersection on its own: the channel fragment is
    // in both, and the stream is in neither recording.
    let (page, end) = ask(
        &mut session,
        &mut client,
        0,
        true,
        for_uri(10, STREAM_ID + 1, BOTH_CHANNELS),
    );
    assert!(page.is_empty(), "the stream has to hold too");
    assert_eq!(
        ControlResponseCode::RECORDING_UNKNOWN,
        end.expect("an empty page is answered").code
    );

    let _ = media_driver.stop();
}

/// One recording descriptor, as a listing session sends it.
struct Descriptor {
    recording_id: i64,
    session_id: i32,
}

/// Ask for one page and read it: `descriptors` of them, and — when the page is
/// one the archive ends with an answer — that answer too.
///
/// Retried while the archive refuses with `ACTIVE_LISTING`. A request that
/// arrives in the same turn as the listing before it is refused, exactly as the
/// reference refuses it, and asking again is what a client does with that
/// answer. What the retry is **not** is a way past a listing that never ended:
/// one of those is refused every time, and the deadline says so.
///
/// # What the retry is keyed on, and why it is not the answer's code
///
/// It is keyed on **how many descriptors came back**: a page that answered with
/// fewer than were asked for is a page to ask again for, and a full one is the
/// answer. That is deliberately not `code == ERROR`: the `ACTIVE_LISTING`
/// refusal this loop was written for arrives as an `ERROR`, but so does every
/// other refusal, and **a `RECORDING_UNKNOWN` does not arrive as one at all**
/// (`control_response_code.rs`: `ERROR = 1`, `RECORDING_UNKNOWN = 2`).
///
/// The difference matters because the archive answers a listing out of the
/// catalog **as it stands**, and the catalog is written by the archive's own
/// recording session — a turn or two behind the request that started the
/// recording. A listing that arrives in that window is answered with the
/// honest `RECORDING_UNKNOWN` of an empty catalog, and the criterion that is
/// waiting for its recording has to ask again rather than take that for the
/// end of the story. Keyed on the count, it does; keyed on the code, it took
/// the empty answer as final and the caller waited out its whole deadline.
///
/// **The count keys it correctly for the three callers that want an empty
/// page.** ⑤, ⑥ and ⑦ ask for **zero** descriptors and expect the
/// `RECORDING_UNKNOWN` that comes back — and `0 < 0` is false, so none of them
/// ever retries. One ruler separates the two cases; no second flag is needed.
fn ask(
    session: &mut Session,
    client: &mut Client,
    descriptors: usize,
    terminal: bool,
    request: impl Fn(i64) -> Vec<u8>,
) -> (Vec<Descriptor>, Option<archive::Response>) {
    let deadline = Instant::now() + DEADLINE;
    let mut attempts = 0;

    loop {
        let correlation_id = session.next_correlation_id();
        let payload = request(correlation_id);

        session
            .send_only(client, &payload, Instant::now() + DEADLINE)
            .expect("the archive takes the request");

        let (page, end) = read_page(session, client, correlation_id, descriptors, terminal);

        if page.len() >= descriptors {
            return (page, end);
        }

        attempts += 1;
        assert!(
            Instant::now() < deadline,
            "the archive answered {attempts} pages short, the last one with {} of \
             {descriptors} descriptors{}",
            page.len(),
            match &end {
                Some(end) => format!(" and {} saying: {}", end.code, end.message()),
                None => " and no answer at all".to_owned(),
            }
        );
    }
}

/// Read one page off the response channel.
///
/// The descriptors and the answer that may end the page arrive on the same
/// channel under the same correlation id, which is why this reads frames until
/// it has what it is waiting for rather than waiting for one answer.
///
/// An `ERROR` is always the end: it is the refusal of the request itself, and no
/// descriptor can follow it.
fn read_page(
    session: &Session,
    client: &mut Client,
    correlation_id: i64,
    descriptors: usize,
    terminal: bool,
) -> (Vec<Descriptor>, Option<archive::Response>) {
    let deadline = Instant::now() + DEADLINE;
    let subscription = session.response_subscription();
    let mut page = Vec::new();
    let mut end = None;
    let mut buffer = vec![0u8; 4096];

    while page.len() < descriptors || (terminal && end.is_none()) {
        assert!(
            Instant::now() < deadline,
            "the page stopped at {} of {descriptors} descriptors",
            page.len()
        );

        client.poll();

        let Some(image) = client.subscription(subscription).and_then(|subscription| {
            subscription
                .images()
                .first()
                .map(deepmsg_client::image::Image::registration_id)
        }) else {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        };

        client.poll_image(subscription, image, 10, |fragment| {
            if !fragment.is_unfragmented() {
                return;
            }

            let length = fragment.payload_length();
            buffer.resize(length, 0);
            assert!(
                fragment.copy_payload(&mut buffer).is_some(),
                "the frame fits"
            );

            if let Some(descriptor) = decode_descriptor(&buffer, correlation_id) {
                page.push(descriptor);
                return;
            }

            let Some(response) = archive::decode(&buffer) else {
                return;
            };

            if response.correlation_id == correlation_id {
                end = Some(response);
            }
        });

        // **A terminal answer ends the request, whatever its code.** A listing's
        // descriptors are sent before the answer that ends it, so an answer in
        // hand means nothing more is coming: waiting for the rest of a page that
        // was answered short would only make a wrong answer slow.
        //
        // **The code used to be tested here** (`== ControlResponseCode::ERROR`)
        // and that was a hole. A listing the archive answers out of an empty
        // catalog ends with `RECORDING_UNKNOWN`, which is not `ERROR`
        // (`control_response_code.rs`: `1` against `2`), so the wait went on to
        // its deadline with the answer already in hand — and `ask`, which is the
        // caller that knows to ask again, never got control back to do it.
        if end.is_some() {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    (page, end)
}

/// Record `channel`: publish on it, ask the archive to record it, and wait for
/// its row to reach the catalog.
///
/// `expected` is how many recordings the catalog should hold once this one is
/// there, which is the wait — the row is written when the archive's spy
/// subscription forms its image, and that is a turn of the archive's own.
fn record(session: &mut Session, client: &mut Client, channel: &str, expected: usize) -> i32 {
    let publication = client
        .add_exclusive_publication(channel, STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept the publication");
    let reader = client
        .add_subscription(channel, STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept a reader");

    let publisher_session = client
        .exclusive_publication(publication)
        .map(|publication| publication.session_id())
        .expect("the publication has a session");

    let control_session_id = session.control_session_id();

    // `StartRecordingRequest2` (63), which is `Archive::start_recording` now —
    // the same request this file used to build byte by byte, with its
    // correlation id drawn from the driver's command ring and a refusal turned
    // into an error carrying the archive's own text. `LOCAL` is the source
    // location and `false` the auto-stop, which is what the constructor wrote.
    //
    // It is the only request site in this file that moves: see the module doc
    // on why the listings stay on the channel.
    session
        .archive_mut()
        .start_recording(client, channel, STREAM_ID, true, false)
        .expect("the archive answers a start");

    publish_and_read(client, publication, reader);

    let (page, _) = ask(session, client, expected, true, move |correlation_id| {
        list_recordings_request(control_session_id, correlation_id, i64::MIN, 10)
    });

    assert_eq!(
        expected,
        page.len(),
        "the recording of {channel} did not reach the catalog"
    );

    publisher_session
}

/// Publish every message and read them back, which is what keeps the driver's
/// window moving.
fn publish_and_read(client: &mut Client, publication: i64, reader: i64) {
    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0;

    while offered < MESSAGE_COUNT {
        let message = format!("message {offered}");

        match client.offer_exclusive(publication, message.as_bytes()) {
            Some(Appended::Ok { .. }) => {
                offered += 1;
                read_whatever_there_is(client, reader);
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
}

/// Read whatever the reader has.
fn read_whatever_there_is(client: &mut Client, reader: i64) {
    let Some(image) = client.subscription(reader).and_then(|subscription| {
        subscription
            .images()
            .first()
            .map(deepmsg_client::image::Image::registration_id)
    }) else {
        return;
    };

    client.poll_image(reader, image, 10, |_| {});
}

/// Decode a `RecordingDescriptor` for `correlation_id`, or `None` for anything
/// else on that channel.
fn decode_descriptor(payload: &[u8], correlation_id: i64) -> Option<Descriptor> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    if header.template_id() != recording_descriptor_codec::SBE_TEMPLATE_ID {
        return None;
    }

    let decoder = RecordingDescriptorDecoder::default().header(header, 0);

    if decoder.correlation_id() != correlation_id {
        return None;
    }

    Some(Descriptor {
        recording_id: decoder.recording_id(),
        session_id: decoder.session_id(),
    })
}

/// The `ListRecordingsRequest` (8): a page of recordings from an id.
fn list_recordings_request(
    control_session_id: i64,
    correlation_id: i64,
    from_recording_id: i64,
    count: i32,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 64];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            ListRecordingsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .from_recording_id(from_recording_id)
            .record_count(count);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// The `ListRecordingsForUriRequest` (9): the same page for one stream on one
/// channel.
fn list_recordings_for_uri_request(
    control_session_id: i64,
    correlation_id: i64,
    from_recording_id: i64,
    count: i32,
    stream_id: i32,
    channel_fragment: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 128];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            ListRecordingsForUriRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .from_recording_id(from_recording_id)
            .record_count(count)
            .stream_id(stream_id)
            .channel(channel_fragment.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}
