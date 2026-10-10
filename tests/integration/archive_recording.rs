//! P2-S2's acceptance: a recording, made by our archive, end to end.
//!
//! The C suite is this slice's judgement and it needs the reference checkout to
//! run. This is the same claim made from inside the workspace, and it is the one
//! CI actually executes: start our archiving media driver, ask it to record a
//! channel, publish, and watch the recording follow.
//!
//! # What it watches, and why each thing
//!
//! The chain a recording goes down is long — a subscription, an image, a catalog
//! row, a position counter, a session that reads the image a block at a time,
//! and a stop that writes where it got to — and every step of it is a different
//! turn of the archive's loop. So what is asserted is the *observable* end of
//! each:
//!
//! * **The position counter**, which is the one the reference's C harness waits
//!   on (`aeron_archive_test.cpp:267-273` polls the counter's **slot**), found
//!   the way that harness finds it: by **session id** (`:275-286`), which is the
//!   question a client with a publication can ask.
//! * **`RecordingPositionRequest`** (12) answers with the live session's counter
//!   and **`StopPositionRequest`** (15) with the catalog's row — the two are
//!   different answers while a recording runs and the same one after it stops.
//! * **`ListRecordingRequest`** (10) is answered by a *listing session* rather
//!   than a reply, so passing means the archive offered a descriptor on a turn
//!   of its own and the client took it.
//! * And the counter **goes away** with the session that owned it, which is what
//!   a reader that held the id across the stop must not be able to see a stale
//!   version of (`RecordingPos.isActive`, `RecordingPos.java:292-298`).
//!
//! # The channel is the C suite's shape
//!
//! A `LOCAL` UDP publication is recorded through a **spy** subscription
//! (`ArchiveConductor.java:559-560`), so the archive subscribes to `aeron-spy:`
//! plus the stripped channel and the driver pairs the two. That is the path the
//! acceptance exercises, and the reason this test records a UDP channel rather
//! than IPC — where no spy prefix appears at all.
//!
//! The publication's own reader is here for the other half of the same
//! mechanism: a UDP publication nobody reads is one the driver stops sending on,
//! and the C suite's tests add one for the same reason.

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_archive::server::conductor::{ARCHIVE_ID_DEFAULT, NULL_POSITION};
use deepmsg_archive::server::recording_pos::{find_counter_id_by_session, parse_key};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::boolean_type::BooleanType;
use deepmsg_codec::archive::list_recording_request_codec::ListRecordingRequestEncoder;
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::recording_descriptor_codec::{self, RecordingDescriptorDecoder};
use deepmsg_codec::archive::recording_position_request_codec::RecordingPositionRequestEncoder;
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_codec::archive::start_recording_request_2_codec::StartRecordingRequest2Encoder;
use deepmsg_codec::archive::stop_position_request_codec::StopPositionRequestEncoder;
use deepmsg_codec::archive::stop_recording_subscription_request_codec::StopRecordingSubscriptionRequestEncoder;
use deepmsg_codec::archive::{ReadBuf, WriteBuf};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The connect's correlation id, which its answer echoes.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0002;

/// The archive's id, which its rec-pos counters are keyed by. The archive is
/// started without `aeron.archive.id`, so it is the property's default — and the
/// key a reader has to know to find a counter (`RecordingPos.java:75-78`).
const ARCHIVE_ID: i64 = ARCHIVE_ID_DEFAULT;

/// The channel recorded, and the stream.
///
/// A port of its own rather than the C suite's 3333: those tests are run by hand
/// and these by `cargo test`, and two drivers on one host must not pick the same
/// one.
const RECORDING_CHANNEL: &str = "aeron:udp?endpoint=localhost:33433";
/// See [`RECORDING_CHANNEL`].
const RECORDING_STREAM_ID: i32 = 33;

/// How many messages the recording is asked to carry: enough to be more than one
/// frame, so the archive has to read the image on more than one turn.
const MESSAGE_COUNT: usize = 64;

/// How long a command to the driver may take.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// The archive's properties, plus the one this test's channel needs.
///
/// `spies.simulate.connection` is what makes a **spy** reader count as a
/// subscriber, which is the shape `ArchiveConductor.java:562-565` creates; the C
/// harness sets it for the same reason (`TestArchive.h:53`).
const PROPERTIES: [&str; 3] = [
    archive::PROPERTIES[0],
    archive::PROPERTIES[1],
    "-Daeron.spies.simulate.connection=true",
];

#[test]
fn a_recording_is_made_and_stops_where_the_publication_did() {
    let archive_dir = TempDir::new("archive-recording-archive");

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("archive-recording", archive_dir.path(), &PROPERTIES)
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

    // The publication whose bytes are to be recorded, and a reader for it: a UDP
    // publication with no subscriber is one the driver does not send on.
    let publication = client
        .add_exclusive_publication(RECORDING_CHANNEL, RECORDING_STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept the publication");
    let reader = client
        .add_subscription(RECORDING_CHANNEL, RECORDING_STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept a reader");

    let publisher_session = client
        .exclusive_publication(publication)
        .map(|publication| publication.session_id())
        .expect("the publication has a session");

    // `StartRecordingRequest2` (63). Its answer is the **subscription's**
    // registration id, and it comes before any image exists: the two are
    // different moments (`ArchiveConductor.java:570` against `:2046-2051`).
    let correlation_id = session.next_correlation_id();
    let payload = start_recording_request(session.control_session_id(), correlation_id);

    // The answer *is* the `relevantId` now, and a refusal is an error carrying
    // the archive's own text — so the assertion that it was an `OK` and the
    // assertion that the id is a registration id are one statement and one
    // `expect`.
    let subscription_id = session
        .send(
            &mut client,
            correlation_id,
            &payload,
            Instant::now() + DEADLINE,
        )
        .expect("the archive answers a start");

    assert!(
        subscription_id > 0,
        "the answer carries the subscription's registration id, which is {subscription_id}"
    );

    // Publish, and read it back so the driver's window keeps moving.
    let position = publish_and_read(&mut client, publication, reader);

    // The recording follows the publication. This is the whole of the slice.
    let recording_id = wait_for_the_counter(&mut client, &aeron_dir, publisher_session, position);

    // The two questions about a recording in flight: one is the live counter,
    // the other a catalog row that has not stopped.
    assert_eq!(
        position,
        recording_position(&mut session, &mut client, recording_id),
        "a recording in flight reports where it has got to"
    );
    assert_eq!(
        NULL_POSITION,
        stop_position(&mut session, &mut client, recording_id),
        "and its catalog row says it has not stopped"
    );

    // Stop it (14), and wait for the stop to reach the catalog: the session
    // reading the image ends, and what it got to is written down
    // (`ArchiveConductor.java:1329-1363`).
    let correlation_id = session.next_correlation_id();
    let payload = stop_recording_request(
        session.control_session_id(),
        correlation_id,
        subscription_id,
    );

    session
        .send(
            &mut client,
            correlation_id,
            &payload,
            Instant::now() + DEADLINE,
        )
        .expect("the archive answers a stop");

    let stopped = wait_for_stop_position(&mut session, &mut client, recording_id);
    assert_eq!(
        position, stopped,
        "a recording stops where its publication did"
    );

    // And the descriptor the catalog hands back, offered by a listing session of
    // the archive's own (`ListRecordingByIdSession`).
    let descriptor = list_recording(&mut session, &mut client, recording_id);
    assert_eq!(recording_id, descriptor.recording_id);
    assert_eq!(RECORDING_STREAM_ID, descriptor.stream_id);
    assert_eq!(publisher_session, descriptor.session_id);
    assert_eq!(
        stopped, descriptor.stop_position,
        "the row the listing sent is the row the stop wrote"
    );

    // The counter went with the session that owned it: what a reader that held
    // the id across the stop must not see is a stale position.
    wait_for_the_counter_to_go(&mut client, &aeron_dir, publisher_session);

    let _ = media_driver.stop();
}

/// Publish every message and read them back, answering with where the
/// publication got to.
fn publish_and_read(client: &mut Client, publication: i64, reader: i64) -> i64 {
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

    let position = client
        .exclusive_publication(publication)
        .and_then(|publication| publication.position())
        .expect("the publication is still held");

    assert!(position > 0, "the publication wrote something");

    position
}

/// Read whatever the reader has, which is what keeps the publisher going.
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

/// Wait for the recording's position counter to reach `position`, answering with
/// the recording id the counter names.
///
/// The C harness's own wait (`aeron_archive_test.cpp:267-273`) with its own
/// lookup (`:275-286`): the counter's **slot** is what a client polls, and the
/// session id is how it is found. A recording that never gets there hangs the
/// reference's harness too, so the deadline here says what it was waiting for
/// rather than that something timed out.
fn wait_for_the_counter(
    client: &mut Client,
    aeron_dir: &Path,
    session_id: i32,
    position: i64,
) -> i64 {
    let deadline = Instant::now() + DEADLINE;

    while Instant::now() < deadline {
        if let Some((recording_id, value)) = recording_counter(client, session_id) {
            if value >= position {
                return recording_id;
            }
        }

        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    panic!(
        "the recording never caught up to {position};\n{}",
        archive::counters(aeron_dir)
    );
}

/// Wait for it to go away, which is the session having given its counter back.
fn wait_for_the_counter_to_go(client: &mut Client, aeron_dir: &Path, session_id: i32) {
    let deadline = Instant::now() + DEADLINE;

    while Instant::now() < deadline {
        if recording_counter(client, session_id).is_none() {
            return;
        }

        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    panic!(
        "the recording's counter outlived its session;\n{}",
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

/// Ask one recording where it has got to (12).
fn recording_position(session: &mut Session, client: &mut Client, recording_id: i64) -> i64 {
    let correlation_id = session.next_correlation_id();
    let control_session_id = session.control_session_id();

    let mut buffer = vec![0u8; 64];
    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            RecordingPositionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);

        body + encoder.encoded_length()
    };
    buffer.truncate(length);

    ok_answer(session, client, correlation_id, &buffer)
}

/// Ask where a recording stopped (15), which is `-1` while it has not.
fn stop_position(session: &mut Session, client: &mut Client, recording_id: i64) -> i64 {
    let correlation_id = session.next_correlation_id();
    let control_session_id = session.control_session_id();

    let mut buffer = vec![0u8; 64];
    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = StopPositionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);

        body + encoder.encoded_length()
    };
    buffer.truncate(length);

    ok_answer(session, client, correlation_id, &buffer)
}

/// Wait until the catalog says the recording stopped, answering with where.
fn wait_for_stop_position(session: &mut Session, client: &mut Client, recording_id: i64) -> i64 {
    let deadline = Instant::now() + DEADLINE;

    while Instant::now() < deadline {
        let stopped = stop_position(session, client, recording_id);

        if stopped >= 0 {
            return stopped;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("the recording's catalog row never got a stop position");
}

/// Send one request that is answered with an `OK`, and answer its `relevantId`.
///
/// A refusal is the error now rather than a `code` on a returned value, so
/// "answered with an `OK`" and "the call came back" are the same statement — the
/// `expect` is the assertion.
fn ok_answer(
    session: &mut Session,
    client: &mut Client,
    correlation_id: i64,
    payload: &[u8],
) -> i64 {
    session
        .send(client, correlation_id, payload, Instant::now() + DEADLINE)
        .expect("the archive answers")
}

/// One recording descriptor, as a listing session sends it.
struct Descriptor {
    recording_id: i64,
    stream_id: i32,
    session_id: i32,
    stop_position: i64,
}

/// Ask for one recording's descriptor (10) and decode the answer.
///
/// The answer is a `RecordingDescriptor` **message** rather than a
/// `ControlResponse` (`ControlResponseProxy.java:54-89`), which is why this does
/// not go through `Session::send`: what comes back for that correlation id is
/// the descriptor itself, and it is read here.
fn list_recording(session: &mut Session, client: &mut Client, recording_id: i64) -> Descriptor {
    let correlation_id = session.next_correlation_id();
    let control_session_id = session.control_session_id();

    let mut buffer = vec![0u8; 64];
    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = ListRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);

        body + encoder.encoded_length()
    };
    buffer.truncate(length);

    session
        .send_only(client, &buffer, Instant::now() + DEADLINE)
        .expect("the archive takes the request");

    archive::await_frame(
        client,
        session.response_subscription(),
        Instant::now() + DEADLINE,
        |payload| decode_descriptor(payload, correlation_id),
    )
    .expect("the archive offers the descriptor")
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
        stream_id: decoder.stream_id(),
        session_id: decoder.session_id(),
        stop_position: decoder.stop_position(),
    })
}

/// The `StartRecordingRequest2` (63) for the channel this test records.
fn start_recording_request(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
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
            .channel(RECORDING_CHANNEL.as_bytes());

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
