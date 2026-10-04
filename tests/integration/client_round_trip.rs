//! The client's command/response loop, without a driver.
//!
//! The interop suite proves this against a real `aeronmd` and is the stronger
//! evidence, but it cannot run in CI — so the matching loop and the deadline
//! are exercised here against replies the test fabricates byte for byte. That
//! is the same technique the reference's own client test uses
//! (`aeron-client/src/test/c/aeron_client_conductor_test.cpp:338-355`).
//!
//! What is *not* here: the encoding and decoding of a message. Those are pure
//! functions with their own tests in `deepmsg-cnc`; what needs a client is the
//! part that decides whether an arriving response is the one it was waiting
//! for, and what happens when none arrives.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, CommandError};
use deepmsg_cnc::command::{
    ADD_DESTINATION_TYPE_ID, ADD_RECEIVE_DESTINATION_TYPE_ID, ON_ERROR_TYPE_ID,
    ON_OPERATION_SUCCEEDED_TYPE_ID, ON_SUBSCRIPTION_READY_TYPE_ID,
    REMOVE_DESTINATION_BY_ID_TYPE_ID, REMOVE_DESTINATION_TYPE_ID,
    REMOVE_RECEIVE_DESTINATION_TYPE_ID, decode_destination_by_id_command,
    decode_destination_command,
};
use deepmsg_cnc::layout;
use deepmsg_cnc::{CncFile, ToDriverRingConsumer};
use deepmsg_core::pal::MappedFile;
use deepmsg_tests::synthetic::{self, EVENT_CAPACITY, SyntheticCnc};

/// The correlation id the client's **first** command after connecting will use.
///
/// Read from the ring's counter rather than assumed: connecting draws one id
/// and the command draws the next, so the command's id is one past whatever
/// the counter holds now. Reading it means a change in how many ids the client
/// draws fails *here*, loudly, instead of producing a reply that matches
/// nothing and a confusing timeout somewhere else.
///
/// The counter is handed out by `fetch_add`, which returns what it replaced, so
/// ids are consecutive: a test that drives `n` commands expects this many,
/// this + 1, and so on.
fn next_correlation_id(cnc: &Path) -> i64 {
    let mapping = MappedFile::open_readonly(cnc).expect("map the CnC file");
    let offsets = synthetic::offsets();
    let field =
        offsets.to_driver + synthetic::COMMAND_CAPACITY + layout::MPSC_CORRELATION_COUNTER_OFFSET;

    let region = mapping.region(field, 8).expect("the counter is in range");
    region.load_i64_acquire(0).expect("readable") + 1
}

/// Write one fabricated reply into the to-clients ring.
fn publish(cnc: &Path, type_id: i32, payload: &[u8]) {
    let mapping = MappedFile::open_readwrite(cnc).expect("map the CnC file writable");
    let offsets = synthetic::offsets();
    let region = mapping
        .region_mut(offsets.to_clients, synthetic::TO_CLIENTS)
        .expect("the to-clients region is writable");

    // One reply per test, so the writer's cursor starts at zero. The receiver
    // starts at `latest_counter`, which is zero in a fresh file too, so it
    // lands on this record either way.
    let mut next = 0i64;
    synthetic::publish_broadcast(&region, EVENT_CAPACITY, &mut next, type_id, payload)
        .expect("the reply fits the ring");
}

/// A ring writer that keeps its cursor, for a test that has to answer more than
/// one command.
///
/// Writing the replies up front does not work, and the reason is worth stating:
/// a reader starts at `latest_counter`, which names the **newest** record, so a
/// receiver born after five records were written would see only the fifth
/// (`crates/cnc/src/broadcast.rs:35`). Each reply is therefore written after the
/// client is listening and before the command that waits for it — and the
/// cursor has to be carried across the writes, because a ring is what this is:
/// a second reply written at zero would land on the first.
struct Replies {
    cnc: PathBuf,
    next: i64,
}

impl Replies {
    fn new(cnc: &Path) -> Self {
        Self {
            cnc: cnc.to_owned(),
            next: 0,
        }
    }

    /// Answer the command that asked under `correlation_id` with an
    /// `ON_OPERATION_SUCCEEDED`, which is the whole of what a destination
    /// command is ever answered with (`aeron_control_protocol.h:117-121`).
    fn success(&mut self, correlation_id: i64) {
        self.write(
            ON_OPERATION_SUCCEEDED_TYPE_ID,
            &correlation_id.to_le_bytes(),
        );
    }

    fn write(&mut self, type_id: i32, payload: &[u8]) {
        let mapping = MappedFile::open_readwrite(&self.cnc).expect("map the CnC file writable");
        let offsets = synthetic::offsets();
        let region = mapping
            .region_mut(offsets.to_clients, synthetic::TO_CLIENTS)
            .expect("the to-clients region is writable");

        synthetic::publish_broadcast(&region, EVENT_CAPACITY, &mut self.next, type_id, payload)
            .expect("the reply fits the ring");
    }
}

fn ready_payload(correlation_id: i64) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&correlation_id.to_le_bytes());
    // What an IPC subscription reports: no channel-status counter exists for
    // it, and that is not an error.
    payload.extend_from_slice(&(-1i32).to_le_bytes());
    payload
}

fn error_payload(correlation_id: i64, code: i32, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&correlation_id.to_le_bytes());
    payload.extend_from_slice(&code.to_le_bytes());
    payload.extend_from_slice(&(message.len() as i32).to_le_bytes());
    payload.extend_from_slice(message.as_bytes());
    payload
}

#[test]
fn matches_a_ready_response_by_correlation_id() {
    let cnc = live_cnc();
    let expected = next_correlation_id(&cnc.cnc_path());
    publish(
        &cnc.cnc_path(),
        ON_SUBSCRIPTION_READY_TYPE_ID,
        &ready_payload(expected),
    );

    let mut client = Client::connect(cnc.path()).expect("connect");
    let id = client
        .add_subscription("aeron:ipc", 1001, Duration::from_secs(1))
        .expect("the fabricated reply must match");

    assert_eq!(expected, id, "the id is the correlation id, echoed");
}

/// The channel-status counter the driver names arrives **after** the
/// subscription is registered, so it is written onto the subscription rather
/// than carried back through the caller.
///
/// That order is the reference's on both sides: Java puts the subscription into
/// its map and only then awaits the response (`ClientConductor.java:749-750`),
/// writing the counter id on it when the response arrives (`:396`), and C
/// creates the subscription inside the ready handler itself
/// (`aeron_client_conductor.c:625-652`). It is what closes the gap in which an
/// image for this subscription could arrive before the subscription existed.
#[test]
fn a_subscription_is_registered_before_its_channel_status_counter_is_known() {
    let cnc = live_cnc();
    let expected = next_correlation_id(&cnc.cnc_path());

    // A counter id rather than the `-1` an IPC subscription really gets,
    // because `-1` is the one value the accessor reports as "none" — this has
    // to be a number the subscription can only have got from the reply.
    let mut payload = expected.to_le_bytes().to_vec();
    payload.extend_from_slice(&77i32.to_le_bytes());
    publish(&cnc.cnc_path(), ON_SUBSCRIPTION_READY_TYPE_ID, &payload);

    let mut client = Client::connect(cnc.path()).expect("connect");
    let id = client
        .add_subscription("aeron:ipc", 1001, Duration::from_secs(1))
        .expect("the fabricated reply must match");

    assert_eq!(
        Some(77),
        client
            .subscription(id)
            .expect("the subscription is registered")
            .channel_status_indicator_id(),
        "the counter id from the reply lands on the subscription"
    );
}

#[test]
fn a_response_for_someone_else_is_ignored_not_mistaken() {
    // Every client reads the whole broadcast ring, so a reply addressed to a
    // different request is the normal case rather than an edge one.
    let cnc = live_cnc();
    let ours = next_correlation_id(&cnc.cnc_path());
    publish(
        &cnc.cnc_path(),
        ON_SUBSCRIPTION_READY_TYPE_ID,
        &ready_payload(ours + 1000),
    );

    let mut client = Client::connect(cnc.path()).expect("connect");
    let error = client
        .add_subscription("aeron:ipc", 1001, Duration::from_millis(200))
        .expect_err("a reply for another request must not complete ours");

    assert!(
        matches!(error, CommandError::TimedOut { correlation_id, .. } if correlation_id == ours),
        "got {error:?}"
    );
}

#[test]
fn surfaces_a_driver_error_against_the_command_that_caused_it() {
    let cnc = live_cnc();
    let expected = next_correlation_id(&cnc.cnc_path());
    publish(
        &cnc.cnc_path(),
        ON_ERROR_TYPE_ID,
        &error_payload(expected, -1004, "unknown subscription"),
    );

    let mut client = Client::connect(cnc.path()).expect("connect");
    let error = client
        .add_subscription("aeron:ipc", 1001, Duration::from_secs(1))
        .expect_err("the driver refused it");

    match error {
        CommandError::Driver { code, message } => {
            assert_eq!(-1004, code);
            assert_eq!("unknown subscription", message);
        }
        other => panic!("expected a driver error, got {other:?}"),
    }
}

#[test]
fn a_command_with_no_reply_expires_rather_than_hanging() {
    let cnc = live_cnc();
    let expected = next_correlation_id(&cnc.cnc_path());

    let mut client = Client::connect(cnc.path()).expect("connect");
    let started = Instant::now();
    let error = client
        .add_subscription("aeron:ipc", 1001, Duration::from_millis(150))
        .expect_err("nothing will answer");

    assert!(
        matches!(error, CommandError::TimedOut { correlation_id, .. } if correlation_id == expected),
        "got {error:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "it must actually wait for the deadline, not give up early"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "and it must not wait much longer than the deadline either"
    );
}

/// A liveness file, which every test here needs and each builds the same way.
fn live_cnc() -> SyntheticCnc {
    // A **live** driver's file: every test here drives a client, and a client
    // reads the ring's heartbeat before it does anything else — a file without
    // one is a driver that never started, and it says so rather than waiting
    // out a deadline.
    SyntheticCnc::new(deepmsg_core::version::CNC_VERSION)
        .with_heartbeat(deepmsg_core::clock::epoch_millis())
}

/// Every command a client has written into the to-driver ring, in order: the
/// type id its record carries, and its payload.
///
/// This is the driver's side of the same file, read the way a driver reads it,
/// so what comes back is what a driver would be handed rather than a
/// restatement of what the client meant to write.
fn commands_written(dir: &Path) -> Vec<(i32, Vec<u8>)> {
    let cnc = CncFile::try_open_writable(dir).expect("the file is published");
    let region = cnc.to_driver_region().expect("a writable command region");
    let mut consumer =
        ToDriverRingConsumer::new(&region.as_read_only()).expect("a readable command ring");

    let mut commands = Vec::new();
    consumer.read(&region, usize::MAX, |type_id, payload| {
        commands.push((type_id, payload.to_vec()));
    });

    commands
}

/// All five destination operations complete on the same reply, and the two that
/// answer with an id answer with the id they asked under.
///
/// There is no separate allocation for a destination in the reference: its
/// client completes a registering resource when the reply's correlation id
/// matches the one it is waiting on (`aeron_client_conductor.c:975-991`), and
/// `aeron_async_destination_get_registration_id` is documented as returning
/// "correlation_id sent to driver" (`aeronc.h:2697-2705`). So the id a caller
/// removes a destination by is the id of the command that created it, and that
/// is what this pins.
#[test]
fn every_destination_operation_completes_on_the_same_reply() {
    let cnc = live_cnc();
    let first = next_correlation_id(&cnc.cnc_path());

    let mut client = Client::connect(cnc.path()).expect("connect");
    let mut replies = Replies::new(&cnc.cnc_path());
    let within = Duration::from_secs(1);

    // Five commands draw five consecutive ids, so each reply can be written for
    // the id its command is about to draw.
    replies.success(first);
    let added = client
        .add_destination(3, "aeron:udp?endpoint=localhost:40456", within)
        .expect("the first reply matches");
    assert_eq!(
        first, added,
        "a destination's registration id is the id its command asked under"
    );

    replies.success(first + 1);
    client
        .remove_destination(3, "aeron:udp?endpoint=localhost:40456", within)
        .expect("the second reply matches");

    replies.success(first + 2);
    client
        .remove_destination_by_id(3, added, within)
        .expect("the third reply matches");

    replies.success(first + 3);
    let added_rcv = client
        .add_rcv_destination(4, "aeron:udp?endpoint=localhost:40457", within)
        .expect("the fourth reply matches");
    assert_eq!(
        first + 3,
        added_rcv,
        "a receive destination is registered the same way, and the ids are consecutive"
    );

    replies.success(first + 4);
    client
        .remove_rcv_destination(4, "aeron:udp?endpoint=localhost:40457", within)
        .expect("the fifth reply matches");

    // And the commands themselves, which the replies cannot vouch for: the
    // driver is not asked to echo a command back, so a client that wired
    // `add_destination` to the wrong type id would still be answered and still
    // look correct from here. The record's type id is the only thing that says
    // which of the five it is, so the type ids are asserted, and the payloads
    // are decoded with the decoder the driver will use — the client's bytes and
    // the driver's reading of them, agreed here without either being restated.
    let commands = commands_written(cnc.path());

    let type_ids: Vec<i32> = commands.iter().map(|(type_id, _)| *type_id).collect();
    assert_eq!(
        vec![
            ADD_DESTINATION_TYPE_ID,
            REMOVE_DESTINATION_TYPE_ID,
            REMOVE_DESTINATION_BY_ID_TYPE_ID,
            ADD_RECEIVE_DESTINATION_TYPE_ID,
            REMOVE_RECEIVE_DESTINATION_TYPE_ID,
        ],
        type_ids,
        "five commands, five records, each under its own type id"
    );

    let payload = |index: usize| commands[index].1.as_slice();

    let add = decode_destination_command(payload(0)).expect("the record a driver reads");
    assert_eq!(3, add.registration_id, "the publication it was added to");
    assert_eq!(first, add.correlation_id);
    assert_eq!(b"aeron:udp?endpoint=localhost:40456", add.channel);

    let remove = decode_destination_command(payload(1)).expect("the record a driver reads");
    assert_eq!(3, remove.registration_id);
    assert_eq!(b"aeron:udp?endpoint=localhost:40456", remove.channel);

    let by_id = decode_destination_by_id_command(payload(2)).expect("the record a driver reads");
    assert_eq!(3, by_id.resource_registration_id, "the publication");
    assert_eq!(
        first, by_id.destination_registration_id,
        "the id `add_destination` handed back is the one this removal names"
    );

    let add_rcv = decode_destination_command(payload(3)).expect("the record a driver reads");
    assert_eq!(
        4, add_rcv.registration_id,
        "the subscription, not a publication"
    );
    assert_eq!(b"aeron:udp?endpoint=localhost:40457", add_rcv.channel);

    let remove_rcv = decode_destination_command(payload(4)).expect("the record a driver reads");
    assert_eq!(4, remove_rcv.registration_id);
    assert_eq!(b"aeron:udp?endpoint=localhost:40457", remove_rcv.channel);
}

/// `REMOVE_DESTINATION_BY_ID` is the one command in the family whose failures
/// the reference answers nothing to, and the client has to survive that the way
/// the reference's own clients do: by timing out.
///
/// `aeron_driver_conductor.c:3188-3200` calls the handler without taking its
/// result, so the `-1` it returns for a publication it cannot find (`:5562-5578`)
/// never reaches the `result < 0` at `:3222-3225` that would send an `ON_ERROR`.
/// Every other destination command assigns it (`:3020`, `:3035`, `:3055-3062`,
/// `:3083-3090`). This build reproduces that rather than improving on it —
/// `docs/compat.md` carries the line — so a caller that names an unknown
/// publication waits. This pins that the wait ends on the caller's own deadline,
/// against the id it used, rather than hanging or blaming a connection.
#[test]
fn removing_a_destination_by_id_waits_for_an_answer_that_never_comes() {
    let cnc = live_cnc();
    let expected = next_correlation_id(&cnc.cnc_path());

    let mut client = Client::connect(cnc.path()).expect("connect");
    let started = Instant::now();
    let error = client
        .remove_destination_by_id(3, 42, Duration::from_millis(150))
        .expect_err("the reference would answer nothing here");

    assert!(
        matches!(error, CommandError::TimedOut { correlation_id, .. } if correlation_id == expected),
        "got {error:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "it must wait out the deadline, not give up early"
    );
}
