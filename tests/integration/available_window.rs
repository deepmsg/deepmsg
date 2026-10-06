//! How much room a publication says it has.
//!
//! `Publication.availableWindow` (`Publication.java:409-413`) is `limit -
//! position`, where the limit is a counter the **driver** maintains from what
//! the slowest subscriber has read. It is what a producer asks before it
//! offers, and what the reference's archive asks in a loop before it writes a
//! replay (`ReplaySession.java:385`) or a replication (`ReplicationSession.java:651`).
//!
//! What a unit test cannot say is that the number means anything, so both
//! directions are asserted here against a real driver: an idle subscriber lets
//! the window open, a subscriber that stops reading closes it until an offer is
//! refused, and reading again opens it. The last is the half that fails if the
//! position is read from a stale place.
//!
//! Everything here is our own client against our own driver over `aeron:ipc`,
//! so it needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_core::logbuffer::position::align_up;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// Shared memory with a term small enough that the window closes: the default
/// IPC term is orders of magnitude larger, and a publication that never met its
/// limit would never report one.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

fn own_driver_and_client(name: &str) -> Option<(OwnDriver, Client)> {
    let mut own = OwnDriver::start(name)?;

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    Some((own, client))
}

/// Poll until the subscription holds an image, which is what puts a subscriber's
/// position behind the publication's limit.
fn await_image(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + DEADLINE;

    while client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .is_empty()
    {
        assert!(Instant::now() < deadline, "no image arrived");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The same payload every time: the test is about how many fit, not what they
/// say.
const PAYLOAD: [u8; 128] = [0x5A; 128];

/// Poll until the publication reports room.
///
/// The limit is the **driver's** counter and it is written on its own duty
/// cycle, so a subscriber that has just arrived has not moved it yet — the
/// window is the last thing to catch up, not the first.
fn await_window(client: &mut Client, publication: i64) -> i64 {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(Instant::now() < deadline, "the window never opened");

        if let Some(window) = client.available_window(publication) {
            if window > 0 {
                return window;
            }
        }

        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The window opens and closes with the reader, and the offer that is refused is
/// the one that says so.
///
/// The two are asserted **together** because either alone has another
/// explanation: a window that never closes could be a constant, and an offer
/// that is refused could be a full term. What ties them is that the same number
/// the caller asks first is the one that decided the refusal.
#[test]
fn the_window_closes_when_the_reader_stops_and_opens_when_it_reads() {
    let Some((_own, mut client)) = own_driver_and_client("window-shared") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    let open = await_window(&mut client, publication);

    assert!(open > 0, "a subscriber that is reading leaves room: {open}");

    // Fill it, with nobody reading. The window is what says when to stop, which
    // is how a producer is meant to use it.
    let deadline = Instant::now() + DEADLINE;
    let mut written = 0;

    while Instant::now() < deadline {
        client.poll();

        match client.offer(publication, &PAYLOAD) {
            Some(Appended::Ok { .. }) => written += 1,
            Some(Appended::BackPressured) => break,
            other => panic!("offer {written} came back {other:?}"),
        }
    }

    assert!(written > 0, "the first offer was refused: {written}");

    let closed = client
        .available_window(publication)
        .expect("the publication this client holds");

    assert!(
        closed <= 0,
        "a reader that is not reading closes the window: {closed} after {written} frames"
    );

    // And reading is what opens it again — which is the direction a window
    // computed once, or computed from the wrong position, gets wrong.
    let deadline = Instant::now() + DEADLINE;
    let mut read = 0;

    loop {
        assert!(
            Instant::now() < deadline,
            "the reader is stuck at {read} of {written}"
        );

        client.poll();
        read += client.poll_subscription(subscription, 10, |_| {});

        if read == written {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    let reopened = client
        .available_window(publication)
        .expect("the publication this client holds");

    assert!(
        reopened > 0,
        "the reader caught up, so there is room again: {reopened}"
    );
}

/// A publication with one producer answers the same question, and the number it
/// answers with is its own offset rather than the log's tail — which is the one
/// place the two kinds compute this differently.
#[test]
fn an_exclusive_publication_reports_a_window_too() {
    let Some((_own, mut client)) = own_driver_and_client("window-exclusive") else {
        driver::announce_own_skip();
        return;
    };

    let registration_id = client
        .add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an exclusive publication");

    // Nobody is subscribed, so there is no room at all: the limit is set from
    // what the subscribers have read, and with none working the reference sets
    // it to the consumer position (`aeron_ipc_publication.c:309-320`). A
    // publication nobody is reading is not one to write into.
    assert_eq!(
        Some(0),
        client.available_window(registration_id),
        "an exclusive publication with no subscriber"
    );

    // Then one arrives, and the window is the term's worth it may run ahead by.
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    let open = await_window(&mut client, registration_id);
    let frame_length = i64::from(align_up(
        i32::try_from(DATA_HEADER_LENGTH + PAYLOAD.len()).expect("small"),
        FRAME_ALIGNMENT,
    ));

    assert!(
        matches!(
            client.offer_exclusive(registration_id, &PAYLOAD),
            Some(Appended::Ok { .. })
        ),
        "and a frame fits in it"
    );

    assert_eq!(
        Some(open - frame_length),
        client.available_window(registration_id),
        "one frame of window was spent"
    );
}

/// A publication this client does not hold has no window, which is where the
/// reference's `CLOSED` (-1) lands — and giving one back is the same answer,
/// because what is gone is gone.
#[test]
fn a_publication_that_is_not_here_has_no_window() {
    let Some((_own, mut client)) = own_driver_and_client("window-absent") else {
        driver::announce_own_skip();
        return;
    };

    assert_eq!(None, client.available_window(i64::MAX));

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");

    assert!(client.available_window(publication).is_some());

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert_eq!(
        None,
        client.available_window(publication),
        "a publication this client has given back is not one it can ask about"
    );
}
