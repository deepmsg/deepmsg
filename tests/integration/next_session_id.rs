//! Asking the driver what session id to publish under, against a real driver.
//!
//! The command exists for one reason: a client about to create a publication
//! wants a session id that no publication the driver *already holds* is using
//! on that stream. Everything else about it — that the answer is a hint, that
//! the cursor moves, that two callers get different ids — follows from how it
//! is implemented rather than being something the command promises.
//!
//! That one reason is what the test arranges. The cursor lives in the driver's
//! own memory and no client can read it, so a clash is not something to wait
//! for: it is arranged by *using an answer*. Ask once to learn where the cursor
//! is, publish under the id it is about to move to, and ask again — the second
//! answer has to step over the publication the first one built.

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_tests::driver::{self, OwnDriver};

const STREAM_ID: i32 = 1001;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn the_driver_never_answers_with_a_session_id_a_publication_already_holds() {
    let Some(mut own) = OwnDriver::start("next-session-id") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    // Where the cursor is: nothing publishes on this stream yet, so the answer
    // is the cursor itself and the ask moves it one on.
    let first = client
        .next_session_id(STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver answers");

    // A publication sitting exactly where the cursor now is. Naming a session
    // is not an allocation — the driver advances only on the ids it speculates
    // (`aeron_driver_conductor.c:1071-1075`) — so this does not move the cursor
    // off the id it just claimed.
    client
        .add_publication(
            &format!("aeron:ipc?session-id={}", first.wrapping_add(1)),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("a publication on the session id the cursor moved to");

    let second = client
        .next_session_id(STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver answers again");

    assert_ne!(
        first.wrapping_add(1),
        second,
        "the session id a publication on this stream already holds"
    );
    assert_eq!(
        first.wrapping_add(2),
        second,
        "the id in the way is skipped, and the one past it is the answer"
    );

    // A second ask does not repeat itself: the cursor moved past what it handed
    // out. The reference's handler advances *before* it checks for a clash
    // (`aeron_driver_conductor.c:6425-6426`), so two clients asking in a row
    // never get the same id.
    let third = client
        .next_session_id(STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver answers a third time");

    assert_eq!(second.wrapping_add(1), third);

    // And a session id is only unique **per stream**, so another stream is
    // answered from the cursor with nothing in the way.
    let elsewhere = client
        .next_session_id(STREAM_ID + 1, DEFAULT_TIMEOUT)
        .expect("the driver answers on another stream");

    assert_eq!(third.wrapping_add(1), elsewhere);
}
