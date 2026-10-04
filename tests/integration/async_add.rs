//! The asynchronous adds, against a real driver.
//!
//! `async_add_subscription`, `async_add_publication` and
//! `async_add_exclusive_publication` write the command and return a handle;
//! `async_add_poll` says what became of it. What needs a driver to say anything
//! is the far end of that: that the handle the add drew is the registration id
//! the resource really has, and — for the two publication kinds — that the log
//! buffer is mapped before the poll says `Ready`, so a caller that sees `Ready`
//! gets a publication it can offer into.
//!
//! Everything here is our own client against our own driver over `aeron:ipc`,
//! so it needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here uses.
const STREAM_ID: i32 = 1001;

/// Shared memory, which is all these need: the add/answer pair is the same
/// shape on either media.
const CHANNEL: &str = "aeron:ipc";

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

fn own_driver_and_client(name: &str) -> Option<(OwnDriver, Client)> {
    let mut own = OwnDriver::start(name)?;

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    Some((own, client))
}

/// Poll until the add stops awaiting, or the deadline passes.
fn until_ready(client: &mut Client, add: AsyncAdd) -> AsyncAddPoll {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(Instant::now() < deadline, "the add never completed");
        client.poll();

        match client.async_add_poll(add) {
            AsyncAddPoll::Awaiting => std::thread::sleep(Duration::from_millis(1)),
            done => return done,
        }
    }
}

#[test]
fn an_async_publication_is_mapped_by_the_time_the_poll_says_ready() {
    let Some((_own, mut client)) = own_driver_and_client("async-add-publication") else {
        driver::announce_own_skip();
        return;
    };

    let add = client
        .async_add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the command is written");

    // A publication exists only once its log buffer does, and the response is
    // what names the file — so there is nothing to hold before the poll.
    assert!(
        client.publication(add.registration_id()).is_none(),
        "nothing is registered until the log is mapped"
    );

    assert!(matches!(until_ready(&mut client, add), AsyncAddPoll::Ready));

    // Which is the point of mapping in the poll: `Ready` means usable.
    assert!(
        client.publication(add.registration_id()).is_some(),
        "and a caller that sees Ready can offer into it"
    );
}

#[test]
fn an_async_exclusive_publication_arrives_the_same_way() {
    let Some((_own, mut client)) = own_driver_and_client("async-add-exclusive") else {
        driver::announce_own_skip();
        return;
    };

    let add = client
        .async_add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert!(matches!(until_ready(&mut client, add), AsyncAddPoll::Ready));
    assert_eq!(
        add.registration_id(),
        client
            .exclusive_publication(add.registration_id())
            .expect("the publication the handle drew")
            .registration_id(),
        "the handle's id is the resource's id"
    );
}

#[test]
fn an_async_subscription_is_ready_and_cancelling_it_gives_it_back() {
    let Some((_own, mut client)) = own_driver_and_client("async-add-cancel") else {
        driver::announce_own_skip();
        return;
    };

    let add = client
        .async_add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the command is written");

    // Registered on the way in, unlike a publication — which is what makes the
    // cancel below a *subscription* removal.
    assert!(client.subscription(add.registration_id()).is_some());
    assert!(matches!(until_ready(&mut client, add), AsyncAddPoll::Ready));

    client
        .async_add_cancel(add)
        .expect("the removal is written");

    assert!(
        client.subscription(add.registration_id()).is_none(),
        "a cancelled add stops being visible at once, without waiting for the \
         removal's own acknowledgement"
    );

    // The removal's acknowledgement arrives with nobody waiting for it, which
    // is the other half of not waiting: this client takes an unmatched answer
    // in its stride rather than mistaking it for somebody else's. The proof is
    // that it goes on working — the next add is written and answered as usual.
    let second = client
        .async_add_subscription(CHANNEL, STREAM_ID + 1, DEFAULT_TIMEOUT)
        .expect("a second subscription is written");

    assert!(
        matches!(until_ready(&mut client, second), AsyncAddPoll::Ready),
        "the client carried on after an answer it was not waiting for"
    );
}
