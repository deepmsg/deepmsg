//! G2-2: a client giving a resource back.
//!
//! Three ways to let go of a publication or a subscription, and they are not
//! the same:
//!
//! * `remove_*` asks the driver and waits for its answer — the resource is gone
//!   from both sides when the call returns;
//! * `revoke_publication` is the same command with the revoke flag, and the
//!   difference is what it does to *readers*: their images are revoked and
//!   reported unavailable with `is_publication_revoked` true — and, as the
//!   last two tests measure, **without waiting for them** to take what is in
//!   the log;
//! * `force_remove_*` tells the driver nothing, which is what a client about to
//!   die does — the driver keeps its link until the client's own liveness
//!   timeout reaps it.
//!
//! Which of the first two it will be can also be decided **before** the
//! removal: `revoke_publication_on_close` marks a publication, and the flag
//! travels out with whatever removal gives it back.
//!
//! Everything here is our own client against our own driver, so it needs no
//! reference checkout and runs in CI. What it asserts that a unit test cannot:
//! the driver's answer is what removes the link, and what the client is left
//! holding afterwards is nothing.
//!
//! The same removal can also be **sent without waiting for that answer**
//! (`Aeron.asyncRemovePublication`, `Aeron.java:350-353`), which is what the
//! last three tests are about. Neither reference has a poll for it — Java
//! returns nothing and takes the resource out of its own map on the spot
//! (`ClientConductor.java:710-722`), and C reports completion through a
//! callback (`aeron_async_remove_publication`, `aeron_client.c:480-489`) —
//! because both run a conductor thread that makes progress on its own. A
//! poll-driven client has no such thread, so `remove_publication_poll` is this
//! build's counterpart of `async_add_poll`, and the deadline the caller passes
//! is what stops a caller that stops polling from leaving an entry behind.

use std::time::{Duration, Instant};

use deepmsg_client::client::{
    AsyncAdd, AsyncAddPoll, AsyncRemove, Client, DEFAULT_TIMEOUT, RemovePoll,
};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// Shared memory: a removed subscription's image is a *network* one, but the
/// shape of a removal — the link going, the counters coming back, the
/// acknowledgement arriving — is the same, and this needs no sockets.
const CHANNEL: &str = "aeron:ipc";

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

fn await_images(client: &mut Client, registration_id: i64) {
    let start = Instant::now();

    while start.elapsed() < DEADLINE {
        let _ = client.poll();

        if client
            .subscription(registration_id)
            .is_some_and(|subscription| !subscription.images().is_empty())
        {
            return;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("no image arrived");
}

/// Our own driver and a client of ours on it, with nothing registered yet.
fn own_driver_and_client(name: &str) -> Option<(OwnDriver, Client)> {
    let mut own = OwnDriver::start(name)?;

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    Some((own, client))
}

/// Poll until the add stops awaiting, or the deadline passes.
fn until_added(client: &mut Client, add: AsyncAdd) -> AsyncAddPoll {
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

/// Poll until the subscription has no image left, which is how a reader is told
/// its stream ended rather than merely stopping.
///
/// `within` is a parameter rather than the deadline because two of the tests
/// below assert the **time** the image took: a revoked publication ends without
/// waiting for a reader that has not read, and a quiet one waits. Measured on
/// this machine, with frames nobody has taken: about a second against more than
/// twelve, so the two windows below cannot both be satisfied by one behaviour.
fn await_image_gone(client: &mut Client, registration_id: i64, within: Duration) {
    let start = Instant::now();

    while start.elapsed() < within {
        let _ = client.poll();

        if client
            .subscription(registration_id)
            .is_none_or(|subscription| subscription.images().is_empty())
        {
            return;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("the image was still there after {within:?}");
}

/// How long a **revoked** publication may take to reach its readers.
///
/// Several times the time it measurably takes, and well inside the time a quiet
/// removal takes with the same unread frames — which is what makes the
/// assertion about the mark rather than about removals in general.
const REVOKED: Duration = Duration::from_secs(4);

/// How long a quiet removal is watched for, to see that it does **not** end.
const QUIET: Duration = Duration::from_secs(3);

/// Frames nobody reads, so that an ending has something to wait for.
const UNREAD_FRAMES: usize = 16;

/// Frames offered and never taken, so that a quiet ending is one it has to wait
/// through.
///
/// Waited for rather than merely offered: the window a publication reports is
/// the driver's counter, so a subscriber that has only just attached has not
/// opened one yet — and an exclusive publication with nobody subscribed has no
/// window at all.
fn offer_unread(client: &mut Client, publication: i64) {
    let deadline = Instant::now() + DEADLINE;

    for frame in 0..UNREAD_FRAMES {
        loop {
            assert!(
                Instant::now() < deadline,
                "frame {frame} was never taken by the log"
            );
            client.poll();

            if matches!(offer_one(client, publication), Some(Appended::Ok { .. })) {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// One frame into a publication of **either** kind: two lists, and an append
/// each. Which one it was is not in the id, and does not need to be.
fn offer_one(client: &Client, publication: i64) -> Option<Appended> {
    const FRAME: [u8; 64] = [0x5A; 64];

    client
        .offer(publication, &FRAME)
        .or_else(|| client.offer_exclusive(publication, &FRAME))
}

/// Whether the subscription still has an image.
fn holds_an_image(client: &mut Client, registration_id: i64) -> bool {
    let _ = client.poll();

    client
        .subscription(registration_id)
        .is_some_and(|subscription| !subscription.images().is_empty())
}

/// Poll until the removal stops awaiting, or the deadline passes.
///
/// The handle survives being polled — it is a pair of ids — so the caller can
/// ask again once this returns.
fn until_removed(client: &mut Client, remove: AsyncRemove) -> RemovePoll {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(Instant::now() < deadline, "the removal never completed");
        client.poll();

        match client.remove_publication_poll(remove) {
            RemovePoll::Awaiting => std::thread::sleep(Duration::from_millis(1)),
            done => return done,
        }
    }
}

/// A publication and a subscription that are talking, so that a removal has
/// something to remove.
fn talking_pair(name: &str) -> Option<(OwnDriver, Client, i64, i64)> {
    let Some((own, mut client)) = own_driver_and_client(name) else {
        driver::announce_own_skip();
        return None;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_images(&mut client, subscription);

    Some((own, client, publication, subscription))
}

#[test]
fn a_subscription_and_a_publication_can_be_given_back() {
    let Some((_own, mut client, publication, subscription)) = talking_pair("lifecycle-remove")
    else {
        return;
    };

    assert!(client.publication(publication).is_some());
    assert!(client.subscription(subscription).is_some());

    // The subscription first: its image is what a receiver feeds, and the
    // acknowledgement is what makes the removal this client's too.
    client
        .remove_subscription(subscription, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(
        client.subscription(subscription).is_none(),
        "the client is not holding it any more"
    );
    assert!(
        client.subscription(subscription).is_none() && client.publication(publication).is_some(),
        "and the publication next to it is untouched"
    );

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(client.publication(publication).is_none());
}

/// A removal the driver refuses is a removal this client does not make: the
/// resource is still its own, and it is still usable.
#[test]
fn a_removal_the_driver_refuses_leaves_the_resource_alone() {
    let Some((_own, mut client, publication, _subscription)) = talking_pair("lifecycle-refused")
    else {
        return;
    };

    // A registration id nobody has: the driver answers `ON_ERROR` with the
    // unknown-publication code rather than an acknowledgement.
    let result = client.remove_publication(i64::MAX, DEFAULT_TIMEOUT);

    assert!(result.is_err(), "the driver refused it");
    assert!(
        client.publication(publication).is_some(),
        "and the publication it does own is still here"
    );
}

/// `force_remove_*` asks nobody: the client lets go, and the driver is left
/// holding a link that its own client timeout will reap.
#[test]
fn a_forced_removal_does_not_ask_the_driver() {
    let Some((_own, mut client, publication, subscription)) = talking_pair("lifecycle-force")
    else {
        return;
    };

    assert!(client.force_remove_subscription(subscription));
    assert!(client.subscription(subscription).is_none());
    assert!(
        !client.force_remove_subscription(subscription),
        "and asking twice is not a removal"
    );

    assert!(client.force_remove_publication(publication));
    assert!(client.publication(publication).is_none());

    // The driver is still there and still serving: a forced removal is a
    // client's own business.
    let _ = client.poll();
}

/// A revoked publication takes its readers' images with it
/// (`REMOVE_PUBLICATION_FLAG_REVOKE`), which is a different ending from the
/// quiet removal above — and the one `PublicationRevokeTest` measures from the
/// far side.
#[test]
fn a_revoked_publication_takes_its_readers_image_with_it() {
    let Some((_own, mut client, publication, subscription)) = talking_pair("lifecycle-revoke")
    else {
        return;
    };

    client
        .revoke_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a revoke it made");

    assert!(
        client.publication(publication).is_none(),
        "a revocation is a removal, with a message to the readers"
    );

    // The image goes with it: the driver tells the subscriber, and the client
    // drops the image when it hears. Nothing was published here, so this says
    // nothing about how long it took — the test after the next one does.
    await_image_gone(&mut client, subscription, DEADLINE);
}

/// The same ending, decided **before** the removal: marking is what a caller
/// that knows how it wants to end says, and the close it has not reached yet is
/// what carries the flag out.
///
/// The archive is that caller — `ControlSession.close` marks its control
/// publication and then closes it on the next line
/// (`ControlSession.java:168-169`), and `ResponseClient.close` does the same —
/// so the two are separate calls here for the same reason.
#[test]
fn a_publication_marked_for_revocation_takes_its_readers_image_with_it() {
    let Some((_own, mut client, publication, subscription)) =
        talking_pair("lifecycle-revoke-on-close")
    else {
        return;
    };

    assert!(
        client.revoke_publication_on_close(publication),
        "the publication this client holds"
    );
    assert!(
        !client.revoke_publication_on_close(i64::MAX),
        "and an id this client holds nothing under is not something to mark"
    );

    // Frames the reader has not taken, which is the whole of what the two
    // endings differ over.
    offer_unread(&mut client, publication);

    // A quiet removal, which is a revocation only because of the mark.
    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(client.publication(publication).is_none());
    await_image_gone(&mut client, subscription, REVOKED);
}

/// The other ending, on the same arrangement — and the assertion that makes the
/// one above mean something.
///
/// Without it, "the image went away" is true of any removal at all: that is
/// what the first version of this test asserted, and a falsification pass is
/// what showed it passing with the mark never reaching the command. What
/// separates the two endings is **unread data** — the reference's own words for
/// the revoked one are "disposes of resources as soon as possible … the image
/// will go unavailable without requiring all data to be drained"
/// (`ExclusivePublication.java:160-166`), so the quiet one is the one that
/// drains.
#[test]
fn a_quiet_removal_waits_for_the_reader_it_left_behind() {
    let Some((_own, mut client, publication, subscription)) =
        talking_pair("lifecycle-quiet-unread")
    else {
        return;
    };

    offer_unread(&mut client, publication);

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(
        client.publication(publication).is_none(),
        "the publication goes either way"
    );

    let start = Instant::now();
    while start.elapsed() < QUIET {
        assert!(
            holds_an_image(&mut client, subscription),
            "the image stays while a reader has frames it has not taken"
        );

        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The asynchronous removal carries the mark too, because the flag is added
/// where both of them build the command rather than by either of them: a
/// publication that ends loudly must not depend on which removal ended it.
#[test]
fn the_asynchronous_removal_honours_the_mark_as_well() {
    let Some((_own, mut client, publication, subscription)) =
        talking_pair("lifecycle-async-revoke-on-close")
    else {
        return;
    };

    assert!(client.revoke_publication_on_close(publication));
    offer_unread(&mut client, publication);

    let remove = client
        .async_remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert!(matches!(
        until_removed(&mut client, remove),
        RemovePoll::Ready
    ));
    assert!(client.publication(publication).is_none());

    await_image_gone(&mut client, subscription, REVOKED);
}

/// And the mark reaches a publication with one producer, which is a different
/// list: the flag is about how a stream ends, not about how many producers it
/// had.
#[test]
fn an_exclusive_publication_can_be_marked_as_well() {
    let Some((_own, mut client)) = own_driver_and_client("lifecycle-exclusive-revoke") else {
        driver::announce_own_skip();
        return;
    };

    let add = client
        .async_add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert!(matches!(until_added(&mut client, add), AsyncAddPoll::Ready));

    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_images(&mut client, subscription);

    let registration_id = add.registration_id();

    assert!(client.revoke_publication_on_close(registration_id));
    offer_unread(&mut client, registration_id);

    client
        .remove_publication(registration_id, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(client.exclusive_publication(registration_id).is_none());
    await_image_gone(&mut client, subscription, REVOKED);
}

/// The removal that does not wait: it is written, and what became of it is
/// found out later.
///
/// What the first two assertions pin is what "later" means here. Nothing has
/// read a driver answer yet, so the removal has not happened: the publication
/// is still this client's, and the poll says `Awaiting` rather than reporting a
/// removal nobody has made. What takes it out of the list is the driver's
/// answer, and `Ready` is that moment.
#[test]
fn a_removal_can_be_sent_without_waiting_for_the_answer() {
    let Some((_own, mut client, publication, _subscription)) =
        talking_pair("lifecycle-async-remove")
    else {
        return;
    };

    let remove = client
        .async_remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert_eq!(
        publication,
        remove.registration_id(),
        "the handle names the resource it is going to take out"
    );

    assert!(
        matches!(client.remove_publication_poll(remove), RemovePoll::Awaiting),
        "the command is in the ring and its answer has not been read"
    );
    assert!(
        client.publication(publication).is_some(),
        "and until the driver answers, this client still holds what the driver holds"
    );

    assert!(matches!(
        until_removed(&mut client, remove),
        RemovePoll::Ready
    ));
    assert!(
        client.publication(publication).is_none(),
        "Ready is the moment it leaves this client's list"
    );

    assert!(
        matches!(client.remove_publication_poll(remove), RemovePoll::Unknown),
        "polling consumed the answer, as it does for an add"
    );
}

/// A refusal is delivered to the poll rather than thrown at the caller, which
/// is the other half of not waiting — and it leaves the resource alone, as the
/// synchronous removal's refusal does.
#[test]
fn a_removal_the_driver_refuses_reaches_the_poll() {
    let Some((_own, mut client, publication, _subscription)) =
        talking_pair("lifecycle-async-refused")
    else {
        return;
    };

    // A registration id nobody has: the driver answers `ON_ERROR` with the
    // unknown-publication code rather than an acknowledgement.
    let remove = client
        .async_remove_publication(i64::MAX, DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert!(
        matches!(until_removed(&mut client, remove), RemovePoll::Failed(_)),
        "the driver refused it, and the refusal is what the poll answers with"
    );
    assert!(
        client.publication(publication).is_some(),
        "and the publication it does own is still here"
    );
}

/// A removal names a publication by registration id and the driver does not
/// care which kind it was, so neither does the client: the poll looks in both
/// of its lists.
///
/// Two lists is this build's doing — the reference has one map, in which an
/// `ExclusivePublication` and a `Publication` share a lookup
/// (`ClientConductor.java:680-722`) — and the poll has to know it.
#[test]
fn a_removal_finds_a_publication_of_either_kind() {
    let Some((_own, mut client)) = own_driver_and_client("lifecycle-async-exclusive") else {
        driver::announce_own_skip();
        return;
    };

    let add = client
        .async_add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert!(matches!(until_added(&mut client, add), AsyncAddPoll::Ready));

    let remove = client
        .async_remove_publication(add.registration_id(), DEFAULT_TIMEOUT)
        .expect("the command is written");

    assert!(matches!(
        until_removed(&mut client, remove),
        RemovePoll::Ready
    ));
    assert!(
        client
            .exclusive_publication(add.registration_id())
            .is_none(),
        "the exclusive list was searched too"
    );
}
