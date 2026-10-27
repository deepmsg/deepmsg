//! Two publications on one channel, which are two handles on one publication.
//!
//! The reference's wire answers a client with **two** ids
//! (`aeron_publication_buffers_ready_t`, `aeron_driver_conductor.c:2387-2388`):
//! the `correlation_id` of the `ADD_PUBLICATION` that is being answered, which
//! is the handle the client keeps and removes by, and the publication's own
//! `registration_id`, which every client on that channel shares. A client that
//! took the second as its handle would hold the *same* handle twice, and closing
//! one would close the channel under the other.
//!
//! That is not hypothetical: it is what made the reference's own
//! `NameReResolutionTest.shouldReResolveUnicastAddressWhenSendChannelEndpointIsReused`
//! fail here with `send_channel_endpoint found in CLOSING state, please retry`
//! — the second publication closed the first's endpoint, and the third
//! `ADD_PUBLICATION` found it gone.
//!
//! Everything here is our own client against our own driver, so it needs no
//! reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

const STREAM_ID: i32 = 1001;
const CHANNEL: &str = "aeron:udp?endpoint=127.0.0.1:24326";

const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn a_second_publication_on_one_channel_is_its_own_handle() {
    let Some(own) = OwnDriver::start("publication-links") else {
        driver::announce_own_skip();
        return;
    };

    let mut client = Client::connect(own.aeron_dir()).expect("a client");

    let first = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");

    let second = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a second publication on the same channel");

    assert_ne!(
        first, second,
        "each add is its own handle, though both map one log buffer"
    );

    // Closing one leaves the other publishing: the endpoint is shared, and the
    // first client's removal must not take it.
    client
        .remove_publication(first, DEFAULT_TIMEOUT)
        .expect("the first is removed");
    client
        .remove_publication(second, DEFAULT_TIMEOUT)
        .expect("and the second is still there to remove");

    // And once both are gone the channel is free again, which is what the
    // reference's own test waits for.
    let start = Instant::now();
    let mut attempts = 0;

    loop {
        attempts += 1;

        match client.add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT) {
            Ok(registration_id) => {
                client
                    .remove_publication(registration_id, DEFAULT_TIMEOUT)
                    .expect("the publication is removed");

                return;
            }
            Err(error) => {
                assert!(
                    error.to_string().contains("CLOSING"),
                    "a publication that cannot be added on a free channel: {error}"
                );
            }
        }

        assert!(
            start.elapsed() < DEADLINE,
            "the endpoint never became reusable ({attempts} attempts)"
        );

        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A publication that replaces another one on the **same endpoint** connects.
///
/// This is what the reference's `AeronCluster` does when a new leadership term
/// elects the **same** member (`AeronCluster.java:826-836`): it closes its
/// ingress publication and adds another on the leader's endpoint — the endpoint
/// the publication it just closed was sending to. The add can meet
/// `send_channel_endpoint found in CLOSING state, please retry`, which the
/// reference's own `shouldReResolveUnicastAddressWhenSendChannelEndpointIsReused`
/// is written around, and its client retries exactly that error
/// (`AeronCluster.java:762-793`). What this asks is the second half: once the
/// driver has taken the replacement, does it **connect**?
#[test]
fn a_publication_that_replaces_another_on_the_same_endpoint_connects() {
    const REPLACE_CHANNEL: &str = "aeron:udp?endpoint=127.0.0.1:24327";

    let Some(receiver) = OwnDriver::start("publication-replace-receiver") else {
        driver::announce_own_skip();
        return;
    };
    let Some(sender) = OwnDriver::start("publication-replace-sender") else {
        driver::announce_own_skip();
        return;
    };

    let mut receiving = Client::connect(receiver.aeron_dir()).expect("the receiving client");
    let subscription = receiving
        .add_subscription(REPLACE_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on the endpoint");

    let mut sending = Client::connect(sender.aeron_dir()).expect("the sending client");
    let first = sending
        .add_publication(REPLACE_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    await_connected(&sending, first);

    // One message really crosses, so the publication is a live one with a
    // position — the state a cluster session's response publication is in when
    // a new term gives it back.
    let deadline = Instant::now() + DEADLINE;
    let mut received = 0;
    while received == 0 && Instant::now() < deadline {
        let _ = sending.offer(first, b"before");
        receiving.poll();
        receiving.poll_subscription(subscription, 10, |message| {
            if message.payload == b"before" {
                received += 1;
            }
        });
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(1, received, "the message crossed before the rebuild");

    // The rebuild, exactly as a session does it: an **async** removal (the
    // driver keeps the publication while it drains — the endpoint goes with it)
    // and the add immediately after, from `ClusterSession::disconnect` and
    // `::connect`. Linking the new registration to the draining publication is
    // what left a re-elected leader unable to send its `NEW_LEADER_EVENT` at
    // all: the registration never sent a `SETUP` and never connected.
    let _ = sending.async_remove_publication(first, DEFAULT_TIMEOUT);

    // A term change takes turns, not microseconds (`prepareForNewLeadership`
    // to `connectIngress` is an election long), so the removal is **processed**
    // by the time the add arrives: the publication is draining, and the
    // endpoint it holds is still the one the add will find.
    let settle = Instant::now() + Duration::from_millis(200);
    while Instant::now() < settle {
        sending.poll();
        std::thread::sleep(Duration::from_millis(2));
    }

    let second = sending
        .add_publication(REPLACE_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the replacement is added");

    await_connected(&sending, second);
}

/// Wait until the driver says the publication has a subscriber.
fn await_connected(client: &Client, publication: i64) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if client
            .publication(publication)
            .and_then(|publication| publication.is_connected())
            == Some(true)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    panic!("the publication never connected");
}
