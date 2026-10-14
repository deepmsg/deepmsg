//! The four counters a persistent subscription publishes (114–117), allocated
//! against a real driver.
//!
//! # These are the reference's two cases
//!
//! `shouldAutoAllocateCounters` (`…_persistent_subscription_test.cpp:817`) and
//! `shouldUseUserProvidedCounters` (`:875`). Both are `system` cases in the
//! ledger (`crates/archive/tests/reference-cases.tsv`), and both are about the
//! **client** side of a persistent subscription rather than the archive's: the
//! archive is started by the reference's tests and never asked anything here.
//!
//! # What the reference asserts, and what this asserts
//!
//! The reference creates a persistent subscription and then reads the counters
//! region back — four counters of the four type ids, at their initial values.
//! The state machine that would write into them afterwards is a later commit of
//! this slice, so what is exercised here is the allocation and the initial
//! values, which is the whole of what `create` does with counters
//! (`…persistent_subscription.c:541-584`) and the whole of what both cases ask.
//!
//! Reading back is done the way the reference's own test does it: **by type id,
//! out of the CnC file**, not through the handle this module just made. A test
//! that read its own handle back would pass even if the counter were registered
//! under the wrong type, and the type id is what every reader keys on.
//!
//! Every test starts its own driver, so the counters region it reads is its own.

use std::time::Duration;

use deepmsg_archive::client::persistent_subscription::{
    Counters, Identity, JOIN_DIFFERENCE_NAME, JOIN_DIFFERENCE_TYPE_ID, LIVE_JOINED_COUNT_TYPE_ID,
    LIVE_JOINED_NAME, LIVE_LEFT_COUNT_TYPE_ID, LIVE_LEFT_NAME, NOTHING_JOINED, Provided,
    STATE_NAME, STATE_TYPE_ID,
};
use deepmsg_client::client::Client;
use deepmsg_tests::driver::{self, OwnDriver};

/// A deadline for a driver round trip, which is what every call here is.
const DEADLINE: Duration = Duration::from_secs(10);

/// The identity every label in this file carries. The channels are a shape, not
/// a subject: nothing subscribes to them.
const IDENTITY: Identity<'_> = Identity {
    replay_channel: "aeron:udp?endpoint=localhost:0|rejoin=false",
    replay_stream_id: 2000,
    live_channel: "aeron:ipc",
    live_stream_id: 1000,
};

/// What a label built from [`IDENTITY`] ends with, whichever name it starts
/// with (`"%s: %d %s %d %s"`, `…persistent_subscription.c:353-355`).
const IDENTITY_IN_LABEL: &str = ": 2000 aeron:udp?endpoint=localhost:0|rejoin=false 1000 aeron:ipc";

/// A driver and a client connected to it, or a skipped test.
fn rig(test_name: &str) -> Option<(OwnDriver, Client)> {
    let Some(mut media_driver) = OwnDriver::start(test_name) else {
        driver::announce_own_skip();
        return None;
    };

    media_driver
        .await_cnc(DEADLINE)
        .expect("our driver comes up");

    let client = Client::connect(media_driver.aeron_dir()).expect("connect to our driver");

    Some((media_driver, client))
}

/// **`shouldAutoAllocateCounters`** (`:817`): with nothing provided, the four
/// exist under the client's own type ids, carrying the identity in their
/// labels, and start at the values the reference's test asserts.
#[test]
fn the_four_counters_are_allocated_under_the_clients_type_ids() {
    let Some((_media_driver, mut client)) = rig("ps-counters-auto") else {
        return;
    };

    let counters = Counters::set_up(&mut client, &IDENTITY, Provided::default(), DEADLINE)
        .expect("the driver allocates four counters");

    let region = client.counters_reader().expect("a counters region");

    for (type_id, name) in [
        (STATE_TYPE_ID, STATE_NAME),
        (JOIN_DIFFERENCE_TYPE_ID, JOIN_DIFFERENCE_NAME),
        (LIVE_LEFT_COUNT_TYPE_ID, LIVE_LEFT_NAME),
        (LIVE_JOINED_COUNT_TYPE_ID, LIVE_JOINED_NAME),
    ] {
        let descriptor = region
            .find_by_type_id(type_id)
            .unwrap_or_else(|| panic!("no counter of type {type_id} was allocated"));

        assert!(
            descriptor.label.starts_with(name),
            "counter {type_id} is labelled {:?}, which does not name it {name}",
            descriptor.label
        );
        assert!(
            descriptor.label.ends_with(IDENTITY_IN_LABEL),
            "counter {type_id} is labelled {:?}, which does not name its two sides",
            descriptor.label
        );
    }

    // The initial values, one at a time: a state of zero, a join difference of
    // `INT64_MIN`, and two counts that have not counted anything.
    assert_eq!(Some(0), counters.state(&region));
    assert_eq!(Some(NOTHING_JOINED), counters.join_difference(&region));
    assert_eq!(Some(0), counters.live_joined(&region));
    assert_eq!(Some(0), counters.live_left(&region));
}

/// **`shouldUseUserProvidedCounters`** (`:875`): counters the caller brought
/// are the ones used — no counter of 114–117 is allocated — their join
/// difference is initialised, and **closing the subscription closes all four**,
/// which is the part of the reference that surprises (`:117-140`).
#[test]
fn counters_a_caller_brought_are_used_and_are_closed_with_the_subscription() {
    let Some((_media_driver, mut client)) = rig("ps-counters-provided") else {
        return;
    };

    // The reference's own shape: four counters of a type of the caller's
    // choosing (it uses 999) with labels of its own (it uses these four).
    let allocate = |client: &mut Client, label: &str| {
        client
            .add_counter(999, &[], label, DEADLINE)
            .unwrap_or_else(|error| panic!("the driver allocates {label}: {error}"))
    };

    let provided = Provided {
        state: Some(allocate(&mut client, "test-state")),
        join_difference: Some(allocate(&mut client, "test-join-difference")),
        live_left: Some(allocate(&mut client, "test-to-replay")),
        live_joined: Some(allocate(&mut client, "test-to-live")),
    };

    let ids = [
        provided
            .state
            .as_ref()
            .expect("just allocated")
            .counter_id(),
        provided
            .join_difference
            .as_ref()
            .expect("just allocated")
            .counter_id(),
        provided
            .live_left
            .as_ref()
            .expect("just allocated")
            .counter_id(),
        provided
            .live_joined
            .as_ref()
            .expect("just allocated")
            .counter_id(),
    ];

    let counters = Counters::set_up(&mut client, &IDENTITY, provided, DEADLINE)
        .expect("four counters, none of them allocated here");

    let region = client.counters_reader().expect("a counters region");

    for type_id in [
        STATE_TYPE_ID,
        JOIN_DIFFERENCE_TYPE_ID,
        LIVE_LEFT_COUNT_TYPE_ID,
        LIVE_JOINED_COUNT_TYPE_ID,
    ] {
        assert!(
            region.find_by_type_id(type_id).is_none(),
            "the caller's counters were provided, so none of type {type_id} may be allocated"
        );
    }

    // The caller's counters hold the values the reference's test asserts,
    // including the join difference this build writes at set-up.
    assert_eq!(Some(NOTHING_JOINED), counters.join_difference(&region));
    assert_eq!(Some(0), counters.live_joined(&region));
    assert_eq!(Some(0), counters.live_left(&region));
    assert_eq!(Some(0), counters.state(&region));

    counters
        .close(&mut client, DEADLINE)
        .expect("all four are removed, provided or not");

    let region = client.counters_reader().expect("a counters region");

    for id in ids {
        assert!(
            region.get(id).is_none(),
            "counter {id} was the caller's and the subscription left it behind"
        );
    }
}
