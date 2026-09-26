//! The command/response loop against a live driver.
//!
//! Two things are being proven here, and the second is the one nothing else
//! would notice:
//!
//! 1. A subscription can be added and its ready response matched by
//!    correlation id.
//! 2. A client that keeps polling **stays alive**. The driver allocates a
//!    heartbeat counter for a new `client_id` and destroys everything it owns
//!    once that counter goes stale — so without the keepalive this client would
//!    work for ten seconds and then be reaped, and every test that ran quickly
//!    enough would still pass.
//!
//! The evidence for both is `ON_CLIENT_TIMEOUT`, which the driver broadcasts
//! when it reaps a client. It is a *positive observation* in the negative
//! control and its absence is the claim in the liveness test, so the pair
//! together says something neither says alone.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::command::{Response, decode_response};
use deepmsg_cnc::counters::CLIENT_HEARTBEAT_TYPE_ID;
use deepmsg_cnc::{CncFile, Received, ToClientsReceiver};
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};

/// A driver that reaps clients quickly, so the test does not have to wait out
/// the ten-second default.
const SHORT_LIVENESS: &str = "-Daeron.client.liveness.timeout=2s";

fn start(test_name: &str, extra: &[&str]) -> Option<(ReferenceDriver, CncFile)> {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return None;
    };

    let mut reference =
        ReferenceDriver::start_with(&binary, test_name, extra).expect("start driver");
    let cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");

    Some((reference, cnc))
}

/// Watch the to-clients ring for a timeout naming `client_id`.
///
/// Returns true if one arrives within `within`. A fresh receiver starts at the
/// live edge, so it sees only what is published after it attaches — which is
/// what makes this a test of the future rather than of history.
fn wait_for_timeout(reference: &ReferenceDriver, client_id: i64, within: Duration) -> bool {
    let Ok(cnc) = CncFile::try_open(reference.aeron_dir()) else {
        return false;
    };
    let Some(region) = cnc.to_clients_region() else {
        return false;
    };
    let Some(mut receiver) = ToClientsReceiver::new(&region) else {
        return false;
    };

    let deadline = Instant::now() + within;
    loop {
        if let Received::Message { type_id } = receiver.receive(&region) {
            if let Response::ClientTimeout { client_id: seen } =
                decode_response(type_id, receiver.message())
            {
                if seen == client_id {
                    return true;
                }
            }
        }

        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn adds_a_subscription_and_matches_its_ready_response() {
    let Some((reference, _cnc)) = start("client-subscribe", &[]) else {
        return;
    };

    let mut client = Client::connect(reference.aeron_dir()).expect("connect");

    let subscription_id = client
        .add_subscription("aeron:ipc", 1001, DEFAULT_TIMEOUT)
        .expect("the driver must confirm the subscription");

    // The subscription id is the correlation id the request used, and it is
    // what every later command will refer to this subscription by. Its *value*
    // is not a contract — it comes from the ring's shared counter, whose
    // starting point depends on how many ids the driver itself has drawn — so
    // the assertion is only that a positive id came back at all. Reaching this
    // line at all is the real claim: `add_subscription` returns `Ok` only when
    // a response carrying this request's correlation id arrived and matched.
    assert!(subscription_id > 0, "got subscription id {subscription_id}");

    // The driver's first response to a new client is its own heartbeat
    // counter, with the client id in the correlation field. That it was taken
    // as *not* a reply to the subscription is the whole point of matching on
    // the correlation id rather than on arrival order.
    assert_eq!(
        0,
        client.unknown_responses(),
        "the heartbeat response should be recognised, not counted as unknown"
    );
}

#[test]
fn keeps_a_client_alive_across_the_liveness_timeout() {
    let Some((reference, cnc)) = start("client-keepalive", &[SHORT_LIVENESS]) else {
        return;
    };

    let mut client = Client::connect(reference.aeron_dir()).expect("connect");
    let client_id = client.client_id();
    let _ = client
        .add_subscription("aeron:ipc", 1002, DEFAULT_TIMEOUT)
        .expect("subscribe");

    let Some(region) = cnc.to_clients_region() else {
        return;
    };
    let Some(mut receiver) = ToClientsReceiver::new(&region) else {
        return;
    };

    // Poll past several liveness timeouts, watching the ring the whole time.
    //
    // Watching *while* polling is the point, and it is what an earlier version
    // of this test got wrong: it stopped polling and then looked for two
    // seconds, which is exactly the liveness timeout, so the act of looking
    // caused the reaping it was trying to rule out. It passed once on timing
    // and failed the next run.
    let until = Instant::now() + Duration::from_secs(4);
    let mut reaped = false;
    while Instant::now() < until {
        client.poll();

        if let Received::Message { type_id } = receiver.receive(&region) {
            if let Response::ClientTimeout { client_id: seen } =
                decode_response(type_id, receiver.message())
            {
                if seen == client_id {
                    reaped = true;
                }
            }
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        !reaped,
        "the driver reaped a client that was polling throughout"
    );

    // And the mechanism, so a failure points at the right thing: the heartbeat
    // counter exists and was refreshed.
    let counters = cnc.counters().expect("counters are readable");
    let heartbeat = counters
        .find_by_type_and_registration(CLIENT_HEARTBEAT_TYPE_ID, client_id)
        .expect("the driver allocated a heartbeat counter for us");
    let value = counters.value(heartbeat).expect("readable");
    let now = now_ms();
    assert!(
        (0..2_000).contains(&(now - value)),
        "the heartbeat should have been refreshed within the last two seconds, \
         but reads {value} against {now}"
    );
}

#[test]
fn reaps_a_client_that_stops_polling() {
    // The negative control, and the reason the test above means anything: if
    // the driver ignored the liveness timeout entirely, "no timeout arrived"
    // would be true for a reason that has nothing to do with the keepalive.
    let Some((reference, _cnc)) = start("client-reaped", &[SHORT_LIVENESS]) else {
        return;
    };

    let mut client = Client::connect(reference.aeron_dir()).expect("connect");
    let client_id = client.client_id();
    let _ = client
        .add_subscription("aeron:ipc", 1003, DEFAULT_TIMEOUT)
        .expect("subscribe");

    // Stop entirely. The subscription, the heartbeat counter and the client
    // record all go with it.
    drop(client);

    assert!(
        wait_for_timeout(&reference, client_id, Duration::from_secs(10)),
        "the driver should reap a client that stopped polling"
    );
}

fn now_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch");

    #[allow(clippy::cast_possible_truncation)]
    let millis = elapsed.as_millis() as i64;
    millis
}
