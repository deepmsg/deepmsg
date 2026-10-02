//! G3-1b: the port a subscription was listening on comes back when it goes.
//!
//! An endpoint is for as long as something is on it. When the last subscription
//! leaves — and no image is left behind — the reference lets it go: the socket
//! is closed, the `rcv-channel` counter's record is reclaimed, and the entry
//! goes (`aeron_receive_channel_endpoint_try_remove_endpoint`,
//! `media/aeron_receive_channel_endpoint.c:691-702`).
//!
//! What that is worth is visible from outside: **binding the same port again**.
//! Without the release the second bind fails with `Address already in use`,
//! which is what `BusySocketTest` waits for a driver to do and stop doing.
//!
//! Everything here is our own client against our own driver, so it needs no
//! reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream both ends use.
const STREAM_ID: i32 = 1001;

/// A port nothing else in this suite uses: the whole point is to bind it
/// twice, so it must be nobody else's.
const PORT: u16 = 24_912;

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

fn channel() -> String {
    format!("aeron:udp?endpoint=127.0.0.1:{PORT}")
}

/// A port is held by the socket, not by the registry: what proves the endpoint
/// was released is that **another driver** can bind it afterwards.
///
/// The same shape the reference's own test uses (`BusySocketTest`): a second
/// driver's bind fails while the first holds the port, and succeeds once the
/// subscription that held it is gone. A same-driver re-add would prove nothing
/// — it joins the endpoint that is already there.
#[test]
fn the_port_comes_back_when_the_last_subscription_goes() {
    let Some(mut holder) = OwnDriver::start("endpoint-holder") else {
        driver::announce_own_skip();
        return;
    };
    let Some(mut waiter) = OwnDriver::start("endpoint-waiter") else {
        driver::announce_own_skip();
        return;
    };

    holder
        .await_cnc(Duration::from_secs(10))
        .expect("the first driver publishes its CnC file");
    waiter
        .await_cnc(Duration::from_secs(10))
        .expect("the second driver publishes its CnC file");

    let mut holder_client = Client::connect(holder.aeron_dir()).expect("connect to the first");
    let mut waiter_client = Client::connect(waiter.aeron_dir()).expect("connect to the second");

    let subscription = holder_client
        .add_subscription(&channel(), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the first driver takes the port");

    // While it holds it, nobody else can have it.
    assert!(
        waiter_client
            .add_subscription(&channel(), STREAM_ID, DEFAULT_TIMEOUT)
            .is_err(),
        "a port two drivers could share is not a port"
    );

    holder_client
        .remove_subscription(subscription, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    // And once the subscription is gone the endpoint goes with it, which is
    // what gives the port back.
    let start = Instant::now();
    let mut taken = false;

    while start.elapsed() < DEADLINE {
        let _ = waiter_client.poll();

        if waiter_client
            .add_subscription(&channel(), STREAM_ID, DEFAULT_TIMEOUT)
            .is_ok()
        {
            taken = true;
            break;
        }

        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(taken, "the port never came back");
}
