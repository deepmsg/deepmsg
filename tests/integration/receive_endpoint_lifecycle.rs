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
//! The same counter reads the other way round while the endpoint is alive: its
//! key is where the socket is bound, which is the only answer to "which port did
//! `:0` become" — `Subscription.resolvedEndpoint`
//! (`Subscription.java:579-586`), and what `ReplayMerge` builds a replay
//! channel from.
//!
//! Everything here is our own client against our own driver, so it needs no
//! reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::counters::{LOCAL_SOCKADDR_TYPE_ID, LocalSocketAddress};
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

/// Where a wildcard-port subscription actually bound.
///
/// `endpoint=127.0.0.1:0` asks for a port and does not name one, so the answer
/// cannot come from the channel string the client wrote — it comes back from
/// the driver, and only from the **key** of the local-address counter it
/// publishes beside the channel's status. That is the whole of the read: the
/// subscription is a name, the counters are the address.
#[test]
fn a_wildcard_port_subscription_reports_where_it_bound() {
    let Some(mut own) = OwnDriver::start("endpoint-resolved") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");
    let subscription = client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver takes a port of its choosing");

    // The loop is a safety net rather than the expected path: this driver binds
    // the socket — and publishes the address counter beside the channel's
    // status — before it answers the ready response, so a caller normally has
    // the address the moment the add returns. What is being waited for is the
    // pair of `ACTIVE`s that make it readable at all.
    let start = Instant::now();
    let mut endpoint = None;

    while start.elapsed() < DEADLINE {
        let _ = client.poll();

        endpoint = {
            let reader = client.counters_reader().expect("the counter regions");
            client
                .subscription(subscription)
                .and_then(|subscription| subscription.resolved_endpoint(&reader))
        };

        if endpoint.is_some() {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    let endpoint = endpoint.expect("the wildcard port was never resolved");

    assert!(
        endpoint.starts_with("127.0.0.1:"),
        "the address the driver bound, not the channel asked for: {endpoint}"
    );

    let port: u16 = endpoint
        .rsplit(':')
        .next()
        .expect("a port")
        .parse()
        .expect("a port number");

    assert_ne!(0, port, "the kernel chose one: {endpoint}");
}

/// A channel with no endpoint has no address to report, and says so rather than
/// inventing one: `aeron:ipc` is shared memory, so no counter is allocated for
/// it at all and there is nothing to read.
#[test]
fn an_ipc_subscription_reports_no_endpoint() {
    let Some(mut own) = OwnDriver::start("endpoint-ipc") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");
    let subscription = client
        .add_subscription("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    let _ = client.poll();

    let reader = client.counters_reader().expect("the counter regions");

    assert_eq!(
        None,
        client
            .subscription(subscription)
            .expect("the subscription")
            .resolved_endpoint(&reader),
        "an IPC subscription is not bound to anything"
    );
}

/// A port this case owns: the cases of one binary run on threads of one
/// process, and [`PORT`] belongs to the port case.
const SEND_ADDRESS_PORT: u16 = 24_913;

fn send_address_channel() -> String {
    format!("aeron:udp?endpoint=127.0.0.1:{SEND_ADDRESS_PORT}")
}

/// Every address a **type 14** counter names — `snd-local-sockaddr` and
/// `rcv-local-sockaddr` alike, because the client's lookup does not tell them
/// apart either ([`Subscription::resolved_endpoint`]).
fn bound_addresses(client: &Client) -> Vec<String> {
    let Some(counters) = client.counters_reader() else {
        return Vec::new();
    };

    let mut addresses = Vec::new();

    counters.for_each(|counter| {
        if counter.type_id != LOCAL_SOCKADDR_TYPE_ID {
            return;
        }

        let Some(key) = counters.key(counter.counter_id) else {
            return;
        };
        let Some(address) = LocalSocketAddress::decode(&key) else {
            return;
        };

        addresses.push(address.address.to_owned());
    });

    addresses
}

/// How many send endpoints the driver still holds. The `snd-channel` counter is
/// the driver's own answer to "is the endpoint gone" — and the one it *does*
/// give back today, which is what keeps the address question below from being
/// circular.
fn send_channel_counters(client: &Client) -> usize {
    let Some(counters) = client.counters_reader() else {
        return 0;
    };

    let mut count = 0;

    counters.for_each(|counter| {
        if counter.label.starts_with("snd-channel:") {
            count += 1;
        }
    });

    count
}

/// Wait for an address the driver did not have before, and return it.
fn wait_for_new_address(client: &mut Client, known: &[String], what: &str) -> String {
    let start = Instant::now();

    while start.elapsed() < DEADLINE {
        let _ = client.poll();

        if let Some(address) = bound_addresses(client)
            .into_iter()
            .find(|address| !known.contains(address))
        {
            return address;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("{what} never published an address");
}

/// **A send endpoint's address counter goes back with it.**
///
/// `snd-local-sockaddr` is a **type 14** counter, the same type
/// `rcv-local-sockaddr` is, and a subscription finds its own bound address by
/// asking for "the first type 14 counter whose key names my channel status id"
/// — `Subscription.resolvedEndpoint` (`Subscription.java:579-586`), which
/// stops at the first match exactly as the reference's
/// `LocalSocketAddressStatus.findAddress` does (`…/status/LocalSocketAddressStatus.java:189-229`).
///
/// So a send endpoint that leaves its counter behind is a counter that can
/// answer **for a subscription** whose channel status id was recycled from the
/// endpoint's. The subscription then reports a dead socket's port, and whoever
/// reads that address dials a port nobody holds. That is the open cluster
/// defect — `records/P3-6c.md` §28, plan
/// `analysis/cluster/fix-the-leaked-send-sockaddr-counter-plan.md` — and this is
/// its driver-level guard.
///
/// The assertion is deliberately **not** the symptom: the symptom needs the
/// recycled id to land on the new subscription, which depends on the free list
/// and the free-to-reuse deadline and would be a test that goes red once in
/// five. This one asks the question the driver owns: is the counter still
/// there after the endpoint that owned it is gone.
#[test]
fn a_released_send_endpoint_leaves_no_address_behind() {
    let Some(mut own) = OwnDriver::start("endpoint-send-address") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    // A reader first, so the publication's socket has somewhere to send and
    // the case does not depend on how the kernel answers a datagram to a port
    // nobody holds.
    client
        .add_subscription(&send_address_channel(), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver takes the port");
    let receiver_address = wait_for_new_address(&mut client, &[], "the subscription");

    let publication = client
        .add_publication(&send_address_channel(), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication whose socket is bound");
    let sender_address = wait_for_new_address(
        &mut client,
        std::slice::from_ref(&receiver_address),
        "the publication",
    );

    assert_ne!(
        receiver_address, sender_address,
        "the two ends are two sockets; a shared address would make this case read the wrong counter"
    );

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    // The endpoint first: `snd-channel` is the counter the driver already gives
    // back, so waiting on it is a wait on the *release*, not on the reading
    // under test.
    let start = Instant::now();

    while 0 < send_channel_counters(&client) {
        let _ = client.poll();

        assert!(
            start.elapsed() < DEADLINE,
            "the send endpoint was never released at all"
        );

        std::thread::sleep(Duration::from_millis(10));
    }

    let addresses = bound_addresses(&client);

    assert!(
        !addresses.contains(&sender_address),
        "the released send endpoint's address counter is still there ({sender_address}); \
         a subscription whose channel status id is recycled from this endpoint's would \
         resolve to a socket that is not its own"
    );

    // The control: the reading is not simply empty.
    assert!(
        addresses.contains(&receiver_address),
        "the control: the subscription's own address ({receiver_address}) has to still be \
         there, or this case is reading the wrong region"
    );
}
