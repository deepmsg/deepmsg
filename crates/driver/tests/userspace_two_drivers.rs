//! Two drivers, two stacks, one wire, no kernel: a publication on one machine
//! and a subscription on the other.
//!
//! This is the acceptance criterion the userspace stack exists for. Everything
//! else in `userspace_golden.rs` tests one transport answering the questions
//! the driver asks; this tests two *drivers* — the conductor, the sender, the
//! receiver, the port manager, the images — running their real passes over a
//! medium that is a pair of queues. If any of G1–G14 were answered wrongly the
//! frames would go somewhere else, or nowhere, and the message would not
//! arrive.
//!
//! Both drivers are `INVOKER`, driven by this test's own loop, and both clients
//! run on threads of their own because their calls wait for the driver's
//! replies (`embedded_driver.rs` is the same shape with one driver). They
//! communicate the way two machines do: a publication on `10.10.0.1` with the
//! subscriber's endpoint as its channel, a subscription on `10.10.0.2` that
//! binds it, ARP in both directions over the wire, and the Aeron frames in
//! between.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_core::clock;
use deepmsg_driver::config::{DriverConfig, ThreadingMode};
use deepmsg_driver::dir;
use deepmsg_driver::driver::Driver;
use deepmsg_driver::media::TransportBindings;
use deepmsg_driver::media::userspace::fake_port::{Side, Wire};
use deepmsg_driver::media::userspace::stack::{Clock, Stack, StackParams};

/// The stream the message travels on.
const STREAM_ID: i32 = 2002;

/// The subscriber's endpoint, which is what the publisher's channel names.
const SUBSCRIBER: &str = "10.10.0.2:40500";

/// The channel both ends name: the subscriber says where it hears, and the
/// publisher says where it sends.
const CHANNEL: &str = "aeron:udp?endpoint=10.10.0.2:40500";

/// How long the pair is given. Generous on purpose: the two drivers are driven
/// by one thread that also has to let the clients run, and a machine under load
/// (a CI runner running the rest of the suite) takes longer to be believed
/// than a quiet one. The archive suite's own ten-second timeout has been a
/// flake twice for exactly this reason.
const TIMEOUT: Duration = Duration::from_secs(30);

const ALICE: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 10, 0, 1);
const BOB: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 10, 0, 2);
const ALICE_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x01];
const BOB_MAC: [u8; 6] = [0x02, 0, 0, 0, 0, 0x02];

/// A directory of our own, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "deepmsg-userspace-{label}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");

        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A driver of one machine's stack, with the directory it owns.
struct Machine {
    dir: TempDir,
    driver: Driver,
}

impl Machine {
    fn start(label: &str, stack: &Arc<Stack>) -> Self {
        let dir = TempDir::new(label);
        let config = DriverConfig {
            aeron_dir: dir.0.clone(),
            threading_mode: ThreadingMode::Invoker,
            transport_bindings: TransportBindings::Userspace(Arc::clone(stack)),
            ..DriverConfig::default()
        };

        let prepared = dir::prepare(&config, clock::epoch_millis()).expect("prepare the directory");
        let cnc = CncFile::create(
            &config.aeron_dir,
            &CncLayout::default(),
            &CncIdentity {
                liveness_timeout_ns: deepmsg_cnc::CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
                start_timestamp_ms: clock::epoch_millis(),
                pid: i64::from(std::process::id()),
            },
        )
        .expect("create the CnC file");

        let driver = Driver::new(
            cnc,
            &config,
            deepmsg_driver::cpuset::CpuAssignment::default(),
        )
        .expect("the driver takes the file over");

        // The directory tree is the driver's to own from here on; `prepared`
        // going out of scope would remove it, and the machine owns the path.
        std::mem::forget(prepared);

        Self { dir, driver }
    }

    fn close(mut self) {
        self.driver.close().expect("the driver closes");
        drop(self.dir);
    }
}

/// The media's per-transport counters, as a client reads them out of the CnC
/// file: one per transport, whose value is what that transport has received
/// (design §4.10, type 76).
fn transport_counters(dir: &std::path::Path) -> Vec<deepmsg_cnc::CounterDescriptor> {
    let reader = CncFile::try_open(dir).expect("the file is published");
    let counters = reader.counters().expect("the counter regions");
    let mut found = Vec::new();

    counters.for_each(|descriptor| {
        if descriptor.type_id == deepmsg_driver::media::userspace::dpdk_counters::type_id::TRANSPORT
        {
            found.push(descriptor.clone());
        }
    });

    found
}

/// Subscribe on one machine and publish on the other, and see the message
/// arrive.
#[test]
fn a_message_crosses_from_one_machines_stack_to_the_others() {
    let wire = Wire::new();
    let alice = Stack::over_a_fake_wire(
        &wire,
        Side::A,
        StackParams::new(ALICE, 24, ALICE_MAC),
        Clock::Monotonic,
    );
    let bob = Stack::over_a_fake_wire(
        &wire,
        Side::B,
        StackParams::new(BOB, 24, BOB_MAC),
        Clock::Monotonic,
    );

    let mut alice_machine = Machine::start("alice", &alice);
    let mut bob_machine = Machine::start("bob", &bob);

    let payload = b"a frame over a wire that is two queues";

    // The publisher stops once the message has arrived, so that a test that
    // passes does not spend its whole deadline offering.
    let delivered = Arc::new(AtomicBool::new(false));

    // Bob subscribes first: his subscription is what binds the endpoint the
    // publisher names, and a publisher that starts first would be retrying
    // until it appeared (which is also worth being able to say, but this test
    // is about the message arriving at all).
    let subscriber = std::thread::spawn({
        let dir = bob_machine.dir.0.clone();
        let delivered = Arc::clone(&delivered);
        move || {
            let mut client = Client::connect(&dir).expect("connect to Bob's driver");
            let subscription = client
                .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
                .expect("a UDP subscription");

            let deadline = Instant::now() + TIMEOUT;
            let mut received = None;
            while received.is_none() && Instant::now() < deadline {
                client.poll();
                client.poll_subscription(subscription, 10, |message| {
                    received = Some(message.payload.to_vec());
                });
            }

            delivered.store(received.is_some(), Ordering::Release);

            received
        }
    });

    let publisher = std::thread::spawn({
        let dir = alice_machine.dir.0.clone();
        let delivered = Arc::clone(&delivered);
        move || {
            let mut client = Client::connect(&dir).expect("connect to Alice's driver");
            let publication = client
                .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
                .expect("a UDP publication");

            let deadline = Instant::now() + TIMEOUT;
            let mut offered = false;
            while Instant::now() < deadline && !delivered.load(Ordering::Acquire) {
                // `offer` answers the new position, or nothing when the term
                // is full: either way the frame is in the buffer and the
                // driver's sender is the one that puts it on the wire.
                offered |= client.offer(publication, payload).is_some();
                client.poll();
                std::thread::sleep(Duration::from_millis(1));
            }

            offered
        }
    });

    // Both drivers, one pass at a time, until the subscriber has its message
    // or the deadline passes.
    let deadline = Instant::now() + TIMEOUT;
    while !subscriber.is_finished() && Instant::now() < deadline {
        alice_machine.driver.do_work();
        bob_machine.driver.do_work();
        std::thread::sleep(Duration::from_micros(200));
    }

    let received = subscriber.join().expect("the subscriber's thread");
    let offered = publisher.join().expect("the publisher's thread");

    // What each machine's transport received, read the way a client reads it —
    // before the drivers are closed, because the counters belong to the CnC
    // file they are written in.
    let alice_transports = transport_counters(&alice_machine.dir.0);
    let bob_transports = transport_counters(&bob_machine.dir.0);

    alice_machine.close();
    bob_machine.close();

    assert!(offered, "the publication was offered at least once");
    assert_eq!(
        Some(payload.to_vec()),
        received,
        "and it arrived on the other machine's stack (subscriber endpoint {SUBSCRIBER})"
    );

    // And the wire carried Aeron frames and ARP, not nothing.
    assert!(
        wire.carried(Side::A) > 0 && wire.carried(Side::B) > 0,
        "both directions were used"
    );

    // Each machine's transport is a counter of its own, and both of them moved
    // (design §4.10, type 76). This is the pair the per-port counts cannot
    // give: the subscriber's socket received the `SETUP` and the data, and the
    // publisher's received the `SETUP` that told it where to send — which is
    // what makes the message cross at all.
    assert_eq!(
        1,
        bob_transports.len(),
        "one transport for the subscription"
    );
    assert!(
        bob_transports[0].value > 0,
        "the subscriber's socket received frames: {:?}",
        bob_transports[0].label
    );
    assert_eq!(
        1,
        alice_transports.len(),
        "one transport for the publication"
    );
    assert!(
        alice_transports[0].value > 0,
        "and the publisher heard the other end's setup: {:?}",
        alice_transports[0].label
    );
}
