//! T5: our driver on a port it drives itself, exchanging with the reference's
//! driver on the kernel — one cable, two media.
//!
//! The reference's DPDK media is closed (ADR-0006, design §2.1), so the two ends
//! of this pairing are not a choice: one side is this build's userspace stack on
//! a NIC bound to `vfio-pci`, and the other is the reference `aeronmd` on the
//! same machine's other card, over the kernel. What crosses between them is
//! ordinary Ethernet frames — the same bytes the kernel would have carried —
//! which is the whole claim of the media.
//!
//! # Running it
//!
//! It needs root, two cabled ports and a bound device, so it is `#[ignore]`d:
//!
//! ```text
//! # the two ports, and the kernel side's address (the reference binds it)
//! sudo dpdk-devbind.py --bind=vfio-pci 0000:01:00.0 0000:01:00.1
//! sudo ip addr add 10.10.0.2/24 dev enp3s0f0 && sudo ip link set enp3s0f0 up
//! # our driver's binary has to be the one with the media in it
//! cargo build --bins --features dpdk
//! sudo -E env PATH="$PATH" RUSTUP_HOME=~/.rustup CARGO_HOME=~/.cargo \
//!     DEEPMSG_REF_AERONMD=…/aeron-ref-664f58e705/cppbuild/Release/binaries/aeronmd \
//!     DEEPMSG_REF_BASICSUBSCRIBER=…/cppbuild/Samples/binaries/BasicSubscriber \
//!     DEEPMSG_REF_BASICPUBLISHER=…/cppbuild/Samples/binaries/BasicPublisher \
//!     DEEPMSG_REF_RESPONSE_CLIENT=…/cppbuild/Samples/binaries/response_client \
//!     DEEPMSG_REF_RESPONSE_SERVER=…/cppbuild/Samples/binaries/response_server \
//!     cargo test -p deepmsg-tests --features interop,dpdk \
//!     --test dpdk_cross_media -- --ignored --nocapture --test-threads=1
//! # and back: the address goes with the binding, or the default suite goes red
//! sudo ip addr del 10.10.0.2/24 dev enp3s0f0 && sudo ip link set enp3s0f0 down
//! sudo dpdk-devbind.py --bind=ixgbe 0000:01:00.0 0000:01:00.1
//! ```
//!
//! **One case at a time, and not as a preference**: a port bound to a driver is
//! that driver's alone, so two cases running at once are two drivers wanting one
//! port. `--test-threads=1` is load-bearing here, not tidiness.
//!
//! `0000:01:00.0` is the old X540, whose two functions share an IOMMU group, so
//! both are bound and one is named to the EAL; `0000:03:00.0` stays on the
//! kernel for the reference. Both are the machine's own hardware from D0
//! (`analysis/dpdk/deepmsg-dpdk-d0-record.md`).
//!
//! # Every arm proves it is on the media
//!
//! A message crossing proves nothing on its own: with no media selected, with
//! the wrong port, or with a fallback doing the work, **both drivers are on the
//! kernel and the message still arrives**. So each arm also asserts that this
//! driver's own counters moved — the media's per-port and per-transport ones,
//! read out of its CnC file — which is the only thing here that can tell "on the
//! media" from "accidentally reachable". T4 learned this the hard way.

#![cfg(all(feature = "interop", feature = "dpdk"))]

use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::CncFile;
use deepmsg_driver::media::userspace::dpdk_counters::type_id;
use deepmsg_tests::driver::{self, OwnDriver, ReferenceDriver};
use deepmsg_tests::samples;

/// The port our driver drives, and the one the reference keeps.
///
/// The old card's two functions are one IOMMU group, so both go to `vfio-pci`
/// and the EAL is allowed exactly one of them (`-a` is an allow-list).
const OWN_PCI: &str = "DEEPMSG_DPDK_T5_PCI";
const OWN_PCI_DEFAULT: &str = "0000:01:00.0";

/// The interface the kernel side is on, which is where the reference binds.
const REFERENCE_INTERFACE: &str = "DEEPMSG_DPDK_T5_INTERFACE";
const REFERENCE_INTERFACE_DEFAULT: &str = "enp3s0f0";

/// The two addresses, one per media, and the port they meet on.
const OWN: &str = "10.10.0.1";
const REFERENCE: &str = "10.10.0.2";
const PREFIX: u8 = 24;

/// The stream every arm of this file uses.
const STREAM_ID: i32 = 3003;

/// How long a driver is given to publish its CnC file.
const READY_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a message is given to cross. Generous because a 10GBASE-T link
/// between two ports DPDK has just started takes about twenty seconds to come
/// up (measured for T4), and nothing crosses until both ends are up.
const CROSSING_TIMEOUT: Duration = Duration::from_secs(90);

/// How long a capture is given, once it says it is listening and once the
/// traffic is over, before the case goes on.
///
/// Both halves are measurements rather than caution, and both were failures
/// first: traffic that begins within milliseconds of the readiness line is lost,
/// and a capture stopped 40 ms after its exchange is empty where the same case
/// stopped 140 ms after it is not. One second sits between what was measured to
/// fail and what was measured to work; it is paid twice per ARP case, by a file
/// that is run by hand.
const CAPTURE_SETTLES: Duration = Duration::from_secs(1);

fn variable(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// Our driver, configured for the media, on the port this file names.
///
/// The media selection has **no `-D` spelling** — the reference reads it from
/// the environment alone (`aeronmd.h:731`) — which is why this takes the
/// environment door rather than a property.
fn own_driver_on_the_media(test_name: &str) -> Option<OwnDriver> {
    let port = variable(OWN_PCI, OWN_PCI_DEFAULT);
    let address = format!("{OWN}/{PREFIX}");

    OwnDriver::start_with_env(
        test_name,
        &[
            &format!("-Ddeepmsg.dpdk.port={port}"),
            &format!("-Ddeepmsg.dpdk.ipv4.address={address}"),
            // One thread runs both agents: a virtual device has one queue pair
            // and a real one would take three, but the mode is not what this
            // file is about.
            "-Daeron.threading.mode=SHARED",
        ],
        &[("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA", "dpdk")],
    )
}

/// The reference driver, on the kernel, which needs no configuration of its
/// own: its address comes from the channel its client is given.
fn reference_driver(test_name: &str) -> Option<(ReferenceDriver, PathBuf)> {
    let binary = driver::locate_verified()?;

    ReferenceDriver::start(&binary, test_name)
        .ok()
        .map(|driver| {
            let dir = driver.aeron_dir().to_path_buf();
            (driver, dir)
        })
}

/// Whether this machine's kernel holds `address`, which is what the reference
/// binds and what the test's premise rests on.
fn the_kernel_holds(address: &str) -> bool {
    UdpSocket::bind((address, 0)).is_ok()
}

/// The values of every media counter of `type_id` in a driver's CnC file, as a
/// client reads them.
///
/// By type rather than by id: the media's counters are allocated after the
/// system ones, and the type is what the reference's own catalogue names
/// (`aeron-client/src/main/c/aeron_counters.h:155-173`).
fn media_counters(cnc: &CncFile, type_id: i32) -> Vec<i64> {
    let mut found = Vec::new();

    if let Some(counters) = cnc.counters() {
        counters.for_each(|descriptor| {
            if descriptor.type_id == type_id {
                found.push(descriptor.value);
            }
        });
    }

    found
}

/// A `tcpdump` on the kernel's side of the cable, for the length of one
/// crossing.
///
/// **The kernel's side is the only place a capture can be taken**, and that is
/// not a preference: the port this driver drives is bound to `vfio-pci`, so the
/// kernel has no interface for it and no `tcpdump` can see it. The other end of
/// the cable is an ordinary `ixgbe` interface, and everything that crosses —
/// our frames and the reference's — passes through it.
///
/// Text rather than a capture file: what this is evidence *of* is that ARP
/// crossed in both directions, and `tcpdump -e` writes exactly the sentences
/// that say so.
///
/// # It is a process, and each of its waits was a wrong answer first
///
/// Three times this type turned an exchange that crossed into "nothing
/// crossed", each in a different way. What it does now is what those
/// measurements leave standing:
///
/// * **The output goes to files, not pipes.** The readiness line is on
///   *stderr*, so a case that waits for it on *stdout* waits for ever — and a
///   case blocked inside the capture reports nothing at all, not a failure.
/// * **Readiness is waited for, and then waited out.** The readiness line comes
///   before the capture filter is installed, so what crosses in that moment is
///   not the socket's to keep. Measured: with the traffic starting within
///   milliseconds of that line — every case of two runs — the capture came back
///   holding nothing, while the same exchanges, in a capture that had been
///   running for seconds, were in the file.
/// * **It is given time to catch up before it is read.** The capture drains a
///   kernel ring; what it has not been scheduled to drain is not in the file
///   yet, and stopping it there loses evidence rather than collecting it.
///   Measured: a case stopped 40 ms after its exchange captured nothing where
///   the same case stopped 140 ms after its had it.
struct ArpCapture {
    child: std::process::Child,
    /// Where its capture goes, and where its complaints go.
    ///
    /// **Files and not pipes, and that is the whole design of this type.**
    /// A file can be read at leisure and blocks nobody meanwhile.
    path: std::path::PathBuf,
}

impl ArpCapture {
    /// Start watching `interface`, or [`None`] when there is no `tcpdump` to
    /// run — the same "announce and carry on" a missing sample gets.
    fn start(interface: &str) -> Option<Self> {
        let path = std::env::temp_dir().join(format!(
            "deepmsg-arp-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("a clock")
                .as_nanos()
        ));
        let log = std::fs::File::create(&path).ok()?;
        // **Its complaints go to a file too, and the file is read back.** A
        // capture whose `stderr` is `/dev/null` cannot be told from a capture
        // that died on start-up: both hand back an empty file, and an empty file
        // reads exactly like "nothing crossed" — the same confusion as "no media
        // selected" reading like "the media carried it".
        let errors = path.with_extension("err");
        let log_of_errors = std::fs::File::create(&errors).ok()?;

        let mut child = std::process::Command::new("tcpdump")
            .args(["-i", interface, "-n", "-e", "-l", "-U", "arp"])
            .stdout(std::process::Stdio::from(log))
            .stderr(std::process::Stdio::from(log_of_errors))
            .spawn()
            .ok()?;

        // Readiness is watched for, and then waited out — see the note on this
        // type for what each half is for.
        let began = Instant::now();
        loop {
            let said = std::fs::read_to_string(&errors).unwrap_or_default();

            if said.contains("listening on") {
                break;
            }

            if let Ok(Some(status)) = child.try_wait() {
                panic!("tcpdump left before it was listening on {interface} ({status}): {said}");
            }

            assert!(
                began.elapsed() < READY_TIMEOUT,
                "tcpdump never said it was listening on {interface}: {said}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        std::thread::sleep(CAPTURE_SETTLES);

        Some(Self { child, path })
    }

    /// Stop watching and hand back everything it saw, with what it said about
    /// itself appended when there is anything to say.
    fn lines(mut self) -> String {
        // Let it catch up before it is stopped, and read it while it is still
        // running: what has been drained is in the file, and stopping it is
        // teardown — nothing about the evidence should depend on teardown.
        std::thread::sleep(CAPTURE_SETTLES);

        let seen = std::fs::read_to_string(&self.path).unwrap_or_default();
        let said = std::fs::read_to_string(self.path.with_extension("err")).unwrap_or_default();

        // **Did it leave on its own?** A capture that exited is not a quiet
        // wire, and `kill` on a child that is already gone hides that.
        let fate = match self.child.try_wait() {
            Ok(Some(status)) => format!("tcpdump left on its own: {status}"),
            Ok(None) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
                String::new()
            }
            Err(excuse) => format!("tcpdump's status could not be read: {excuse}"),
        };

        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.path.with_extension("err"));

        // **A capture that died says so.** A `tcpdump` that never attached and
        // a quiet wire produce the same empty file, so its complaints are
        // carried back into whatever the caller asserts — a case that fails on
        // this says *which* of the two it was.
        let mut text = seen;
        if !said.trim().is_empty() {
            text.push_str(&format!("\n=== and tcpdump itself said:\n{said}"));
        }
        if !fate.is_empty() {
            text.push_str(&format!("\n=== {fate}"));
        }

        text
    }
}

/// Forget what this machine's kernel knows about `address`, so the next frame
/// for it has to be asked about.
///
/// The kernel caches what it learns — including from the *requests* it hears,
/// which is how it learns this driver's address in the first case — so a second
/// case that needs the kernel to ask would see no request at all. Measured: the
/// second run of the publishing case captured an empty file for exactly that
/// reason.
fn forget_the_kernels_neighbour(address: &str, interface: &str) {
    let flushed = std::process::Command::new("ip")
        .args(["neigh", "flush", address, "dev", interface])
        .status();

    assert!(
        flushed.as_ref().is_ok_and(|status| status.success()),
        "the kernel's entry for {address} on {interface} could not be forgotten: {flushed:?}"
    );
}

/// `address` was asked for on the wire and answered.
///
/// A frame cannot leave without this: the peer's MAC is unknown to both stacks,
/// and neither has a kernel to ask for the other's. What is asserted per case is
/// the exchange **that case's traffic needed** — the initiator resolves the
/// address it is about to send to, and the responder's reply is the other
/// half:
///
/// * our driver publishing to the reference's address asks the **kernel** for
///   it, and this driver's answer is not "proved" here — the kernel answering
///   is the foreign implementation's half;
/// * the reference publishing to ours asks **us**, and our stack's reply is
///   what the capture shows.
///
/// Together the two cases cover both directions, which is what the plan asks
/// for; neither covers both alone, because in each one only the sender has a
/// reason to ask. (The other direction's *frames* still flow — the receiving
/// side learns the sender's hardware address from the request itself, which is
/// why one exchange is enough for a stream that goes both ways.)
fn assert_arp_crossed(capture: &str, address: &str) {
    assert!(
        capture.contains(&format!("Request who-has {address}")),
        "nobody asked for {address}'s hardware address on the wire:\n{capture}"
    );
    assert!(
        capture.contains(&format!("Reply {address} is-at")),
        "{address} was asked for and never answered:\n{capture}"
    );
}

/// Wait until this driver's port is on the wire.
///
/// **Nothing may be sent before this**, and that is a measurement rather than a
/// precaution: a 10GBASE-T link between two ports DPDK has just started takes
/// about twenty seconds to come up (measured for T4), and a peer that gives up
/// sooner — the reference's `BasicPublisher` sends its messages a second apart
/// and then exits — is a peer that never sees the frames. The driver's own
/// counter 75 is the only thing that can say the port is up, and it is published
/// on the conductor's timer.
fn await_the_media_link(cnc: &CncFile) {
    let deadline = Instant::now() + CROSSING_TIMEOUT;

    loop {
        if media_counters(cnc, type_id::PORT_INFO).first() == Some(&1) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "this driver's port never came up on the wire"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The media is where this driver's data paths are: one port, one transport,
/// and frames through both.
///
/// The three assertions are three different things a driver that only *looks*
/// like it is on the media would fail: a port's own numbers exist (75), a
/// transport's count moved (76 — written by the thread that read the socket,
/// not by a timer), and the frames crossed the port rather than a socket (82,
/// one counter each way, both of them).
fn assert_the_media_carried_it(cnc: &CncFile) {
    // Polled rather than read once, and that is not politeness: the port's own
    // counters (75, 82, 83) are written by the conductor's **timer**, so right
    // after a message crosses they are still the numbers the last tick read —
    // measured, on the first run of this file, as `packets-received = 0` beside
    // a transport counter that had already recorded the datagram. The
    // transport's own count (76) is written where the socket is read, which is
    // why the two disagree for up to one tick.
    let deadline = Instant::now() + READY_TIMEOUT;

    loop {
        let link = media_counters(cnc, type_id::PORT_INFO);
        let transports = media_counters(cnc, type_id::TRANSPORT);
        let packets = media_counters(cnc, type_id::PKTS);
        let bytes = media_counters(cnc, type_id::BYTES);

        let carried = link.first() == Some(&1)
            && transports.first().is_some_and(|count| *count > 0)
            && packets.len() == 2
            && packets.iter().all(|count| *count > 0)
            && bytes.iter().any(|count| *count > 0);

        if carried {
            return;
        }

        let last = vec![
            ("link", link),
            ("transports", transports),
            ("packets", packets),
            ("bytes", bytes),
        ];

        assert!(
            Instant::now() < deadline,
            "the media's own counters never showed frames through the port: {last:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Our publisher's frames reach the reference's subscriber, over our port.
#[test]
#[ignore = "needs two cabled ports, one bound to vfio-pci, and the kernel's half configured"]
fn our_driver_publishes_across_a_real_port_to_the_reference() {
    assert!(
        the_kernel_holds(REFERENCE),
        "the kernel has to hold {REFERENCE} on {} for the reference to bind it \
         (see this file's runbook)",
        variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT)
    );

    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };
    let Some((mut reference, reference_dir)) = reference_driver("dpc-reference-subscriber") else {
        driver::announce_skip();
        return;
    };

    let port = 40_700 + (std::process::id() % 100) as u16;
    let channel = format!("aeron:udp?endpoint={REFERENCE}:{port}");
    let message = b"t5: a frame our port carried to the kernel's";

    // The subscriber first: it binds the port the publisher sends to.
    let subscriber = samples::Sample::start(
        &subscriber_binary,
        "subscriber",
        &reference_dir,
        &["-c", &channel, "-s", &STREAM_ID.to_string()],
    );
    subscriber.await_output(READY_TIMEOUT, "its channel", |output| {
        output.contains("Subscribing to channel")
    });

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a CnC file");

    let Some(mut own) = own_driver_on_the_media("dpc-our-publisher") else {
        driver::announce_own_skip();
        return;
    };
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver publishes a CnC file");

    await_the_media_link(&own_cnc);

    // From here the wire is watched: both stacks have to find each other's
    // hardware address before a frame can go anywhere.
    let arp = ArpCapture::start(&variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT));

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver confirms the publication");

    let deadline = Instant::now() + CROSSING_TIMEOUT;
    let mut offered = false;
    while Instant::now() < deadline {
        offered |= client.offer(publication, message).is_some();
        // The publisher's duty cycle: everything it is told comes off the same
        // ring as the `offer` answer.
        client.poll();
        std::thread::sleep(Duration::from_millis(10));

        if subscriber.output().contains("t5: a frame") {
            break;
        }
    }

    assert!(offered, "the publication was offered at least once");
    assert!(
        subscriber.output().contains("t5: a frame"),
        "the reference's subscriber never saw the message:\n{}",
        subscriber.output()
    );

    // Both stacks had to find each other's hardware address before any of this
    // could happen, and the kernel's side of the cable is where that is
    // visible. This case's sender is this driver, so the address resolved here
    // is the reference's — by the kernel, from the other end of the cable.
    if let Some(arp) = arp {
        assert_arp_crossed(&arp.lines(), REFERENCE);
    }

    // And it crossed *here*, which is the assertion this file exists for.
    assert_the_media_carried_it(&own_cnc);

    let _ = own.stop();
    let _ = reference.stop();
}

/// The reference's publisher's frames reach our subscriber, over our port.
#[test]
#[ignore = "needs two cabled ports, one bound to vfio-pci, and the kernel's half configured"]
fn our_driver_subscribes_across_a_real_port_from_the_reference() {
    assert!(
        the_kernel_holds(REFERENCE),
        "the kernel has to hold {REFERENCE} on {} (see this file's runbook)",
        variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT)
    );

    let Some(publisher_binary) = samples::locate("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };
    let Some((mut reference, reference_dir)) = reference_driver("dpc-reference-publisher") else {
        driver::announce_skip();
        return;
    };

    let port = 40_800 + (std::process::id() % 100) as u16;
    let channel = format!("aeron:udp?endpoint={OWN}:{port}");

    let Some(mut own) = own_driver_on_the_media("dpc-our-subscriber") else {
        driver::announce_own_skip();
        return;
    };
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver publishes a CnC file");

    // Our subscription binds our own address, which is the port's: nothing on
    // the kernel has it, so the media is the only thing that can serve it. The
    // client lives on the thread, which is the shape every other subscription
    // in this suite has.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = std::thread::spawn({
        let dir = own.aeron_dir().to_path_buf();
        let seen = Arc::clone(&seen);
        let channel = channel.clone();
        move || {
            let mut client = Client::connect(&dir).expect("connect our client");
            let subscription = client
                .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
                .expect("our driver confirms the subscription");

            let deadline = Instant::now() + CROSSING_TIMEOUT;
            let mut received = None;
            while received.is_none() && Instant::now() < deadline {
                client.poll();
                client.poll_subscription(subscription, 10, |message| {
                    received = Some(message.payload.to_vec());
                });
            }

            *seen.lock().expect("the seen list") = received.unwrap_or_default();
        }
    });

    await_the_media_link(&own_cnc);

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a CnC file");

    // **Forget the kernel's neighbour entry, then watch — in that order and no
    // other.** A stale entry is a *correct* one here (this driver's address does
    // not change), so the kernel sends to it without asking anybody, and a case
    // that captured before the flush would see an empty file and call it a
    // failure. Measured: it passed alone and failed in the whole file, because a
    // previous case had already taught the kernel this address. The window
    // between the two is now nothing but the far end's start-up, which is the
    // first thing that can make the kernel speak.
    let interface = variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT);
    forget_the_kernels_neighbour(OWN, &interface);
    let arp = ArpCapture::start(&interface);

    let publisher = samples::Sample::start(
        &publisher_binary,
        "publisher",
        &reference_dir,
        // `-m` is how many messages, not what they say: the sample's payload is
        // its own `Hello World! <n>`, and the assertion below reads that back.
        // Well more than the crossing needs: the sample sends one a second and
        // then exits, so the count is the budget it has to be heard in.
        &["-c", &channel, "-s", &STREAM_ID.to_string(), "-m", "60"],
    );
    publisher.await_output(READY_TIMEOUT, "its channel", |output| {
        output.contains("Publishing to channel")
    });

    let _ = subscriber.join();

    let seen = seen.lock().expect("the seen list").clone();
    assert!(
        String::from_utf8_lossy(&seen).starts_with("Hello World!"),
        "our subscription never saw the reference's message (it saw {} bytes).\n\
         === this driver's media counters: transport {:?}, packets {:?}, \
         bytes {:?}, arp-miss {:?}\n\
         === this driver's log\n{}\n=== the reference's publisher\n{}",
        seen.len(),
        media_counters(&own_cnc, type_id::TRANSPORT),
        media_counters(&own_cnc, type_id::PKTS),
        media_counters(&own_cnc, type_id::BYTES),
        media_counters(&own_cnc, type_id::ARP_MISS),
        own.log_tail(40),
        publisher.output()
    );

    // And here the sender is the reference, so what the capture shows is the
    // kernel asking *this* driver for its hardware address — the media's own
    // ARP answering a foreign stack.
    if let Some(arp) = arp {
        assert_arp_crossed(&arp.lines(), OWN);
    }

    assert_the_media_carried_it(&own_cnc);

    let _ = own.stop();
    let _ = reference.stop();
}

/// A frame this driver withholds on the way out is asked for again, over the
/// port it drives.
///
/// `deepmsg.debug.send.data.loss.drop.every` is this build's own injection
/// point — it holds a frame back while recording it as sent, so a real hole
/// appears on the wire — and what fills it is the *reference's* NAK coming back
/// to a driver whose receive path is a port it drives itself. That is the
/// pairing worth having: the sender of the NAK is a foreign implementation.
///
/// **What this can and cannot assert**: the interceptor's generator is
/// per-channel here where the reference's is process-wide (recorded in
/// `docs/compat.md`), so "the third frame was the one dropped" is not a claim
/// either side can make. What is asserted is that the stream arrives whole and
/// that *this* driver counted a retransmission — a hole, and the answer to it.
#[test]
#[ignore = "needs two cabled ports, one bound to vfio-pci, and the kernel's half configured"]
fn a_withheld_frame_is_asked_for_again_over_the_port() {
    assert!(
        the_kernel_holds(REFERENCE),
        "the kernel has to hold {REFERENCE} on {} (see this file's runbook)",
        variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT)
    );

    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };
    let Some((mut reference, reference_dir)) = reference_driver("dpc-reference-lossy") else {
        driver::announce_skip();
        return;
    };

    let port = 40_900 + (std::process::id() % 100) as u16;
    let channel = format!("aeron:udp?endpoint={REFERENCE}:{port}");

    let mut subscriber = samples::Sample::start(
        &subscriber_binary,
        "subscriber",
        &reference_dir,
        &["-c", &channel, "-s", &STREAM_ID.to_string()],
    );
    subscriber.await_output(READY_TIMEOUT, "its channel", |output| {
        output.contains("Subscribing to channel")
    });

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a CnC file");

    // One frame in four, counted over frames: the `SETUP` and the status
    // messages that share the slot are counted with the data.
    let port_number = variable(OWN_PCI, OWN_PCI_DEFAULT);
    let address = format!("{OWN}/{PREFIX}");
    let Some(mut own) = OwnDriver::start_with_env(
        "dpc-our-lossy-publisher",
        &[
            &format!("-Ddeepmsg.dpdk.port={port_number}"),
            &format!("-Ddeepmsg.dpdk.ipv4.address={address}"),
            "-Daeron.threading.mode=SHARED",
            "-Ddeepmsg.debug.send.data.loss.drop.every=4",
        ],
        &[("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA", "dpdk")],
    ) else {
        driver::announce_own_skip();
        return;
    };
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver publishes a CnC file");
    await_the_media_link(&own_cnc);

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver confirms the publication");

    // Twenty messages, so that several are withheld: the count is per frame and
    // a stream of one could slip between two drops.
    const MESSAGES: usize = 20;
    let payloads: Vec<String> = (0..MESSAGES).map(|i| format!("t5-loss-{i:03}")).collect();

    // **One message is offered until the far end has it, and only then the
    // next.** Offering the batch as fast as the publication accepts it is what
    // the first version of this case did, and it does not work: a publication
    // with no image yet accepts everything into its term and sends none of it,
    // so the subscriber's image — which forms from the `SETUP` that follows —
    // joins behind data that was never put on the wire. Waiting for each
    // message to be *seen* is also what makes the injection mean something:
    // a withheld frame is a message that does not appear until the NAK comes
    // back and the retransmission lands.
    let deadline = Instant::now() + CROSSING_TIMEOUT;
    let mut index = 0usize;

    while index < MESSAGES && Instant::now() < deadline {
        let _ = client.offer(publication, payloads[index].as_bytes());
        client.poll();
        std::thread::sleep(Duration::from_millis(2));

        if subscriber.output().contains(&payloads[index]) {
            index += 1;
        }
    }

    let offered = index;

    let last = payloads.last().cloned().expect("a last payload");
    let output = subscriber.await_output(Duration::from_secs(30), "every message", |output| {
        output.contains(&last)
    });

    let retransmits = counter_value_of(
        &own_cnc,
        deepmsg_driver::system_counters::id::RETRANSMITS_SENT,
    );

    let _ = subscriber.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();

    assert!(offered > 0, "the publication was offered nothing");
    for payload in &payloads {
        assert!(
            output.contains(payload),
            "the reference's subscriber never saw {payload}, so a withheld \
             frame was not recovered:\n{output}"
        );
    }
    assert!(
        retransmits.is_some_and(|value| value > 0),
        "a withheld frame is one a receiver has to ask for, and this driver \
         counted no retransmission: {retransmits:?}\n{output}"
    );
}

/// The value of one system counter, by id, out of a driver's CnC file.
fn counter_value_of(cnc: &CncFile, counter_id: i32) -> Option<i64> {
    cnc.counters()?.value(counter_id)
}

/// A frame the reference sends is dropped **on this side**, our NAK goes out,
/// and the reference sends it again — over the port.
///
/// The interceptor chain hangs on the receive *endpoint*
/// (`crates/driver/src/receiver.rs`, `endpoint.drops`), not on a transport, so
/// it is the same chain a kernel socket would have; what it costs the frames is
/// what makes this driver the one that has to ask. The NAK that goes out is
/// ours and the retransmission is the reference's, which is the other direction
/// of the same exchange.
///
/// **`fixed-loss`, not `loss`.** A rate is a *draw*: over the few frames this
/// case carries, one can drop nothing at all — measured, twice, with a rate of
/// an eighth and then a half over all frame types, on a stream that arrived
/// whole with a NAK counter reading zero. `fixed-loss` is a **rule** — a byte
/// range of one term, dropped once per stream and session
/// (`media/aeron_udp_channel_transport_fixed_loss.c`) — so the hole is not a
/// probability. The arrangement is the reference harness's own
/// (`CTestMediaDriver.java:409-426`), and the range needs the stream pinned:
/// the channel names `init-term-id=0|term-id=0|term-offset=0` so that term 0 is
/// the term the stream starts in, which is the term the interceptor has been
/// told about.
///
/// `AERON_NAK_UNICAST_DELAY` is set the way the harness sets it
/// (`CTestMediaDriver.java:449-452`), so the NAK is not coalesced into a delay
/// this case would have to wait out.
#[test]
#[ignore = "needs two cabled ports, one bound to vfio-pci, and the kernel's half configured"]
fn a_frame_dropped_here_is_asked_for_again_over_the_port() {
    use deepmsg_driver::position::type_id::RECEIVER_NAKS_SENT;

    assert!(
        the_kernel_holds(REFERENCE),
        "the kernel has to hold {REFERENCE} on {} (see this file's runbook)",
        variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT)
    );

    let Some(publisher_binary) = samples::locate("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };
    let Some((mut reference, reference_dir)) = reference_driver("dpc-reference-lossy-sender")
    else {
        driver::announce_skip();
        return;
    };

    let port = 41_000 + (std::process::id() % 100) as u16;
    // The position triple is what makes `term-id=0` in the interceptor's range
    // a real term: a publication that names no position picks a random initial
    // term id.
    let channel = format!(
        "aeron:udp?endpoint={OWN}:{port}|term-length=64k|init-term-id=0|term-id=0|term-offset=0"
    );

    let port_number = variable(OWN_PCI, OWN_PCI_DEFAULT);
    let address = format!("{OWN}/{PREFIX}");
    let Some(mut own) = OwnDriver::start_with_env(
        "dpc-our-lossy-subscriber",
        &[
            &format!("-Ddeepmsg.dpdk.port={port_number}"),
            &format!("-Ddeepmsg.dpdk.ipv4.address={address}"),
            "-Daeron.threading.mode=SHARED",
        ],
        &[
            ("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA", "dpdk"),
            // **`fixed-loss`, not `loss`.** A rate is a *draw*: over the few
            // frames this case carries one can easily drop nothing at all, and
            // a `rate` with no `recv-msg-mask` drops **nothing at all** —
            // measured, over three runs of this case, as a stream that arrived
            // whole with `receiver-naks-sent` reading zero. `fixed-loss` is a
            // **rule**: a byte range of one term, dropped once per stream and
            // session, which is the arrangement the reference's own harness
            // uses (`CTestMediaDriver.java:409-426`). The range needs the
            // stream pinned to the term it names, which is why the channel
            // below carries `init-term-id=0|term-id=0|term-offset=0`.
            ("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "fixed-loss"),
            (
                "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_FIXED_LOSS_ARGS",
                "term-id=0|term-offset=0|length=2048",
            ),
            // The harness sets this too (`CTestMediaDriver.java:449-452`), so
            // the NAK is not coalesced into a delay this case would have to
            // wait out.
            ("AERON_NAK_UNICAST_DELAY", "0"),
        ],
    ) else {
        driver::announce_own_skip();
        return;
    };
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver publishes a CnC file");
    await_the_media_link(&own_cnc);

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = std::thread::spawn({
        let dir = own.aeron_dir().to_path_buf();
        let seen = Arc::clone(&seen);
        let channel = channel.clone();
        move || {
            let mut client = Client::connect(&dir).expect("connect our client");
            let subscription = client
                .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
                .expect("our driver confirms the subscription");

            let deadline = Instant::now() + CROSSING_TIMEOUT;
            // The publisher sends one a second and then exits, so the count is
            // the budget: wait for as many as it promised.
            while seen.lock().expect("the seen list").len() < 8 && Instant::now() < deadline {
                client.poll();
                client.poll_subscription(subscription, 10, |message| {
                    seen.lock()
                        .expect("the seen list")
                        .push(String::from_utf8_lossy(message.payload).into_owned());
                });
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    });

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a CnC file");

    let publisher = samples::Sample::start(
        &publisher_binary,
        "publisher",
        &reference_dir,
        &["-c", &channel, "-s", &STREAM_ID.to_string(), "-m", "8"],
    );
    publisher.await_output(READY_TIMEOUT, "its channel", |output| {
        output.contains("Publishing to channel")
    });

    let _ = subscriber.join();
    let seen = seen.lock().expect("the seen list").clone();

    let reader = deepmsg_cnc::CncFile::try_open(own.aeron_dir()).expect("the CnC file");
    let counters = reader.counters().expect("the counter regions");
    let naks = counters
        .find_by_type_id(RECEIVER_NAKS_SENT)
        .and_then(|counter| counters.value(counter.counter_id));

    let _ = own.stop();
    let _ = reference.stop();

    assert_eq!(
        8,
        seen.len(),
        "the reference's stream did not arrive whole even though a dropped frame \
         is what a NAK is for: {seen:?}\n{}",
        publisher.output()
    );
    assert!(
        naks.is_some_and(|value| value > 0),
        "this driver asked for nothing, so nothing here was dropped: \
         receiver-naks-sent={naks:?} over {seen:?}\n=== this driver's log\n{}",
        own.log_tail(25)
    );
}

/// A **dynamic MDC** channel crosses between the media and the kernel.
///
/// The shape that makes this different from every case above: the channel names
/// `control=` and **no `endpoint=` at all**. The publication binds the control
/// address and the subscription elicits to it, so the address that decides which
/// media carries the channel is the *control* one — and here it is a media-side
/// address on our half and a kernel-side one on the reference's, which is the
/// half of the rule that keeps MDC on a port this driver drives possible at all.
/// The other half (a loopback control address falls back to the kernel) is what
/// the reference's own system tests found and what the fallback rule was
/// extended for.
///
/// It is also the case that would have been refused outright before that
/// extension: our publication's destination is `0.0.0.0` until its stream tells
/// it otherwise, which the reachability refusal used to answer `ENETUNREACH`.
#[test]
#[ignore = "needs two cabled ports, one bound to vfio-pci, and the kernel's half configured"]
fn a_dynamic_mdc_channel_crosses_between_the_media_and_the_kernel() {
    assert!(
        the_kernel_holds(REFERENCE),
        "the kernel has to hold {REFERENCE} on {} (see this file's runbook)",
        variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT)
    );

    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };
    let Some((mut reference, reference_dir)) = reference_driver("dpc-mdc-reference-subscriber")
    else {
        driver::announce_skip();
        return;
    };

    let port = 41_100 + (std::process::id() % 100) as u16;
    // Our half binds the control address — the parse puts it in the *local*
    // pair for a channel with no endpoint — and the reference's half dials it.
    let publication_channel =
        format!("aeron:udp?control={OWN}:{port}|control-mode=dynamic|term-length=64k");
    let subscription_channel =
        format!("aeron:udp?endpoint=0.0.0.0:0|control={OWN}:{port}|term-length=64k");
    let message = b"t5: a dynamic MDC message over the port";

    let mut subscriber = samples::Sample::start(
        &subscriber_binary,
        "subscriber",
        &reference_dir,
        &["-c", &subscription_channel, "-s", &STREAM_ID.to_string()],
    );
    subscriber.await_output(READY_TIMEOUT, "its channel", |output| {
        output.contains("Subscribing to channel")
    });

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a CnC file");

    let Some(mut own) = own_driver_on_the_media("dpc-our-mdc-publisher") else {
        driver::announce_own_skip();
        return;
    };
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver publishes a CnC file");
    await_the_media_link(&own_cnc);

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication = client
        .add_publication(&publication_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver confirms the MDC publication");

    let deadline = Instant::now() + CROSSING_TIMEOUT;
    let mut offered = false;
    while Instant::now() < deadline {
        offered |= client.offer(publication, message).is_some();
        client.poll();
        std::thread::sleep(Duration::from_millis(10));

        if subscriber.output().contains("t5: a dynamic MDC") {
            break;
        }
    }

    assert!(offered, "the publication was offered at least once");
    assert!(
        subscriber.output().contains("t5: a dynamic MDC"),
        "the reference's subscriber never saw the MDC message:\n{}",
        subscriber.output()
    );

    assert_the_media_carried_it(&own_cnc);

    // **The sample goes before the drivers do.** A `BasicSubscriber` left
    // running polls a driver directory that the next case's `stop` deletes —
    // and it was the thing that made the case *after* this one hang rather than
    // fail (measured: this case's subscriber left behind, and the publishing
    // case never finished).
    let _ = subscriber.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();
}

/// A **response channel** crosses between the media and the kernel.
///
/// The reference's own two response samples, one per driver: the requester on
/// ours (so its response subscription takes an ephemeral port on the media) and
/// the responder on the reference's (so its publication binds the control
/// address on the kernel). The nine-step handshake — the client's offer, the
/// server's image, the session it answers on, the elicit by name, the server's
/// publication learning where it may send — therefore crosses the cable.
///
/// The address discipline is the reference's, from `samples_configuration.h`:
/// the response channel is `aeron:udp?control=<addr>` and nothing else, which is
/// what gives the requester somewhere to elicit and the responder nowhere to
/// send until it is asked. Our half's subscription names **no local address**, so
/// this is the case that pins the other half of the fallback rule end to end: a
/// control address that is routable — the kernel's address on the other end of
/// the cable — keeps the channel on the media.
#[test]
#[ignore = "needs two cabled ports, one bound to vfio-pci, and the kernel's half configured"]
fn a_response_channel_crosses_between_the_media_and_the_kernel() {
    assert!(
        the_kernel_holds(REFERENCE),
        "the kernel has to hold {REFERENCE} on {} (see this file's runbook)",
        variable(REFERENCE_INTERFACE, REFERENCE_INTERFACE_DEFAULT)
    );

    let Some(client_binary) = samples::locate("response_client") else {
        driver::announce_tool_skip("response_client");
        return;
    };
    let Some(server_binary) = samples::locate("response_server") else {
        driver::announce_tool_skip("response_server");
        return;
    };
    let Some((mut reference, reference_dir)) = reference_driver("dpc-response-server") else {
        driver::announce_skip();
        return;
    };

    // The two channels, at ports this process owns rather than the samples'
    // defaults, and both on the **kernel's** side of the cable: the request
    // channel is the server's subscription's endpoint, and the response channel
    // is the server's publication's control address. Our half reaches both
    // through the wire, which is the point.
    let request = format!(
        "aeron:udp?endpoint={REFERENCE}:{}",
        41_200 + (std::process::id() % 100) as u16
    );
    let response = format!(
        "aeron:udp?control={REFERENCE}:{}",
        41_300 + (std::process::id() % 100) as u16
    );

    // The server first: its subscription has to be up before the client offers,
    // because an offer to a publication with no reader is not connected. Its
    // readiness cannot be read — the samples' `printf` is fully buffered to a
    // file and this one never exits — so the client's own first `yay!` is what
    // says the pair is up.
    let mut server = samples::Sample::start(
        &server_binary,
        "response-server",
        &reference_dir,
        &["-c", &request, "-d", &response],
    );

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver publishes a CnC file");

    let Some(mut own) = own_driver_on_the_media("dpc-our-response-client") else {
        driver::announce_own_skip();
        return;
    };
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver publishes a CnC file");
    await_the_media_link(&own_cnc);

    let mut client = samples::Sample::start(
        &client_binary,
        "response-client",
        own.aeron_dir(),
        &["-c", &request, "-d", &response, "-m", "5"],
    );

    // The assertion, and the only one that covers the whole handshake: the
    // client has no image on its response subscription until the server's image
    // has said which session it answers on, the client has elicited by name, and
    // the server's publication has learned from that ask where it may send. Any
    // of the nine steps missing and this line never prints.
    let output = client.await_output(CROSSING_TIMEOUT, "a response", |output| {
        output.contains("Response to stream")
    });

    assert!(
        output.contains("yay!"),
        "the client's offers were answered:\n{output}"
    );

    assert_the_media_carried_it(&own_cnc);

    let _ = client.terminate(Duration::from_secs(5));
    let _ = server.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();
}
