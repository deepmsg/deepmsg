//! T4: two media drivers, one port each, and a message that crosses between
//! them — over frames this build put on a wire itself.
//!
//! This is the acceptance criterion the DPDK media exists for, and it is the
//! one thing no other test in this repository can be: **two driver processes**
//! (one EAL each), each driving its own port, with a client on each end and
//! nothing but Ethernet frames between them. Every other test of the userspace
//! stack runs both drivers in one process over an in-memory wire
//! (`userspace_two_drivers.rs`); this one has the process boundary, the two
//! EALs, the vfio-less device and the driver binary's own startup in the way.
//!
//! # Why it needs privileges, and what it is given instead
//!
//! The port is `net_af_packet` over a **veth pair**, which needs `CAP_NET_RAW`
//! — so this case is `#[ignore]`d and runs on a machine where the pair has been
//! made by hand, exactly like the D3 crate's own veth case. The frames are
//! real: they go out one kernel interface and arrive at the other, through
//! `AF_PACKET`, with a real Ethernet header and a real ARP exchange in front of
//! them.
//!
//! **Neither end is given an address**, and that is the point rather than a
//! detail: with no kernel address on either side of the pair, nothing but the
//! two DPDK stacks can answer the other's ARP — so the message arriving is
//! also the proof that ARP works between two of *these* stacks, which is the
//! thing the design's §4.5 says a vfio-bound port has to do for itself.
//!
//! # The counters this can and cannot check
//!
//! The design's T4 asks for the message to arrive *and* for both drivers'
//! counters 76 and 84 to be non-zero. **76 is asserted here** — one per
//! transport, written by the thread that reads the socket, so a real port and a
//! real receive path are the only way to move it — and so are 75, 82, 83 and
//! 91, out of the CnC file each driver wrote. **84 is not, and that is a
//! correction to the design rather than a gap**: it is `rte_eth_stats.imissed`,
//! what the *device* dropped, which is zero in a healthy run and moves only
//! when a port is overrun. A message crossing cannot make it non-zero, so the
//! test asserts it is readable and leaves the number to a pressure test (the
//! D4 record's §「7c 的更正」).

#![cfg(feature = "dpdk")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::CncFile;
use deepmsg_driver::media::userspace::dpdk_counters::type_id;
use deepmsg_driver::system_counters::id;

/// The two ends of the pair, named by whoever made it: a veth pair's
/// interfaces, or two hardware ports' PCI addresses.
const VETH_A: &str = "DEEPMSG_DPDK_T4_VETH_A";
const VETH_B: &str = "DEEPMSG_DPDK_T4_VETH_B";
const PCI_A: &str = "DEEPMSG_DPDK_T4_PCI_A";
const PCI_B: &str = "DEEPMSG_DPDK_T4_PCI_B";

/// Where one driver's data paths run.
///
/// Two shapes because the case has two: a `net_af_packet` port over a veth end,
/// which any machine with root can make, and a hardware port bound to
/// `vfio-pci`, which is what the media exists for. What differs between them is
/// what the EAL is told and whether the memory is huge pages: a device that
/// DMAs needs the pages the machine has reserved, where a veth is a software
/// queue and `--no-huge` keeps it out of them.
#[derive(Clone, Debug)]
enum Device {
    /// A virtual device, by the interface its frames go through.
    Veth(String),
    /// A hardware port, by the PCI address the EAL finds it at.
    Pci(String),
}

impl Device {
    /// What the EAL is given to find it.
    fn eal_args(&self) -> String {
        match self {
            Self::Veth(interface) => format!("--no-huge --vdev=net_af_packet0,iface={interface}"),
            Self::Pci(address) => format!("-a {address}"),
        }
    }

    /// What `deepmsg.dpdk.port` names, which is how the driver finds its port
    /// once the EAL has probed it: a vdev's name, or the PCI address EAL knows
    /// the device by.
    fn port(&self) -> &str {
        match self {
            Self::Veth(_) => "net_af_packet0",
            Self::Pci(address) => address,
        }
    }
}

/// The two devices this run was given, in the order they are used.
///
/// # Panics
///
/// When neither pair of variables is set: which devices to use is the one thing
/// this case cannot decide for itself.
fn the_two_devices() -> (Device, Device) {
    match (
        std::env::var(VETH_A),
        std::env::var(VETH_B),
        std::env::var(PCI_A),
        std::env::var(PCI_B),
    ) {
        (Ok(a), Ok(b), _, _) => (Device::Veth(a), Device::Veth(b)),
        (_, _, Ok(a), Ok(b)) => (Device::Pci(a), Device::Pci(b)),
        _ => panic!(
            "set either {VETH_A} and {VETH_B} (a veth pair), or {PCI_A} and \
             {PCI_B} (two ports bound to vfio-pci):\n  \
             sudo ip link add deepmsg-a type veth peer name deepmsg-b\n  \
             sudo ip link set deepmsg-a up\n  sudo ip link set deepmsg-b up\n\
             sudo dpdk-devbind.py --bind=vfio-pci 0000:01:00.0 0000:01:00.1 0000:03:00.0"
        ),
    }
}

/// The stream the message travels on.
const STREAM_ID: i32 = 2002;

/// The two addresses, one per driver, and the channel that joins them.
const ALICE: &str = "10.10.0.1/24";
const BOB: &str = "10.10.0.2/24";
const CHANNEL: &str = "aeron:udp?endpoint=10.10.0.2:40500";

/// The message.
const PAYLOAD: &[u8] = b"t4: a frame this build put on a wire";

/// How long the pair is given. Generous for the same reason
/// `userspace_two_drivers.rs` is: two driver processes, two EALs, two clients
/// and an ARP exchange before any of it, on a machine that may be running the
/// rest of a suite.
///
/// **And a real cable makes it a measurement rather than a guess**: a
/// 10GBASE-T link between two ports that a driver has just started takes about
/// **twenty seconds** to come up, measured on the X540 pair with testpmd
/// holding both ends (the kernel's carrier a moment earlier says nothing — `ip
/// link set down` before the bind starts the PHY's negotiation over). Until
/// both ends are up nothing crosses, so this is the window the exchange has to
/// happen inside, not the exchange itself.
const TIMEOUT: Duration = Duration::from_secs(90);

/// How long a freshly spawned driver is given to publish its CnC file.
const READY_TIMEOUT: Duration = Duration::from_secs(20);

/// A driver subprocess in a directory of its own, killed if it outlives the
/// test.
struct Driver {
    child: Child,
    dir: PathBuf,
}

impl Driver {
    /// Start a driver whose data paths are on `device`, holding `address`.
    ///
    /// `SHARED` and not the default `DEDICATED`: a threading mode is a map of
    /// queues to threads, `DEDICATED` wants three queue pairs, and a virtual
    /// device has one of each — the same wall every vdev in this build has met
    /// (the D4 record's "虚拟设备只有一条队列"). The hardware port would take
    /// three, but the mode is not what this case is about: what it is about is
    /// whether a message crosses, and one thread running both agents is the
    /// smallest thing that can answer.
    fn start(dir: &Path, device: &Device, address: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_deepmsg-driver"))
            .arg(format!("-Ddeepmsg.dir={}", dir.display()))
            .arg("-Daeron.threading.mode=SHARED")
            // The selection is the one setting with no `-D` spelling: the
            // reference reads it from the environment alone
            // (`aeronmd.h:731`), and so does this build. Without it the driver
            // is configured *for* the media and not *on* it — every `dpdk.*`
            // setting is read either way — and the channel goes to the kernel,
            // where 10.10.0.2 is an address it does not have.
            .env("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA", "dpdk")
            .arg(format!("-Ddeepmsg.dpdk.eal.args={}", device.eal_args()))
            .arg(format!("-Ddeepmsg.dpdk.port={}", device.port()))
            .arg(format!("-Ddeepmsg.dpdk.ipv4.address={address}"))
            // Both streams kept: the media's own line ("dpdk media on …") goes
            // to stdout and a refusal to stderr, and a case that fails without
            // either is a case nobody can read.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the driver binary is built alongside this test");

        Self {
            child,
            dir: dir.to_owned(),
        }
    }

    /// Wait for **this** driver's CnC file, identified by pid.
    ///
    /// By pid because two drivers are running at once here and a directory may
    /// outlive the driver that wrote it — the mistake `dir.rs` exists to
    /// prevent in the other direction (`lifecycle.rs` makes the same check for
    /// the same reason).
    fn await_cnc(&mut self) -> CncFile {
        let pid = i64::from(self.child.id());
        let deadline = Instant::now() + READY_TIMEOUT;

        loop {
            if let Ok(cnc) = CncFile::try_open(&self.dir) {
                if cnc.metadata().pid == pid {
                    return cnc;
                }
            }

            if !self.is_running() {
                let dir = self.dir.display().to_string();
                let stderr = self.stderr();
                let stdout = self.stdout();
                panic!(
                    "the driver in {dir} exited before publishing a CnC file;\n\
                     stdout:\n{stdout}\nstderr:\n{stderr}"
                );
            }

            if Instant::now() >= deadline {
                let dir = self.dir.display().to_string();
                // Stopped first: this branch is reached with the driver still
                // running, and reading a pipe whose writer is alive waits for a
                // write that is never coming — a case that should fail after
                // `READY_TIMEOUT` would instead sit there until the harness
                // killed it.
                self.stop();
                let stderr = self.stderr();
                let stdout = self.stdout();
                panic!(
                    "no CnC file for pid {pid} in {dir} within {READY_TIMEOUT:?};\n\
                     stdout:\n{stdout}\nstderr:\n{stderr}"
                );
            }
            std::thread::sleep(Duration::from_millis(16));
        }
    }

    /// Stop the driver, so that its output can be read.
    ///
    /// A pipe stays open while its writer lives, so `stdout()`/`stderr()`
    /// **block** until the process is gone — which is worth knowing before
    /// calling them on a driver that is still running, and is what made this
    /// case hang instead of fail the first time it was written.
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Everything the driver has written to stdout, which is where a healthy
    /// start says which port it is on.
    fn stdout(&mut self) -> String {
        use std::io::Read as _;

        let mut out = String::new();
        if let Some(mut stdout) = self.child.stdout.take() {
            let _ = stdout.read_to_string(&mut out);
        }
        out
    }

    /// Everything the driver has written to stderr, once it has exited.
    fn stderr(&mut self) -> String {
        use std::io::Read as _;

        let mut out = String::new();
        if let Some(mut stderr) = self.child.stderr.take() {
            let _ = stderr.read_to_string(&mut out);
        }
        out
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A directory of our own in the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};

        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-t4-{label}-{}-{n}", std::process::id()));
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One driver's counter **of a type**, read the way a client reads it.
///
/// By type rather than by id because the media's counters are allocated after
/// the system ones and the manager hands the ids out: the type is what the
/// reference's own table names (`aeron-client/src/main/c/aeron_counters.h`), and
/// it is also what tells one of a media's counters from a driver's.
fn media_counter(cnc: &CncFile, type_id: i32) -> Option<i64> {
    media_counters(cnc, type_id)
        .last()
        .map(|counter| counter.value)
}

/// Every counter of a type, which is what a type that is *one per* something
/// has to be read as: two transports are two counters, and asking for "the"
/// one of them is asking the wrong question.
fn media_counters(cnc: &CncFile, type_id: i32) -> Vec<deepmsg_cnc::CounterDescriptor> {
    let mut found = Vec::new();

    if let Some(counters) = cnc.counters() {
        counters.for_each(|descriptor| {
            if descriptor.type_id == type_id {
                found.push(descriptor.clone());
            }
        });
    }

    found
}

/// One driver's counter, read the way a client reads it.
///
/// Through [`CncFile::counters`] and not [`CncFile::counter_regions`]: the two
/// are the same bytes at two access levels, and the second is the **writer's**
/// pair — it is built from writable windows and answers `None` on the
/// read-only mapping [`CncFile::try_open`] makes, which is the one this case
/// holds. A counter is read, not allocated, here.
fn counter(cnc: &CncFile, counter_id: i32) -> i64 {
    cnc.counters()
        .expect("a mapped CnC file")
        .value(counter_id)
        .unwrap_or_else(|| panic!("counter {counter_id} is allocated"))
}

/// A message crosses from one machine's driver to the other's, over frames the
/// two of them put on a wire themselves.
///
/// Needs either a veth pair or two hardware ports, and root either way, so it
/// is `#[ignore]`d; run it by hand:
///
/// ```text
/// sudo ip link add deepmsg-a type veth peer name deepmsg-b
/// sudo ip link set deepmsg-a up && sudo ip link set deepmsg-b up
/// sudo -E env PATH="$PATH" RUSTUP_HOME=~/.rustup CARGO_HOME=~/.cargo \
///     DEEPMSG_DPDK_T4_VETH_A=deepmsg-a DEEPMSG_DPDK_T4_VETH_B=deepmsg-b \
///     cargo test -p deepmsg-driver --features dpdk -- --ignored --nocapture dpdk_two_drivers
/// ```
///
/// …or, for two hardware ports with a cable between them, one per driver
/// (`-a` is an allow-list, so a second function bound to `vfio-pci` beside the
/// one named is not probed):
///
/// ```text
/// sudo dpdk-devbind.py --bind=vfio-pci 0000:01:00.0 0000:01:00.1 0000:03:00.0
/// sudo -E env PATH="$PATH" RUSTUP_HOME=~/.rustup CARGO_HOME=~/.cargo \
///     DEEPMSG_DPDK_T4_PCI_A=0000:01:00.0 DEEPMSG_DPDK_T4_PCI_B=0000:03:00.0 \
///     cargo test -p deepmsg-driver --features dpdk -- --ignored --nocapture dpdk_two_drivers
/// ```
///
/// **No addresses on either end**: the kernels must not be the ones answering
/// ARP, or the message would prove nothing about the two stacks. A bound
/// hardware port has none by construction — that is what binding it takes away
/// — and a veth end is simply left unaddressed.
#[test]
#[ignore = "needs two devices and root: set DEEPMSG_DPDK_T4_VETH_A/_B (veth) or DEEPMSG_DPDK_T4_PCI_A/_B (vfio) and run under sudo"]
fn a_message_crosses_between_two_drivers_on_a_dpdk_port() {
    let (device_a, device_b) = the_two_devices();

    let alice_dir = TempDir::new("alice");
    let bob_dir = TempDir::new("bob");

    // One at a time, and not for tidiness. Two `net_af_packet` ports coming up
    // in the same instant race in the PMD's own fanout setup and one of them
    // loses with `could not set PACKET_FANOUT on AF_PACKET socket …: Invalid
    // argument` — a failure of the *second* EAL to probe its device, before
    // either driver has done anything (the D4 record has the diagnosis). The
    // second driver waits for the first to publish its CnC file, which is
    // after its port is up.
    let mut alice = Driver::start(&alice_dir.0, &device_a, ALICE);
    let alice_cnc = alice.await_cnc();
    let mut bob = Driver::start(&bob_dir.0, &device_b, BOB);
    let bob_cnc = bob.await_cnc();

    // Bob subscribes first: his subscription is what binds the endpoint Alice
    // will send to, and the two drivers are separate processes, so there is no
    // shared loop to order them in.
    let bound = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicBool::new(false));
    let messages = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first = Arc::new(std::sync::Mutex::new(Vec::new()));

    let subscriber = {
        let dir = bob_dir.0.clone();
        let bound = Arc::clone(&bound);
        let received = Arc::clone(&received);
        let messages = Arc::clone(&messages);
        let first = Arc::clone(&first);

        std::thread::spawn(move || {
            let mut client = Client::connect(&dir).expect("a client for the second driver");
            let subscription = client
                .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
                .expect("a subscription on the second driver");
            bound.store(true, Ordering::Release);

            let deadline = Instant::now() + TIMEOUT;
            while Instant::now() < deadline && !received.load(Ordering::Acquire) {
                // One duty cycle before the read, and it is not a formality:
                // `ON_AVAILABLE_IMAGE` is what puts the image on the
                // subscription, and it arrives on the same to-clients ring the
                // driver's ready responses do. A loop that only calls
                // `poll_subscription` never takes it off, and a subscription
                // with no images polls nothing however healthy the driver is —
                // which is exactly how this case failed before it was written
                // down here (`userspace_two_drivers.rs` polls the same way).
                client.poll();

                let _ = client.poll_subscription(subscription, 10, |message| {
                    messages.fetch_add(1, Ordering::Relaxed);

                    let mut first = first.lock().expect("the first message");
                    if first.is_empty() {
                        *first = message.payload.to_vec();
                    }

                    if message.payload == PAYLOAD {
                        received.store(true, Ordering::Release);
                    }
                });
            }
        })
    };

    let ready = Instant::now() + TIMEOUT;
    while !bound.load(Ordering::Acquire) && Instant::now() < ready {
        std::thread::sleep(Duration::from_millis(5));
    }
    // Stopped before either is read, for the reason `Driver::stop` gives: a
    // live process holds its end of the pipe open, so reading one is a wait
    // for a write that is not coming. This is the branch a driver on the wrong
    // media lands in — the endpoint is an address only the media has, so the
    // kernel answers the bind with `Cannot assign requested address` and the
    // subscription never becomes ready — and it used to hang here rather than
    // say so.
    if !bound.load(Ordering::Acquire) {
        alice.stop();
        bob.stop();

        panic!(
            "the second driver never bound the endpoint the first one sends to.\n\
             === the second driver: stdout\n{}\n=== the second driver: stderr\n{}",
            bob.stdout(),
            bob.stderr()
        );
    }

    // Alice publishes at the subscriber's endpoint, which is how two machines
    // are joined: the channel names where the frames go, not where they are
    // heard.
    let mut alice_client = Client::connect(&alice_dir.0).expect("a client for the first driver");
    let publication = alice_client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the first driver");

    let deadline = Instant::now() + TIMEOUT;
    let mut offered = false;
    while Instant::now() < deadline && !received.load(Ordering::Acquire) {
        offered |= alice_client.offer(publication, PAYLOAD).is_some();
        // The publisher's duty cycle too: `offer` writes into the term buffer
        // either way, but everything the publisher is *told* — its connection
        // state, an unavailable image — comes off the same ring.
        alice_client.poll();
        std::thread::sleep(Duration::from_millis(5));
    }

    // The subscriber thread is **not** joined: a `poll_subscription` waiting
    // for a fragment nobody sent does not return, so joining it would hang the
    // test rather than fail it. The deadline this test carries is the main
    // thread's, and the thread goes when the test process does.
    drop(subscriber);

    // What the subscriber saw, before the assertions judge it: a case that
    // fails with "no message" and no count says nothing about whether the
    // frames arrived at all.
    let seen = messages.load(Ordering::Relaxed);
    let first_payload = first.lock().expect("the first message").clone();
    assert!(offered, "the publication was offered at least once");
    if !received.load(Ordering::Acquire) {
        alice.stop();
        bob.stop();

        let alice_err = alice.stderr();
        let alice_out = alice.stdout();
        let bob_err = bob.stderr();
        let bob_out = bob.stdout();
        panic!(
            "the message did not cross between the two drivers.\n\
             === the sender: stdout\n{alice_out}\n=== the sender: stderr\n{alice_err}\n\
             === the receiver: stdout\n{bob_out}\n=== the receiver: stderr\n{bob_err}\n\
             the subscriber's callback saw {seen} messages, the first of them {:?}",
            String::from_utf8_lossy(&first_payload)
        );
    }

    // What the two drivers published about the frames, which is what says the
    // media moved them rather than the message arriving by some other route:
    // one driver's bytes sent, the other's bytes received. The design's 76 is
    // asserted below; 84 is not, and the module note says why.
    assert!(
        counter(&alice_cnc, id::BYTES_SENT) > 0,
        "the first driver sent no bytes through its port"
    );
    assert!(
        counter(&bob_cnc, id::BYTES_RECEIVED) > 0,
        "the second driver received no bytes through its port"
    );

    // The media's **own** counters, which is the half of §4.10 that needs a
    // real port: 75 is the link's state and 91 is what the pool has free, and
    // both are numbers only the device and the pool can answer. A driver whose
    // data paths are the kernel's has neither, so this is also what says the
    // media is where the frames went.
    //
    // Read as soon as they appear rather than after a fixed wait: the
    // conductor publishes them on the timer tier, whose first firing is the
    // pass after startup and whose next is an interval later.
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let up = |cnc: &CncFile| media_counter(cnc, type_id::PORT_INFO);
        if up(&alice_cnc) == Some(1) && up(&bob_cnc) == Some(1) {
            break;
        }

        assert!(
            Instant::now() < deadline,
            "the media's own counters were never published: the sender's link is {:?} and \
             the receiver's {:?}",
            up(&alice_cnc),
            up(&bob_cnc)
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    for (name, cnc) in [("the sender", &alice_cnc), ("the receiver", &bob_cnc)] {
        assert!(
            media_counter(cnc, type_id::MEMPOOL_AVAILABLE).is_some_and(|free| free > 0),
            "{name}'s pool has frames to give"
        );

        // 84 is read, not asserted to be non-zero: it counts what the **device**
        // dropped, which is zero in a healthy run and moves only when a port is
        // overrun (the D4 record's correction to the design's T4).
        assert!(
            media_counter(cnc, type_id::MISSED_PACKETS).is_some(),
            "{name}'s missed-packet counter is published"
        );
    }

    // 76, one per transport, written by the thread that reads the socket: the
    // receiver's transport took the frames off the port, and the sender's took
    // the `SETUP` that told it where to send. This is the counter the design
    // asks for by name — "one per transport, the value is what it received" —
    // and it is the only one here whose value a *thread* moves rather than a
    // timer.
    for (name, cnc) in [("the sender", &alice_cnc), ("the receiver", &bob_cnc)] {
        assert!(
            media_counter(cnc, type_id::TRANSPORT).is_some_and(|received| received > 0),
            "{name}'s transport counted the frames it received"
        );
    }

    // And one apiece: a second transport on either side would be a second
    // counter, which is what the type means.
    assert_eq!(
        1,
        media_counters(&alice_cnc, type_id::TRANSPORT).len(),
        "one transport for the publication"
    );
    assert_eq!(
        1,
        media_counters(&bob_cnc, type_id::TRANSPORT).len(),
        "one transport for the subscription"
    );

    // **And the receiver's checksum verdicts are clean.** 89 is read rather
    // than asserted non-zero, and read as `Some(0)` rather than as a
    // possibility: the counter must be there (the media publishes it), and a
    // healthy run of a media that computes its checksums has none of them.
    //
    // This is also where a wrong convention lands. A pseudo-header sum written
    // the other way round — complemented, the easy mistake — is a frame whose
    // checksum does not hold once the NIC has finished it; the receiving side
    // finds that out (its port judges the checksum, and the transport re-reads
    // it with software arithmetic) and counts it here.
    for (name, cnc) in [("the sender", &alice_cnc), ("the receiver", &bob_cnc)] {
        assert_eq!(
            Some(0),
            media_counter(cnc, type_id::CHECKSUM_FAILURE),
            "{name} read a frame whose checksum did not hold"
        );
    }

    assert!(
        alice.is_running() && bob.is_running(),
        "both drivers are still running; a media that took a process with it \
         would have shown up here"
    );

    // **Whose checksums these frames carry**, which is the one fact about this
    // media that no counter shows and no local reading can: the startup line
    // says it. A port that computes them takes the pseudo-header sum and
    // finishes it in hardware; one that does not takes both checksums written
    // out in full. The two produce frames that *look* alike from here and
    // differ on the wire, which is the whole reason this case exists — a
    // `net_pcap` port in the gate says `software` (see `dpdk_port.rs`), and a
    // real NIC says `nic` only if it was asked for what it can do.
    //
    // **Read after stopping them, and after the assertion above**: a pipe stays
    // open while its writer lives, so reading one on a running driver blocks
    // instead of answering — which is what made this case hang rather than fail
    // the first time it was written (`Driver::stop`'s note says the same thing
    // from the other side, and the order of these two blocks is the whole of
    // what keeps a failure a failure).
    alice.stop();
    bob.stop();

    for (name, out) in [
        ("the sender", alice.stdout()),
        ("the receiver", bob.stdout()),
    ] {
        assert!(
            out.contains("tx checksums nic"),
            "{name}'s port computes checksums, and its startup line says so; it said: {out}"
        );
    }
}
