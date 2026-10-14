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
//! counters 76 and 84 to be non-zero. The second half cannot be met yet and
//! the test does not pretend otherwise: 76 is one per transport and nothing
//! allocates it, and 84 is one of the media's own counters, which are built
//! (`dpdk_counters.rs`) but not yet owned by anything that samples them. What
//! is asserted instead is what today's driver does publish and what says the
//! same thing about the frames: the **bytes sent by one driver and received by
//! the other**, out of the CnC file each of them wrote.

#![cfg(feature = "dpdk")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::{CncFile, CounterRegions};
use deepmsg_driver::system_counters::id;

/// The two ends of the pair, named by whoever made it.
const VETH_A: &str = "DEEPMSG_DPDK_T4_VETH_A";
const VETH_B: &str = "DEEPMSG_DPDK_T4_VETH_B";

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
const TIMEOUT: Duration = Duration::from_secs(30);

/// How long a freshly spawned driver is given to publish its CnC file.
const READY_TIMEOUT: Duration = Duration::from_secs(20);

/// A driver subprocess in a directory of its own, killed if it outlives the
/// test.
struct Driver {
    child: Child,
    dir: PathBuf,
}

impl Driver {
    /// Start a driver whose data paths are on `interface`, holding `address`.
    ///
    /// `SHARED` and not the default `DEDICATED`: a threading mode is a map of
    /// queues to threads, `DEDICATED` wants three queue pairs, and `net_af_packet`
    /// has one of each — the same wall every virtual device in this build has
    /// met (the D4 record's "虚拟设备只有一条队列"). One thread runs both
    /// agents here, which is all a message crossing needs.
    fn start(dir: &Path, interface: &str, address: &str) -> Self {
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
            .arg(format!(
                "-Ddeepmsg.dpdk.eal.args=--no-huge --vdev=net_af_packet0,iface={interface}"
            ))
            .arg("-Ddeepmsg.dpdk.port=net_af_packet0")
            .arg(format!("-Ddeepmsg.dpdk.ipv4.address={address}"))
            .stdout(Stdio::null())
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
                panic!(
                    "the driver in {dir} exited before publishing a CnC file; its stderr was:\n{}",
                    self.stderr()
                );
            }

            if Instant::now() >= deadline {
                let dir = self.dir.display().to_string();
                panic!(
                    "no CnC file for pid {pid} in {dir} within {READY_TIMEOUT:?}; its stderr \
                     so far:\n{}",
                    self.stderr(),
                );
            }
            std::thread::sleep(Duration::from_millis(16));
        }
    }

    fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
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

/// One driver's counter, read the way a client reads it.
fn counter(cnc: &CncFile, counter_id: i32) -> i64 {
    let regions: CounterRegions<'_> = cnc.counter_regions().expect("a mapped CnC file");
    regions
        .reader()
        .value(counter_id)
        .unwrap_or_else(|| panic!("counter {counter_id} is allocated"))
}

/// A message crosses from one machine's driver to the other's, over frames the
/// two of them put on a wire themselves.
///
/// Needs a veth pair and root, so it is `#[ignore]`d; run it by hand:
///
/// ```text
/// sudo ip link add deepmsg-a type veth peer name deepmsg-b
/// sudo ip link set deepmsg-a up && sudo ip link set deepmsg-b up
/// sudo -E env PATH="$PATH" RUSTUP_HOME=~/.rustup CARGO_HOME=~/.cargo \
///     DEEPMSG_DPDK_T4_VETH_A=deepmsg-a DEEPMSG_DPDK_T4_VETH_B=deepmsg-b \
///     cargo test -p deepmsg-driver --features dpdk -- --ignored --nocapture dpdk_two_drivers
/// ```
///
/// **No addresses on either end**: the kernels must not be the ones answering
/// ARP, or the message would prove nothing about the two stacks.
#[test]
#[ignore = "needs a veth pair and CAP_NET_RAW: set DEEPMSG_DPDK_T4_VETH_A/_B and run under sudo"]
fn a_message_crosses_between_two_drivers_on_a_dpdk_port() {
    let (interface_a, interface_b) = match (std::env::var(VETH_A), std::env::var(VETH_B)) {
        (Ok(a), Ok(b)) => (a, b),
        _ => panic!(
            "set {VETH_A} and {VETH_B} to the two ends of a veth pair:\n  \
             sudo ip link add deepmsg-a type veth peer name deepmsg-b\n  \
             sudo ip link set deepmsg-a up\n  sudo ip link set deepmsg-b up"
        ),
    };

    let alice_dir = TempDir::new("alice");
    let bob_dir = TempDir::new("bob");

    // One at a time, and not for tidiness. Two `net_af_packet` ports coming up
    // in the same instant race in the PMD's own fanout setup and one of them
    // loses with `could not set PACKET_FANOUT on AF_PACKET socket …: Invalid
    // argument` — a failure of the *second* EAL to probe its device, before
    // either driver has done anything (the D4 record has the diagnosis). The
    // second driver waits for the first to publish its CnC file, which is
    // after its port is up.
    let mut alice = Driver::start(&alice_dir.0, &interface_a, ALICE);
    let alice_cnc = alice.await_cnc();
    let mut bob = Driver::start(&bob_dir.0, &interface_b, BOB);
    let bob_cnc = bob.await_cnc();

    // Bob subscribes first: his subscription is what binds the endpoint Alice
    // will send to, and the two drivers are separate processes, so there is no
    // shared loop to order them in.
    let bound = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicBool::new(false));

    let subscriber = {
        let dir = bob_dir.0.clone();
        let bound = Arc::clone(&bound);
        let received = Arc::clone(&received);

        std::thread::spawn(move || {
            let mut client = Client::connect(&dir).expect("a client for the second driver");
            let subscription = client
                .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
                .expect("a subscription on the second driver");
            bound.store(true, Ordering::Release);

            let deadline = Instant::now() + TIMEOUT;
            while Instant::now() < deadline && !received.load(Ordering::Acquire) {
                let _ = client.poll_subscription(subscription, 10, |message| {
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
    assert!(
        bound.load(Ordering::Acquire),
        "the second driver never bound the endpoint the first one sends to; \
         its stderr was:\n{}",
        bob.stderr()
    );

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
        std::thread::sleep(Duration::from_millis(5));
    }

    // The subscriber thread is **not** joined: a `poll_subscription` waiting
    // for a fragment nobody sent does not return, so joining it would hang the
    // test rather than fail it. The deadline this test carries is the main
    // thread's, and the thread goes when the test process does.
    drop(subscriber);

    assert!(offered, "the publication was offered at least once");
    assert!(
        received.load(Ordering::Acquire),
        "the message did not cross between the two drivers; the first driver's \
         stderr was:\n{}",
        alice.stderr()
    );

    // What the two drivers published about the frames, which is what says the
    // media moved them rather than the message arriving by some other route:
    // one driver's bytes sent, the other's bytes received. The design's own
    // 76/84 pair cannot be checked yet — see the module note.
    assert!(
        counter(&alice_cnc, id::BYTES_SENT) > 0,
        "the first driver sent no bytes through its port"
    );
    assert!(
        counter(&bob_cnc, id::BYTES_RECEIVED) > 0,
        "the second driver received no bytes through its port"
    );

    assert!(
        alice.is_running() && bob.is_running(),
        "both drivers are still running; a media that took a process with it \
         would have shown up here"
    );
}
