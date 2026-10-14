//! T3: the crate's own criteria (ADR-0006, slice D3).
//!
//! Every case that wants an EAL runs in a **process of its own**, because
//! `rte_eal_init` is once per process and `rte_eal_cleanup` does not undo it
//! (D0). The child is this test binary itself — re-entered with
//! `--ignored --exact <helper>` — and not a binary under `target/debug`,
//! because `cargo test` does not rebuild those: the same trap D1 hit, where a
//! stale binary makes a criterion pass while testing something else.
//!
//! The frames come from `net_pcap`, which reads a capture this file writes and
//! writes one this file reads: a real PMD, real descriptors, real mbufs, and
//! **no privileges** — no veth pair, no `CAP_NET_RAW`, no huge pages. The one
//! case that needs those (`net_af_packet` over a veth) is `#[ignore]`d and runs
//! on the bench machine.
//!
//! A filtered test binary exits 0 with nothing run, so the parent insists on
//! seeing its child report exactly one passing test. A case that quietly stops
//! running is the one failure mode this file must not have.

#![cfg(feature = "dpdk")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use deepmsg_dpdk::{
    Checksum, Disposition, Eal, FrameRing, Mempool, MempoolSettings, ParkedFrame, Port,
    PortSettings, QueuePair, RegisteredThread, restore_affinity, save_affinity,
};

/// The EAL arguments every case here starts with.
///
/// `--no-huge` because no machine in this slice's CI needs huge pages for a
/// virtual device, and `--no-shconf` because `--no-huge` implies the legacy
/// memory layout, which is incompatible with `--in-memory` (D0 measured both).
/// `--no-telemetry` keeps EAL's telemetry thread out of the process.
const EAL_ARGS: [&str; 4] = [
    "deepmsg-dpdk-test",
    "--no-huge",
    "--no-shconf",
    "--no-telemetry",
];

/// Where a child is told to find the capture it receives.
const RX_PCAP: &str = "DEEPMSG_DPDK_T3_RX_PCAP";

/// Run one case in a child process of this same test binary.
fn in_a_child(helper: &str, env: &[(&str, &str)]) {
    let exe = std::env::current_exe().expect("the test binary's own path");
    let mut command = Command::new(&exe);
    command.args([
        "--exact",
        helper,
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]);
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("running {}: {error}", exe.display()));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "the child case `{helper}` failed ({}):\n{stdout}\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains("1 passed"),
        "the child case `{helper}` did not run. A filtered test binary exits 0 \
         with nothing run, so this is the one way a criterion could pass by not \
         being looked at:\n{stdout}\n{stderr}"
    );
}

/// A frame the parent writes and the child expects: 60 bytes on the wire, a
/// broadcast destination, an ethertype of this file's own, and a payload that
/// says which frame it is.
fn a_frame(index: u8) -> Vec<u8> {
    let mut frame = vec![0xff; 6];
    frame.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 0x88, 0xb5]);
    frame.extend_from_slice(b"deepmsg-dpdk-t3-frame-");
    frame.push(b'0' + index);
    frame.resize(60, 0);
    frame
}

/// Write frames as a libpcap capture: the 24-byte file header, then one record
/// header and the frame per packet. `net_pcap` reads exactly this, and its
/// `tx_pcap` writes the same shape back.
fn write_capture(path: &Path, frames: &[Vec<u8>]) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0xa1b2_c3d4u32.to_le_bytes()); // magic
    bytes.extend_from_slice(&2u16.to_le_bytes()); // major
    bytes.extend_from_slice(&4u16.to_le_bytes()); // minor
    bytes.extend_from_slice(&0u32.to_le_bytes()); // thiszone
    bytes.extend_from_slice(&0u32.to_le_bytes()); // sigfigs
    bytes.extend_from_slice(&65535u32.to_le_bytes()); // snaplen
    bytes.extend_from_slice(&1u32.to_le_bytes()); // link type: Ethernet

    for frame in frames {
        bytes.extend_from_slice(&0u32.to_le_bytes()); // ts_sec
        bytes.extend_from_slice(&0u32.to_le_bytes()); // ts_usec
        bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes()); // incl_len
        bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes()); // orig_len
        bytes.extend_from_slice(frame);
    }

    std::fs::write(path, bytes).expect("writing a capture");
}

/// A file of this test's own, removed when the test that made it returns.
struct Capture(PathBuf);

impl Capture {
    fn of(name: &str, frames: &[Vec<u8>]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "deepmsg-dpdk-t3-{}-{name}.pcap",
            std::process::id()
        ));
        write_capture(&path, frames);
        Self(path)
    }

    /// A path with nothing at it: what a `tx_pcap` port is told to write to.
    fn for_writing(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "deepmsg-dpdk-t3-{}-{name}.pcap",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &str {
        self.0.to_str().expect("a UTF-8 path")
    }
}

/// Read a capture back: the frames in it, in order.
fn read_capture(path: &Path) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).unwrap_or_else(|error| {
        panic!("reading {}: {error}", path.display());
    });
    assert!(bytes.len() >= 24, "a capture has a file header");

    let mut frames = Vec::new();
    let mut cursor = 24; // the file header
    while cursor + 16 <= bytes.len() {
        let lengths = &bytes[cursor..cursor + 16];
        let incl_len = u32::from_le_bytes(lengths[8..12].try_into().expect("four bytes"));
        cursor += 16;
        let end = cursor + incl_len as usize;
        assert!(end <= bytes.len(), "a record's frame is in the file");
        frames.push(bytes[cursor..end].to_vec());
        cursor = end;
    }

    frames
}

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// The `--vdev` argument that gives this case a port reading from `path`.
fn net_pcap(name: &str, capture: &str) -> String {
    format!("--vdev={name},rx_pcap={capture}")
}

// ---- the EAL of the process -------------------------------------------

/// EAL starts once, and the process's second `init` is the first EAL.
#[test]
fn eal_init_is_once_per_process() {
    in_a_child("eal_init_is_once_per_process_in_the_child", &[]);
}

/// The body of the case above; not itself a criterion when run here.
#[test]
#[ignore = "runs as a child of `eal_init_is_once_per_process`: one EAL per process"]
fn eal_init_is_once_per_process_in_the_child() {
    let eal = Eal::init(EAL_ARGS).expect("EAL starts");

    let again = Eal::init(EAL_ARGS).expect("a second init answers the first");
    assert!(
        std::ptr::eq(eal, again),
        "the second `init` must answer the EAL the first one started, not start another"
    );

    // A `std::thread` EAL never started: no lcore id until it registers, and
    // the id it gets is one of EAL's (R7 — the id is what turns the pool's
    // per-lcore cache on for that thread).
    std::thread::spawn(|| {
        assert_eq!(Eal::lcore_id(), None, "a fresh std::thread has no lcore id");
        let registered = RegisteredThread::register().expect("a std::thread can register");
        let id = registered.lcore_id();
        assert_eq!(
            Eal::lcore_id(),
            Some(id),
            "the registered thread knows its lcore"
        );
        assert!(
            id < Eal::lcore_count(),
            "lcore {id} is one of EAL's {} lcores",
            Eal::lcore_count()
        );
    })
    .join()
    .expect("the thread joins");

    save_affinity().expect("the affinity can be saved");
    restore_affinity().expect("and put back");

    Eal::cleanup().expect("EAL shuts down");

    // Cleanup is final: there is no second EAL in this process.
    let refused = Eal::init(EAL_ARGS).expect_err("after cleanup, init must be refused");
    assert!(
        refused.to_string().contains("cleaned up"),
        "the refusal says why: {refused}"
    );
}

// ---- the receive path --------------------------------------------------

/// A frame written into a capture arrives through a real PMD, byte for byte.
#[test]
fn a_prepared_frame_arrives_through_the_pmd() {
    let frame = a_frame(0);
    let capture = Capture::of("one", std::slice::from_ref(&frame));
    in_a_child(
        "a_prepared_frame_arrives_through_the_pmd_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

/// The body of the case above.
#[test]
#[ignore = "runs as a child of `a_prepared_frame_arrives_through_the_pmd`"]
fn a_prepared_frame_arrives_through_the_pmd_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let expected = a_frame(0);

    let mut port = Started::net_pcap("net_pcap0", &capture);
    let before = port.pool.available();

    let mut kept = Vec::new();
    let received = receive(&port.pool, &mut port.pairs[0], &mut kept, 1);
    assert_eq!(received, 1, "the prepared frame arrives");
    assert_eq!(kept.len(), 1, "and the callback kept it");

    assert_eq!(
        kept[0].bytes(),
        expected.as_slice(),
        "the bytes that arrive are the bytes that were written"
    );
    assert_eq!(
        port.pool.available(),
        before - 1,
        "a parked frame is one the pool no longer has"
    );

    // And it goes back when it is dropped — the accounting that catches a
    // frame the data path forgot about.
    kept.clear();
    assert_eq!(
        port.pool.available(),
        before,
        "a dropped frame is back in its pool"
    );
}

/// Frames that came off a port cross a ring and come back to their pool.
#[test]
fn frames_cross_a_ring_and_come_back_to_their_pool() {
    let frames: Vec<Vec<u8>> = (0..8).map(a_frame).collect();
    let capture = Capture::of("eight", &frames);
    in_a_child(
        "frames_cross_a_ring_and_come_back_to_their_pool_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

/// The body of the case above: the ring's halves go to two threads, which is
/// what a single producer and a single consumer mean.
#[test]
#[ignore = "runs as a child of `frames_cross_a_ring_and_come_back_to_their_pool`"]
fn frames_cross_a_ring_and_come_back_to_their_pool_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");

    let mut port = Started::net_pcap("net_pcap0", &capture);
    let before = port.pool.available();

    let mut arrived = Vec::new();
    assert_eq!(
        receive(&port.pool, &mut port.pairs[0], &mut arrived, 8),
        8,
        "eight frames arrive"
    );
    assert_eq!(port.pool.available(), before - 8);

    let ring = FrameRing::create("t3_ring", 64, &port.pool).expect("a ring");
    let (mut producer, mut consumer) = ring.split();

    // The producer half goes to its own thread: this is the arrangement the
    // driver's receive path uses to hand frames to another thread's transports.
    let sent = std::thread::scope(|scope| {
        scope
            .spawn(move || producer.enqueue(&mut arrived))
            .join()
            .expect("the producer thread")
    });
    assert_eq!(sent, 8, "the ring took every frame");

    let mut crossed = Vec::new();
    assert_eq!(
        consumer.dequeue(&mut crossed, 8),
        8,
        "and gave every frame back"
    );
    for (index, frame) in crossed.iter().enumerate() {
        assert_eq!(
            frame.bytes(),
            a_frame(index as u8).as_slice(),
            "frame {index} crossed the ring unchanged"
        );
    }

    drop(crossed);
    drop(consumer);
    assert_eq!(
        port.pool.available(),
        before,
        "every frame that crossed the ring is back in its pool"
    );
}

/// A wire with nothing on it answers zero, and costs no frames.
///
/// The empty case is a capture with no records rather than the null device:
/// `net_null` *generates* frames, which this case found out the first time it
/// ran.
#[test]
fn an_empty_wire_costs_nothing() {
    let empty = Capture::of("empty", &[]);
    in_a_child(
        "an_empty_wire_costs_nothing_in_the_child",
        &[(RX_PCAP, empty.path())],
    );
}

#[test]
#[ignore = "runs as a child of `an_empty_wire_costs_nothing`"]
fn an_empty_wire_costs_nothing_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let mut port = Started::net_pcap("net_pcap0", &capture);
    let before = port.pool.available();

    let mut kept = Vec::new();
    assert_eq!(
        receive(&port.pool, &mut port.pairs[0], &mut kept, 32),
        0,
        "nothing is on the wire"
    );
    assert!(kept.is_empty());
    assert_eq!(
        port.pool.available(),
        before,
        "an empty burst costs no frames"
    );

    // A burst of zero frames is answered, not attempted.
    assert_eq!(
        port.pairs[0].rx_burst(&port.pool, 0, &mut |_| Disposition::Release, &mut kept),
        0
    );
}

/// A burst answers what it was asked for and no more, and the frames the
/// callback released are back in the pool before it returns.
#[test]
fn a_burst_answers_what_it_was_asked_for() {
    let frames: Vec<Vec<u8>> = (0..8).map(a_frame).collect();
    let full = Capture::of("full", &frames);
    in_a_child(
        "a_burst_answers_what_it_was_asked_for_in_the_child",
        &[(RX_PCAP, full.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_burst_answers_what_it_was_asked_for`"]
fn a_burst_answers_what_it_was_asked_for_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let mut port = Started::net_pcap("net_pcap0", &capture);
    let before = port.pool.available();

    let received = port.pairs[0].rx_burst(
        &port.pool,
        3,
        &mut |_| Disposition::Release,
        &mut Vec::new(),
    );
    assert_eq!(
        received, 3,
        "eight frames are on the wire and three were asked for"
    );
    assert_eq!(
        port.pool.available(),
        before,
        "released frames are back in the pool before the burst returns"
    );
}

// ---- what the port says, and what a refusal says ------------------------

/// The port describes itself: its name, its PMD, its queues, its MAC and its
/// MTU — the surface D4's configuration checks read.
#[test]
fn a_port_describes_itself() {
    let empty = Capture::of("described", &[]);
    in_a_child(
        "a_port_describes_itself_in_the_child",
        &[(RX_PCAP, empty.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_port_describes_itself`"]
fn a_port_describes_itself_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let port = Started::net_pcap("net_pcap0", &capture);

    assert!(
        Port::names().iter().any(|name| name == "net_pcap0"),
        "the port answers to the name it was given: {:?}",
        Port::names()
    );

    let info = port._port.info();
    assert_eq!(info.name, "net_pcap0");
    assert_eq!(info.driver, "net_pcap");
    assert!(info.max_rx_queues >= 1 && info.max_tx_queues >= 1);
    assert_eq!(
        info.socket_id, None,
        "a port that reports -1 has no socket, and -1 is not a socket number (D0)"
    );

    assert_eq!(port._port.mac().len(), 6, "a port has an address");
    assert!(port._port.mtu() > 0, "and reports an MTU");

    // What the port says it can judge, against what its frames say it judged:
    // the case above receives frames from this port and every one of them
    // answers `NotJudged` (D0 — the virtual devices have no checksum offload).
    // A capability read out of `rx_offload_capa` that disagreed with the frames
    // would be a driver asking a port for something it cannot do.
    assert!(
        !info.can_judge_ipv4_checksum() && !info.can_judge_udp_checksum(),
        "net_pcap judges no checksums, and reports none: {:#x}",
        info.rx_offloads
    );
}

/// A failure carries DPDK's own words, which is what makes an error worth
/// reading at three in the morning.
#[test]
fn a_refusal_says_what_dpdk_said() {
    let empty = Capture::of("refused", &[]);
    in_a_child(
        "a_refusal_says_what_dpdk_said_in_the_child",
        &[(RX_PCAP, empty.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_refusal_says_what_dpdk_said`"]
fn a_refusal_says_what_dpdk_said_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");

    let mut args = EAL_ARGS.to_vec();
    let vdev = net_pcap("net_pcap0", &capture);
    args.push(&vdev);
    Eal::init(args).expect("EAL starts");

    // A pool of four billion frames: DPDK cannot make it, and says why.
    let refused = Mempool::create(
        "t3_absurd",
        &MempoolSettings {
            count: u32::MAX,
            cache: 0,
            ..MempoolSettings::default()
        },
    )
    .expect_err("a pool that cannot be made is an error");

    assert_ne!(
        refused.code, 0,
        "the error carries DPDK's errno, not this crate's refusal: {refused}"
    );
    assert!(
        refused
            .to_string()
            .contains("the mempool could not be made"),
        "and says which call answered it: {refused}"
    );

    let pool = Mempool::create("t3_pool", &MempoolSettings::default()).expect("a pool");

    // More receive queues than the PMD has: the call answers a code without
    // setting an errno, and the code is what the error has to carry.
    let refused = Port::start(
        "net_pcap0",
        &PortSettings {
            rx_queues: 2,
            tx_queues: 0,
            ..PortSettings::default()
        },
        &pool,
    )
    .expect_err("net_pcap has one receive queue");
    assert_ne!(
        refused.code, 0,
        "the error carries the call's code: {refused}"
    );
    assert!(
        refused
            .to_string()
            .contains("the port refused that configuration"),
        "and names the call that answered: {refused}"
    );

    // A port that does not exist: this crate's own refusal, with no code,
    // because nothing was called.
    let refused =
        Port::start("no-such-port", &carrying(), &pool).expect_err("there is no such port");
    assert_eq!(refused.code, 0, "nothing was called: {refused}");
    assert!(
        refused.to_string().contains("no-such-port"),
        "and it names the port: {refused}"
    );
}

// ---- the transmit path -------------------------------------------------

/// Where a child is told to write what it sends.
const TX_PCAP: &str = "DEEPMSG_DPDK_T3_TX_PCAP";

/// A frame handed to the port leaves through it, byte for byte — read back out
/// of the capture the PMD wrote.
#[test]
fn a_transmitted_frame_leaves_through_the_pmd() {
    let capture = Capture::for_writing("tx");
    in_a_child(
        "a_transmitted_frame_leaves_through_the_pmd_in_the_child",
        &[(TX_PCAP, capture.path())],
    );

    let written = read_capture(Path::new(capture.path()));
    assert_eq!(written.len(), 1, "one frame left through the port");
    assert_eq!(
        written[0],
        a_frame(0),
        "the bytes that left are the bytes that were written into the frame"
    );
}

/// The body of the case above.
#[test]
#[ignore = "runs as a child of `a_transmitted_frame_leaves_through_the_pmd`"]
fn a_transmitted_frame_leaves_through_the_pmd_in_the_child() {
    let capture = std::env::var(TX_PCAP).expect("the parent passes the capture's path");
    let expected = a_frame(0);
    let mut port = Started::net_pcap_tx("net_pcap0", &capture);
    let before = port.pool.available();

    let sent = port.pairs[0].tx_burst(&port.pool, 1, &mut |room| {
        room[..expected.len()].copy_from_slice(&expected);
        expected.len()
    });

    assert_eq!(sent, 1, "the frame left");
    assert_eq!(
        port.pool.available(),
        before,
        "the frame the NIC took is no longer the pool's, and nothing else was lost"
    );
}

/// The ports a frame and a queue pair have to cross.
///
/// `QueuePair`, `ParkedFrame` and the ring's halves are `Send`: they are made
/// on one thread and used on another. They are also not `Sync`, which is the
/// other half of the same invariant — and that half cannot be asserted here,
/// because proving the *absence* of a trait needs a compile-fail harness this
/// workspace has no room for. What enforces it is the type: `PhantomData<*mut
/// ()>` in `QueuePair` and in both ring halves.
#[test]
fn what_crosses_threads_can() {
    fn assert_send<T: Send>() {}

    assert_send::<QueuePair>();
    assert_send::<ParkedFrame<'static>>();
    assert_send::<deepmsg_dpdk::Producer<'static>>();
    assert_send::<deepmsg_dpdk::Consumer<'static>>();
    assert_send::<Mempool>();
    assert_send::<Port>();
}

// ---- the case that needs privileges ------------------------------------

/// The two ends of a veth pair, for the case below.
const VETH_A: &str = "DEEPMSG_DPDK_T3_VETH_A";
const VETH_B: &str = "DEEPMSG_DPDK_T3_VETH_B";

/// A frame crosses a real kernel interface, through `net_af_packet`.
///
/// `#[ignore]`d, and the only case here that is: it needs a veth pair and
/// `CAP_NET_RAW`, which is to say root. T3's criteria do not rest on it — the
/// frame path is judged on `net_pcap` above, without privileges — so this is
/// the bench machine's case, run by hand:
///
/// ```text
/// sudo ip link add deepmsg-a type veth peer name deepmsg-b
/// sudo ip link set deepmsg-a up && sudo ip link set deepmsg-b up
/// sudo -E env PATH="$PATH" RUSTUP_HOME=~/.rustup CARGO_HOME=~/.cargo \
///     cargo test -p deepmsg-dpdk --features dpdk -- --ignored af_packet
/// ```
#[test]
#[ignore = "needs a veth pair and CAP_NET_RAW: set DEEPMSG_DPDK_T3_VETH_A/_B and run under sudo"]
fn af_packet_carries_a_frame_through_a_veth() {
    let (a, b) = match (std::env::var(VETH_A), std::env::var(VETH_B)) {
        (Ok(a), Ok(b)) => (a, b),
        _ => panic!(
            "set {VETH_A} and {VETH_B} to the two ends of a veth pair:\n  \
             sudo ip link add deepmsg-a type veth peer name deepmsg-b\n  \
             sudo ip link set deepmsg-a up\n  sudo ip link set deepmsg-b up"
        ),
    };
    let _ = (&a, &b);

    let expected = a_frame(0);
    let args = [
        EAL_ARGS[0],
        EAL_ARGS[1],
        EAL_ARGS[2],
        EAL_ARGS[3],
        &format!("--vdev=net_af_packet0,iface={a}"),
        &format!("--vdev=net_af_packet1,iface={b}"),
    ];

    let mut sending = Started::start(&args, "net_af_packet0", carrying());
    let mut receiving = Started::start(&args, "net_af_packet1", carrying());

    let sent = sending.pairs[0].tx_burst(&sending.pool, 1, &mut |room| {
        room[..expected.len()].copy_from_slice(&expected);
        expected.len()
    });
    assert_eq!(sent, 1, "the frame left the first interface");

    let mut arrived = Vec::new();
    let received = receive(&receiving.pool, &mut receiving.pairs[0], &mut arrived, 1);
    assert_eq!(received, 1, "and arrived on the other one");
    assert_eq!(
        arrived[0].bytes(),
        expected.as_slice(),
        "the bytes that crossed the veth are the bytes that were sent"
    );
}

// ---- the pieces the cases above share ---------------------------------

/// A port that only receives: one receive queue, and nothing to send on.
fn reading() -> PortSettings {
    PortSettings {
        rx_queues: 1,
        tx_queues: 0,
        rx_descriptors: 128,
        ..PortSettings::default()
    }
}

/// A port with one queue pair, which is what a case that sends needs.
fn carrying() -> PortSettings {
    PortSettings {
        rx_queues: 1,
        tx_queues: 1,
        rx_descriptors: 128,
        tx_descriptors: 128,
        ..PortSettings::default()
    }
}

/// A started port, its pool, and its queue pairs: what a child case needs
/// before it can receive anything.
///
/// The fields are in the order they have to go away in. The queues first — the
/// last of them dropping is what stops and closes the port — then the port's
/// own handle, and only then the pool the receive queues were set up with.
struct Started {
    pairs: Vec<QueuePair>,
    _port: std::sync::Arc<Port>,
    pool: Mempool,
}

impl Started {
    /// A `net_pcap` port reading from `capture`.
    fn net_pcap(name: &str, capture: &str) -> Self {
        let mut args = EAL_ARGS.to_vec();
        let vdev = net_pcap(name, capture);
        args.push(&vdev);
        Self::start(&args, name, reading())
    }

    /// A `net_pcap` port writing to `capture`: nothing arrives, everything
    /// sent lands in the file.
    fn net_pcap_tx(name: &str, capture: &str) -> Self {
        let mut args = EAL_ARGS.to_vec();
        let vdev = format!("--vdev={name},tx_pcap={capture}");
        args.push(&vdev);
        Self::start(&args, name, carrying())
    }

    /// Start the named port on an EAL already given `args`.
    ///
    /// A case with two ports calls this twice: the EAL's `init` answers the
    /// first call and every later one, so the devices both ports need are in
    /// the first argument list.
    fn start(args: &[&str], name: &str, settings: PortSettings) -> Self {
        Eal::init(args).expect("EAL starts");

        let pool = Mempool::create(
            &format!("t3_{name}_pool"),
            &MempoolSettings {
                count: 1023,
                cache: 64,
                ..MempoolSettings::default()
            },
        )
        .expect("a pool");

        let (port, pairs) = Port::start(name, &settings, &pool).expect("the port starts");

        Self {
            pairs,
            _port: port,
            pool,
        }
    }
}

/// Receive up to `want` frames, keeping each one, until the deadline.
///
/// A free function rather than a method on [`Started`]: the frames it keeps
/// borrow the pool, and a method would have to borrow the whole struct — the
/// queues included — for as long as they live.
fn receive<'pool>(
    pool: &'pool Mempool,
    pair: &mut QueuePair,
    kept: &mut Vec<ParkedFrame<'pool>>,
    want: usize,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(5);

    while kept.len() < want && Instant::now() < deadline {
        pair.rx_burst(
            pool,
            want - kept.len(),
            &mut |frame| {
                assert_eq!(
                    frame.checksum(),
                    Checksum::NotJudged,
                    "a virtual device judges no checksums"
                );
                Disposition::Keep
            },
            kept,
        );
        std::thread::sleep(Duration::from_micros(200));
    }

    kept.len()
}
