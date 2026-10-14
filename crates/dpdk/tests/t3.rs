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

    fn path(&self) -> &str {
        self.0.to_str().expect("a UTF-8 path")
    }
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

// ---- the pieces the cases above share ---------------------------------

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
        Self::start(name, args)
    }

    fn start(name: &str, args: Vec<&str>) -> Self {
        Eal::init(args).expect("EAL starts");

        let pool = Mempool::create(
            "t3_pool",
            &MempoolSettings {
                count: 1023,
                cache: 64,
                ..MempoolSettings::default()
            },
        )
        .expect("a pool");

        let (port, pairs) = Port::start(
            name,
            &PortSettings {
                rx_queues: 1,
                tx_queues: 0,
                rx_descriptors: 128,
                ..PortSettings::default()
            },
            &pool,
        )
        .expect("the port starts");

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
