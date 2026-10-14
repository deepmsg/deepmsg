//! The DPDK port, driven through a real PMD (D4's `DpdkPort`).
//!
//! What is being tested is the seam and not the crate underneath it: that the
//! frames the userspace stack's own traits hand over are the frames a DPDK
//! port received, byte for byte, and that they cost no copy on the way — a
//! kept frame leaves the burst as the mbuf the port owns, and it is the pool's
//! accounting that says so.
//!
//! The frames come from `net_pcap`, which reads a capture this file writes and
//! writes one this file reads — the same zero-privilege PMD the crate's own T3
//! uses, with real descriptors and real mbufs and no root, no veth and no huge
//! pages. The case that needs those is the crate's `#[ignore]`d one.
//!
//! Every case that wants an EAL runs in a child process of this same binary,
//! because `rte_eal_init` is once per process and `rte_eal_cleanup` does not
//! undo it (D0). The child is re-entered with `--ignored --exact <helper>`, and
//! a filtered run exits 0 with nothing run — so the parent insists on seeing
//! its child report exactly one passing test.

#![cfg(feature = "dpdk")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use deepmsg_dpdk::{Eal, Mempool, MempoolSettings, Port, PortSettings, QueuePair};
use deepmsg_driver::media::userspace::dpdk_port::{DpdkRxPort, DpdkTxPort};
use deepmsg_driver::media::userspace::frame::{Checksums, ETHERNET_HEADER_LENGTH};
use deepmsg_driver::media::userspace::packet_io::{
    Disposition, Offloads, ReadyFrame, RxPort, TxPort,
};
use deepmsg_driver::media::userspace::ports::Binding;

/// The EAL arguments every case here starts with.
///
/// `--no-huge` because no case here needs huge pages for a virtual device, and
/// `--no-shconf` because it implies the legacy memory layout that `--in-memory`
/// is incompatible with (D0 measured the pair). `--no-telemetry` keeps EAL's
/// telemetry thread out of the process.
const EAL_ARGS: [&str; 4] = [
    "deepmsg-driver-dpdk-port",
    "--no-huge",
    "--no-shconf",
    "--no-telemetry",
];

/// Where a child is told to find the capture it receives.
const RX_PCAP: &str = "DEEPMSG_DRIVER_DPDK_RX_PCAP";

/// Where a child is told to write what it sends.
const TX_PCAP: &str = "DEEPMSG_DRIVER_DPDK_TX_PCAP";

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

/// A frame the parent writes and the child expects: an Ethernet destination of
/// this file's own and a payload that says which frame it is.
fn a_frame(index: u8) -> Vec<u8> {
    let mut frame = vec![0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
    frame.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x02, 0x88, 0xb5]);
    frame.extend_from_slice(b"deepmsg-dpdk-port-frame-");
    frame.push(b'0' + index);
    frame.resize(60, 0);
    frame
}

/// Write frames as a libpcap capture: the 24-byte file header, then one record
/// header and the frame per packet. `net_pcap` reads exactly this shape and
/// its `tx_pcap` writes it back.
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
            "deepmsg-driver-dpdk-port-{}-{name}.pcap",
            std::process::id()
        ));
        write_capture(&path, frames);
        Self(path)
    }

    /// A path with nothing at it: what a `tx_pcap` port is told to write to.
    fn for_writing(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "deepmsg-driver-dpdk-port-{}-{name}.pcap",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
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

// ---- the receive path ---------------------------------------------------

/// A frame written into a capture arrives through a real PMD as a parked mbuf,
/// byte for byte, and the pool it came from says so.
#[test]
fn a_frame_arrives_as_a_parked_mbuf() {
    let frames: Vec<Vec<u8>> = (0..3).map(a_frame).collect();
    let capture = Capture::of("received", &frames);
    in_a_child(
        "a_frame_arrives_as_a_parked_mbuf_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

/// The body of the case above.
#[test]
#[ignore = "runs as a child of `a_frame_arrives_as_a_parked_mbuf`"]
fn a_frame_arrives_as_a_parked_mbuf_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let expected: Vec<Vec<u8>> = (0..3).map(a_frame).collect();

    let mut started = Started::net_pcap("net_pcap0", &capture);
    let before = started.pool.available();
    let mut port = DpdkRxPort::new(started.pair(), started.pool);

    // What the port says it judges, against what its frames say it judged:
    // `net_pcap` has no checksum offload (D0), so every frame it hands over is
    // the stack's to verify.
    assert_eq!(Offloads::NONE, port.offloads());

    let binding = Binding {
        slot: 7,
        port: 40500,
        generation: 3,
    };
    let mut kept = Vec::new();
    let handed = port.rx_burst(
        3,
        &mut |frame| {
            assert_eq!(
                Checksums::Software,
                frame.checksums,
                "a port that judges nothing does not vouch for a frame"
            );
            Disposition::Keep(binding)
        },
        &mut kept,
    );

    assert_eq!(3, handed, "three frames were on the wire");
    assert_eq!(3, kept.len(), "and the reader kept all three");
    for (index, (routed, frame)) in kept.iter().enumerate() {
        assert_eq!(
            &binding, routed,
            "each frame carries the binding it was routed to"
        );
        assert!(
            matches!(frame, ReadyFrame::Mbuf(_)),
            "a kept frame is still the port's own mbuf, not a copy"
        );
        assert_eq!(
            expected[index].as_slice(),
            frame.as_bytes(),
            "frame {index} arrived byte for byte"
        );
    }

    assert_eq!(
        before - 3,
        started.pool.available(),
        "three frames are out of the pool while they are parked"
    );
    kept.clear();
    assert_eq!(
        before,
        started.pool.available(),
        "and every one of them is back when its transport lets it go"
    );
}

/// A frame the reader consumes is released inside the burst, so a pass that
/// keeps nothing costs nothing.
#[test]
fn a_frame_the_reader_does_not_want_goes_back_to_the_pool() {
    let frames: Vec<Vec<u8>> = (0..4).map(a_frame).collect();
    let capture = Capture::of("consumed", &frames);
    in_a_child(
        "a_frame_the_reader_does_not_want_goes_back_to_the_pool_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_frame_the_reader_does_not_want_goes_back_to_the_pool`"]
fn a_frame_the_reader_does_not_want_goes_back_to_the_pool_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let mut started = Started::net_pcap("net_pcap0", &capture);
    let before = started.pool.available();
    let mut port = DpdkRxPort::new(started.pair(), started.pool);

    let mut kept = Vec::new();
    let handed = port.rx_burst(2, &mut |_| Disposition::Consumed, &mut kept);

    assert_eq!(2, handed, "two of the four were asked for");
    assert!(kept.is_empty(), "and none was kept");
    assert_eq!(
        before,
        started.pool.available(),
        "a released frame is back in its pool before the burst returns"
    );
}

// ---- the transmit path --------------------------------------------------

/// A frame the stack builds into the room the port allocated leaves through it,
/// byte for byte — read back out of the capture the PMD wrote.
#[test]
fn a_frame_leaves_through_the_port_it_was_built_into() {
    let capture = Capture::for_writing("sent");
    in_a_child(
        "a_frame_leaves_through_the_port_it_was_built_into_in_the_child",
        &[(TX_PCAP, capture.path())],
    );

    let written = read_capture(Path::new(capture.path()));
    assert_eq!(1, written.len(), "one frame left through the port");
    assert_eq!(
        a_frame(0),
        written[0],
        "the bytes that left are the bytes that were written into the frame"
    );
}

#[test]
#[ignore = "runs as a child of `a_frame_leaves_through_the_port_it_was_built_into`"]
fn a_frame_leaves_through_the_port_it_was_built_into_in_the_child() {
    let capture = std::env::var(TX_PCAP).expect("the parent passes the capture's path");
    let expected = a_frame(0);

    let mut started = Started::net_pcap_tx("net_pcap0", &capture);
    let before = started.pool.available();
    let mut port = DpdkTxPort::new(started.pair(), started.pool);

    // The room the port hands the sender is the frame's own headroom — the
    // buffer the stack writes its headers and payload straight into, which is
    // the copy this path does not pay.
    let sent = port.tx_burst(1, &mut |room| {
        assert!(
            room.len() >= expected.len(),
            "the room is a whole frame: {} bytes for a {}-byte frame",
            room.len(),
            expected.len()
        );
        room[..expected.len()].copy_from_slice(&expected);
        expected.len()
    });

    assert_eq!(1, sent, "the frame left");
    assert_eq!(
        before,
        started.pool.available(),
        "the frame the NIC took is the pool's no longer, and nothing else was lost"
    );
}

// ---- what the port says about itself ------------------------------------

/// The port's MAC and MTU are the device's, and its MTU counts the Ethernet
/// header — the number a frame builder fits into.
#[test]
fn a_port_describes_itself_from_the_device() {
    let empty = Capture::of("described", &[]);
    in_a_child(
        "a_port_describes_itself_from_the_device_in_the_child",
        &[(RX_PCAP, empty.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_port_describes_itself_from_the_device`"]
fn a_port_describes_itself_from_the_device_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let mut started = Started::net_pcap("net_pcap0", &capture);
    let device_mtu = usize::from(started.port.mtu());
    let port = DpdkRxPort::new(started.pair(), started.pool);

    assert_eq!(
        started.port.mac(),
        port.mac(),
        "the MAC every frame carries is the device's"
    );
    assert_eq!(
        device_mtu + ETHERNET_HEADER_LENGTH,
        port.mtu(),
        "the largest frame is the device's MTU with the Ethernet header counted"
    );
}

// ---- the pieces the cases above share -----------------------------------

/// A started port, its pool, and the queue pair the port is driven over.
///
/// The fields are in the order they have to go away in: the queue first — the
/// queue holds the port alive, and the last one dropping is what stops the
/// device — and the pool never, because it is leaked.
///
/// Leaking is how this test says what the driver will say with a
/// `OnceLock<Mempool>`: a DPDK port's frames borrow the pool they came from, so
/// a port whose pool is process-lived hands out frames with no borrow of the
/// port in them. One port per process is the design's own rule (§4.8), and a
/// child process is one.
struct Started {
    pair: Option<QueuePair>,
    port: Arc<Port>,
    pool: &'static Mempool,
}

impl Started {
    /// A `net_pcap` port reading from `capture`.
    fn net_pcap(name: &str, capture: &str) -> Self {
        let vdev = format!("--vdev={name},rx_pcap={capture}");
        Self::start(name, &vdev)
    }

    /// A `net_pcap` port writing to `capture`: nothing arrives, everything
    /// sent lands in the file.
    fn net_pcap_tx(name: &str, capture: &str) -> Self {
        let vdev = format!("--vdev={name},tx_pcap={capture}");
        Self::start(name, &vdev)
    }

    fn start(name: &str, vdev: &str) -> Self {
        let mut args = EAL_ARGS.to_vec();
        args.push(vdev);
        Eal::init(args).expect("EAL starts");

        let pool = Box::leak(Box::new(
            Mempool::create(
                "dpdk_port_pool",
                &MempoolSettings {
                    count: 1023,
                    cache: 64,
                    ..MempoolSettings::default()
                },
            )
            .expect("a pool"),
        ));

        let settings = PortSettings {
            rx_queues: 1,
            tx_queues: 1,
            rx_descriptors: 128,
            tx_descriptors: 128,
            ..PortSettings::default()
        };
        let (port, pairs) = Port::start(name, &settings, pool).expect("the port starts");

        Self {
            pair: pairs.into_iter().next(),
            port,
            pool,
        }
    }

    /// The queue pair, handed to the port under test.
    fn pair(&mut self) -> QueuePair {
        self.pair.take().expect("a started port has a queue pair")
    }
}
