//! The DPDK media's lifecycle and its queue map: open, run, and give
//! everything back (D4's `DpdkMedia`).
//!
//! What is being judged, and only the first is about DPDK at all:
//!
//! * **The close path's account.** A pool in a `OnceLock` is never dropped, so
//!   `Mempool::available` is the whole ledger of what a media took: the frames
//!   its receive queues filled themselves with, the frames a transport parked,
//!   and the ones a burst had not reached. After everything the media handed
//!   out is dropped, the pool is whole again — which is what catches a port
//!   that was not closed and a frame that leaked, the two defects a zero-copy
//!   path is expensive about.
//! * **What the driver tells the EAL.** The arguments are composed by `eal.rs`,
//!   whose cases run in the default gate; here the real ones are built from a
//!   `DpdkSettings` and handed to a real `rte_eal_init`, so a composition that
//!   is wrong only in the presence of an actual EAL fails here rather than
//!   there.
//! * **The queue map's edges.** One media per process; a device that cannot
//!   carry the pairs a threading mode wants, refused by name; and the ready
//!   ring full, which on this path is a frame going back to its pool rather
//!   than a `Vec` being dropped.
//!
//! Each case runs in a child process of this binary, because `rte_eal_init` is
//! once per process (D0).
//!
//! The port every case here uses is `net_pcap`, which has **one** receive queue
//! and one transmit queue — so the modes that put both agents on one thread
//! open, and `DEDICATED`, which wants three pairs, does not. That is a fact
//! about the CI path rather than about this port: every virtual device this
//! build's tests run on reports one queue of each, so the three-pair shape is
//! one only a real NIC is ever asked for.

#![cfg(feature = "dpdk")]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering;

use deepmsg_driver::config::{DpdkSettings, ThreadingMode};
use deepmsg_driver::media::userspace::dpdk::{DpdkError, DpdkMedia};
use deepmsg_driver::media::userspace::frame;
use deepmsg_driver::media::userspace::frame::{
    BROADCAST, IPV4_UDP_HEADER_LENGTH, Ipv4Write, UdpWrite,
};
use deepmsg_driver::media::userspace::packet_io::{Disposition, Offloads};
use deepmsg_driver::media::userspace::ports::Binding;
use deepmsg_driver::media::userspace::stack::{Clock, Stack, StackParams};

mod common;

use common::pcap::{Capture, in_a_child};

/// Where a child is told to find the capture it receives.
const RX_PCAP: &str = "DEEPMSG_DRIVER_DPDK_MEDIA_RX_PCAP";

/// The stack's own address, which the frames in a capture are addressed to.
const OWN: Ipv4Addr = Ipv4Addr::new(10, 10, 0, 1);

/// The port the frames are for, which the case that pumps binds.
const BOUND: u16 = 40500;

/// The settings every case here opens with: a `net_pcap` port reading the
/// capture the parent wrote, and the memory the EAL needs to start without
/// privileges.
///
/// `--no-huge` is the operator's own argument and stays one: the media adds
/// neither memory flag itself, it adds the one that pairs with what it was
/// given (`eal.rs`).
fn settings(capture: &str) -> DpdkSettings {
    DpdkSettings {
        eal_args: format!("--no-huge --vdev=net_pcap0,rx_pcap={capture}"),
        port: Some("net_pcap0".to_owned()),
        address: Some((OWN, 24)),
        gateway: Some(Ipv4Addr::new(10, 10, 0, 254)),
        ..DpdkSettings::default()
    }
}

/// The mode every case that only wants a port opens with.
///
/// `net_pcap` has one queue of each kind, and this is the map that wants one
/// pair — the two agents on one thread, sharing the one transmit queue.
const ONE_PAIR: ThreadingMode = ThreadingMode::Shared;

/// A frame of this file's own: an Ethernet destination and a payload that says
/// which frame it is.
fn a_frame(index: u8) -> Vec<u8> {
    let mut frame = vec![0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
    frame.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x02, 0x88, 0xb5]);
    frame.extend_from_slice(b"deepmsg-dpdk-media-frame-");
    frame.push(b'0' + index);
    frame.resize(60, 0);
    frame
}

/// A frame the stack can route: UDP to `OWN:BOUND`, carrying an Aeron `DATA`
/// frame, which is what a transport's ring is for.
///
/// The destination is the **broadcast** address because the port's own MAC
/// belongs to the device, and the device does not exist until the child starts
/// the EAL — which is after this capture has to be on disk. A broadcast frame
/// is one every port reads ([`frame::EthernetHeader::is_for`]), and the stack
/// routes it by its address and port like any other.
fn a_datagram(index: u8) -> Vec<u8> {
    let mut aeron = vec![0u8; 8 + 16];
    aeron[6..8].copy_from_slice(&1i16.to_le_bytes()); // frame type: DATA
    aeron[16] = index;

    let mut bytes = vec![0u8; IPV4_UDP_HEADER_LENGTH + aeron.len()];
    bytes[IPV4_UDP_HEADER_LENGTH..].copy_from_slice(&aeron);
    let written = frame::write_ipv4_udp(
        &mut bytes,
        &UdpWrite {
            ipv4: Ipv4Write {
                destination_hardware: BROADCAST,
                source_hardware: [0x02, 0, 0, 0, 0, 0x02],
                source: Ipv4Addr::new(10, 10, 0, 2),
                destination: OWN,
                identification: u16::from(index),
            },
            source_port: 40000,
            destination_port: BOUND,
            payload_length: aeron.len(),
        },
    )
    .expect("a frame");

    bytes.truncate(written);
    bytes
}

fn capture_of(name: &str, count: u8) -> Capture {
    let frames: Vec<Vec<u8>> = (0..count).map(a_frame).collect();
    Capture::of(name, &frames)
}

fn capture_of_datagrams(name: &str, count: u8) -> Capture {
    let frames: Vec<Vec<u8>> = (0..count).map(a_datagram).collect();
    Capture::of(name, &frames)
}

// ---- what a media takes, and what it gives back -------------------------

/// Everything a media hands out goes back to the pool when the media goes
/// away: the frames a transport parked included.
#[test]
fn a_media_gives_its_frames_back_when_it_closes() {
    let capture = capture_of("closes", 3);
    in_a_child(
        "a_media_gives_its_frames_back_when_it_closes_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_media_gives_its_frames_back_when_it_closes`"]
fn a_media_gives_its_frames_back_when_it_closes_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let media = DpdkMedia::open(&settings(&capture), ONE_PAIR).expect("the media opens");

    let pool = media.pool();
    let whole = media.pool_size();
    assert_eq!(
        whole,
        pool.available(),
        "a pool nothing has been taken from is whole"
    );

    // The port describes itself, and a virtual device judges no checksums —
    // the same answer the frames themselves give (the crate's own case).
    assert_eq!("net_pcap0", media.info().name);
    assert_eq!("net_pcap", media.info().driver);
    assert_eq!(Offloads::NONE, media.offloads());

    let mut ports = media.into_ports().expect("the mode's three roles");
    assert_eq!(
        6,
        ports.rx.mac().len(),
        "the receive port's MAC is the device's"
    );

    let binding = Binding {
        slot: 1,
        port: BOUND,
        generation: 1,
    };
    let mut parked = Vec::new();
    assert_eq!(
        3,
        ports
            .rx
            .rx_burst(3, &mut |_| Disposition::Keep(binding), &mut parked),
        "three frames were in the capture"
    );
    assert_eq!(
        whole - 3,
        pool.available(),
        "and three frames are out of the pool while a transport holds them"
    );

    drop(parked);
    drop(ports);

    assert_eq!(
        whole,
        pool.available(),
        "every frame and the receive queue's own descriptors are back: a pool \
         short here is a frame the close path forgot, or a port that was never \
         closed"
    );
}

// ---- one pair, two directions -------------------------------------------

/// Where a child is told to write what it sends.
const TX_PCAP: &str = "DEEPMSG_DRIVER_DPDK_MEDIA_TX_PCAP";

/// One queue pair carries both directions: the receive port reads its receive
/// queue and the transmit port writes its transmit queue, out of the pair the
/// machine gave a mode whose two agents share a thread.
///
/// The frame is read back out of the capture the PMD wrote, so what is being
/// asserted is that a transmit port built from the *shared* pair reaches the
/// device — a port that had quietly been given a second pair of its own would
/// pass every other case here and put its frames on a queue nobody reads.
#[test]
fn a_shared_pair_carries_frames_both_ways() {
    let capture = Capture::of("both-ways-rx", &[a_datagram(0)]);
    let sent = Capture::for_writing("both-ways-tx");
    in_a_child(
        "a_shared_pair_carries_frames_both_ways_in_the_child",
        &[(RX_PCAP, capture.path()), (TX_PCAP, sent.path())],
    );

    let written = common::pcap::read_capture(std::path::Path::new(sent.path()));
    assert_eq!(1, written.len(), "one frame left through the port");
    assert_eq!(
        a_frame(0),
        written[0],
        "and the bytes that left are the bytes that were written into it"
    );
}

#[test]
#[ignore = "runs as a child of `a_shared_pair_carries_frames_both_ways`"]
fn a_shared_pair_carries_frames_both_ways_in_the_child() {
    use deepmsg_driver::media::userspace::stack::TransmitQueues;

    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let sent = std::env::var(TX_PCAP).expect("and the one to write");

    let media = DpdkMedia::open(
        &DpdkSettings {
            eal_args: format!("--no-huge --vdev=net_pcap0,rx_pcap={capture},tx_pcap={sent}"),
            ..settings(&capture)
        },
        ONE_PAIR,
    )
    .expect("the media opens");

    let mut ports = media.into_ports().expect("the mode's three roles");
    let TransmitQueues::Shared(tx) = &mut ports.tx else {
        panic!("a mode with one pair shares its transmit queue between the two roles");
    };

    let expected = a_frame(0);
    assert_eq!(
        1,
        tx.tx_burst(1, &mut |room| {
            room[..expected.len()].copy_from_slice(&expected);
            expected.len()
        }),
        "the frame left through the pair the receive port came from"
    );

    let mut parked = Vec::new();
    assert_eq!(
        1,
        ports
            .rx
            .rx_burst(1, &mut |_| Disposition::Consumed, &mut parked),
        "and the receive queue of the same pair still hands frames over"
    );
}

// ---- the ready ring, on this path ---------------------------------------

/// A frame the ready ring has no room for is counted (88) and **goes straight
/// back to its pool** — which is the zero-copy path's own way of dropping one:
/// there is no buffer to free elsewhere, because there was never a buffer to
/// begin with.
///
/// This is the D4 half of the falsification D2 ran over the fake wire ("环容量
/// 设 1 + 打压", its §7.2): the ring is given room for two frames, eight
/// arrive, and the six the ring refuses are the pool's again by the time the
/// pump returns.
#[test]
fn a_frame_the_ring_has_no_room_for_goes_back_to_the_pool() {
    let capture = capture_of_datagrams("ring", 8);
    in_a_child(
        "a_frame_the_ring_has_no_room_for_goes_back_to_the_pool_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_frame_the_ring_has_no_room_for_goes_back_to_the_pool`"]
fn a_frame_the_ring_has_no_room_for_goes_back_to_the_pool_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let media = DpdkMedia::open(&settings(&capture), ONE_PAIR).expect("the media opens");
    let pool = media.pool();
    let whole = media.pool_size();

    let ports = media.into_ports().expect("the mode's three roles");
    // A window one datagram wide is a ring two frames deep
    // (`StackParams::ring_capacity`): the smallest one this stack can have.
    let params = StackParams {
        receive_window: 1,
        ..StackParams::new(OWN, 24, ports.rx.mac())
    };
    let capacity = params.ring_capacity();
    assert_eq!(2, capacity, "the smallest ring there is");

    let stack = Stack::new(params, Clock::Monotonic, ports.rx, ports.tx, None);
    let (_binding, ring) = stack
        .bind(Some(OWN), BOUND, None, SocketAddr::from((OWN, BOUND)))
        .expect("the port binds");

    assert_eq!(
        8,
        stack.pump(),
        "eight datagrams arrived through the PMD in one pass"
    );

    assert_eq!(
        capacity,
        ring.len(),
        "the ring holds what it has room for and not one more"
    );
    assert_eq!(
        8 - capacity,
        stack.counters().queue_drop.load(Ordering::Relaxed) as usize,
        "and every frame it had no room for is counted (88)"
    );
    assert_eq!(
        whole - capacity as u32,
        pool.available(),
        "exactly the frames in the ring are out of the pool: the ones 88 counted \
         are back in it already, because dropping one of these is dropping the \
         mbuf the port handed over"
    );

    // Two things hold the parked frames and both have to go: the stack, which
    // holds the ports, and this test's own handle on the ring the slot table
    // made.
    drop(stack);
    drop(ring);

    assert_eq!(
        whole,
        pool.available(),
        "and the ring's frames are back too, when the stack that held them goes"
    );
}

// ---- the EAL the driver started -----------------------------------------

/// The EAL knows about the process's own thread and nothing else.
///
/// This is the *consequence* of the argument `eal.rs` always adds, and the one
/// D0 measured: with no lcore set of its own the EAL starts a worker pinned to
/// every CPU the process may use — seven of them on this machine — and
/// `lcore_count` answers 8. With `--lcores=0@(<the process's own cpuset>)` it
/// answers 1, which is what a driver that pins its own threads needs and what
/// no argument-string check can see.
#[test]
fn the_eal_starts_no_workers_of_its_own() {
    let capture = capture_of("lcores", 0);
    in_a_child(
        "the_eal_starts_no_workers_of_its_own_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `the_eal_starts_no_workers_of_its_own`"]
fn the_eal_starts_no_workers_of_its_own_in_the_child() {
    use deepmsg_dpdk::Eal;
    use deepmsg_driver::media::userspace::dpdk::register_thread;

    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");

    // Before any media: there is no EAL in this process and nothing to
    // register with, which is what a driver on the kernel's sockets sees.
    assert!(
        register_thread().is_none(),
        "a process with no EAL has nothing to register a thread with"
    );

    let _media = DpdkMedia::open(&settings(&capture), ONE_PAIR).expect("the media opens");

    assert_eq!(
        1,
        Eal::lcore_count(),
        "one lcore is the process's own thread: a higher number is the EAL's \
         own workers, which is what a driver that pins its threads to CPUs \
         cannot have"
    );

    // The thread that started the EAL is its main lcore, so it has an id
    // already. A thread the driver starts does not — and that is the thread
    // `run_agent` registers, because without an id the pool's per-lcore cache
    // is not used and every allocation it makes goes to the shared ring (R7).
    assert!(
        Eal::lcore_id().is_some(),
        "the thread that initialised the EAL is its main lcore"
    );

    std::thread::spawn(|| {
        assert!(
            Eal::lcore_id().is_none(),
            "a fresh std::thread has no lcore id"
        );

        let registered = register_thread().expect("a media is open");
        assert!(
            Eal::lcore_id().is_some(),
            "and registering it is what gives it one"
        );

        drop(registered);
    })
    .join()
    .expect("the thread joins");
}

// ---- what a device that cannot carry the shape is told ------------------

/// §4.7's `DEDICATED` shape is one receive queue and two transmit queues, which
/// on a device that hands out queue pairs is three of each — and `net_pcap` has
/// one.
///
/// The refusal is the criterion: it names the port and both numbers, where
/// `rte_eth_dev_configure` would answer a code with no sentence attached. And
/// it is a fact about the CI path rather than about this port: every virtual
/// device this build's tests run on reports one queue of each, so the
/// three-pair shape is one only a real NIC is ever asked for.
#[test]
fn a_device_with_too_few_queues_is_refused_by_name() {
    let capture = capture_of("few", 0);
    in_a_child(
        "a_device_with_too_few_queues_is_refused_by_name_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_device_with_too_few_queues_is_refused_by_name`"]
fn a_device_with_too_few_queues_is_refused_by_name_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");

    let refused = DpdkMedia::open(&settings(&capture), ThreadingMode::Dedicated)
        .expect_err("net_pcap has one queue");
    assert!(matches!(refused, DpdkError::Capability { .. }));
    assert!(
        refused.to_string().contains("net_pcap0"),
        "the message names the port: {refused}"
    );
    assert!(
        refused.to_string().contains("1 receive and 1 transmit"),
        "and what the device has, beside what was asked: {refused}"
    );
}

// ---- one media per process ----------------------------------------------

/// A second media is refused rather than opened on the first one's pool.
#[test]
fn a_second_media_is_refused() {
    let capture = capture_of("second", 1);
    in_a_child(
        "a_second_media_is_refused_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `a_second_media_is_refused`"]
fn a_second_media_is_refused_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let media = DpdkMedia::open(&settings(&capture), ONE_PAIR).expect("the first one opens");
    let pool = media.pool();
    let whole = media.pool_size();

    let refused =
        DpdkMedia::open(&settings(&capture), ONE_PAIR).expect_err("the second one does not");
    assert!(matches!(refused, DpdkError::AlreadyOpen));
    assert!(
        refused.to_string().contains("already open"),
        "and says why: {refused}"
    );
    assert_eq!(
        whole,
        pool.available(),
        "a refusal costs the pool nothing — nothing was opened on the way"
    );

    // The first media is untouched by the refusal, which is the half that
    // matters: a refusal that had opened a port before noticing would leave a
    // configured device and no value to close it with.
    let mut ports = media
        .into_ports()
        .expect("the first media still has its roles");
    let mut parked = Vec::new();
    assert_eq!(
        1,
        ports
            .rx
            .rx_burst(1, &mut |_| Disposition::Consumed, &mut parked),
        "and still runs"
    );

    drop(ports);
    assert_eq!(whole, pool.available());
}

/// The stack the media builds carries the media's address, gateway and MAC —
/// and it is what holds the ports, so dropping it is what stops the port.
#[test]
fn the_media_hands_the_stack_its_own_settings() {
    let capture = capture_of("params", 0);
    in_a_child(
        "the_media_hands_the_stack_its_own_settings_in_the_child",
        &[(RX_PCAP, capture.path())],
    );
}

#[test]
#[ignore = "runs as a child of `the_media_hands_the_stack_its_own_settings`"]
fn the_media_hands_the_stack_its_own_settings_in_the_child() {
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let media = DpdkMedia::open(&settings(&capture), ONE_PAIR).expect("the media opens");
    let pool = media.pool();
    let whole = media.pool_size();

    let stack = media.into_stack(Clock::Monotonic).expect("a stack");
    let params = stack.params();

    assert_eq!(OWN, params.address);
    assert_eq!(24, params.prefix_length);
    assert_eq!(Some(Ipv4Addr::new(10, 10, 0, 254)), params.gateway);
    assert_ne!(
        [0; 6], params.mac,
        "the MAC is the device's — a port has an address to send from"
    );
    assert_eq!(
        deepmsg_driver::config::MTU_LENGTH_DEFAULT as usize,
        params.mtu,
        "the MTU is the Aeron one the channels are configured with, not the \
         frame size the port carries (that one is `RxPort::mtu`)"
    );
    assert_eq!(
        deepmsg_driver::config::RCV_INITIAL_WINDOW_LENGTH_DEFAULT as usize,
        params.receive_window
    );

    drop(stack);

    assert_eq!(
        whole,
        pool.available(),
        "dropping the stack is what closed the port and gave the frames back"
    );
}

/// A driver running on the media leaves **stderr empty**, which is a contract
/// and not a preference: the reference's own harness asserts the driver
/// process's stderr is zero bytes after every test method
/// (`aeron-test-support/src/main/java/io/aeron/test/MediaDriverTestUtil.java:128-131`),
/// and it launches this driver with the reference's own C harness. An EAL's
/// five startup lines are enough to fail every one of those tests for a reason
/// that has nothing to do with them, which is what `--log-level=1` is for
/// (`eal.rs::LOG_LEVEL`).
///
/// **And the media line has to be there**, because an empty stderr is also
/// what a driver that never opened a media produces: without
/// `AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA` in its environment this driver
/// runs on the kernel's sockets with no EAL at all, and this case would pass
/// while proving nothing. It is the same trap T4 fell into, and the same fix —
/// assert the thing that says the media is where the process is.
///
/// `net_null` and `--no-huge`, so this needs no privilege and no device: the
/// question is what the driver *says*, not what its port does.
#[test]
fn a_driver_on_the_media_writes_nothing_to_stderr() {
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-dpdk-stderr-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock")
            .as_nanos()
    ));

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_deepmsg-driver"))
        .arg(format!("-Ddeepmsg.dir={}", dir.display()))
        .arg("-Daeron.threading.mode=SHARED")
        .arg("-Ddeepmsg.dpdk.eal.args=--no-huge --vdev=net_null0")
        .arg("-Ddeepmsg.dpdk.port=net_null0")
        .arg("-Ddeepmsg.dpdk.ipv4.address=10.10.0.9/24")
        .env("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA", "dpdk")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the driver binary is built alongside this test");

    // The CnC file is published after the media is open, so waiting for it is
    // waiting for the EAL to have started and said whatever it was going to
    // say.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !dir.join(deepmsg_cnc::CNC_FILE_NAME).exists() {
        assert!(
            child.try_wait().expect("a status").is_none(),
            "the driver exited before it published its CnC file"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "no CnC file in {} within 20 s",
            dir.display()
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    let _ = child.kill();
    let output = child.wait_with_output().expect("the driver's output");
    let _ = std::fs::remove_dir_all(&dir);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stdout.contains("dpdk media on net_null0"),
        "the media is where this process runs, which is what makes the \
         assertion below worth making:\n{stdout}"
    );
    assert_eq!(
        "", stderr,
        "an EAL and a PMD write to stderr, and the reference's harness reads \
         it:\n{stderr}"
    );
}
