//! The DPDK media's lifecycle: open, run, and give everything back (D4's
//! `DpdkMedia`).
//!
//! Three things are being judged, and only the first is about DPDK at all.
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
//! * **One media per process.** The EAL and the pool are process-wide and
//!   neither can be made twice, so a second open says so instead of quietly
//!   configuring a port on the first one's pool.
//!
//! Each case runs in a child process of this binary, because `rte_eal_init` is
//! once per process (D0).
//!
//! The port every case here uses is `net_pcap`, which has **one** receive queue
//! and one transmit queue — so a media opened with one pair, and what §4.7's
//! "one receive queue and two transmit queues" means for a device that cannot
//! carry it, is a criterion of its own below.

#![cfg(feature = "dpdk")]

use std::net::Ipv4Addr;

use deepmsg_driver::config::DpdkSettings;
use deepmsg_driver::media::userspace::dpdk::{DpdkError, DpdkMedia};
use deepmsg_driver::media::userspace::dpdk_port::DpdkRxPort;
use deepmsg_driver::media::userspace::packet_io::{Disposition, Offloads, RxPort};
use deepmsg_driver::media::userspace::ports::Binding;

mod common;

use common::pcap::{Capture, in_a_child};

/// Where a child is told to find the capture it receives.
const RX_PCAP: &str = "DEEPMSG_DRIVER_DPDK_MEDIA_RX_PCAP";

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
        address: Some((Ipv4Addr::new(10, 10, 0, 1), 24)),
        gateway: Some(Ipv4Addr::new(10, 10, 0, 254)),
        ..DpdkSettings::default()
    }
}

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

fn capture_of(name: &str, count: u8) -> Capture {
    let frames: Vec<Vec<u8>> = (0..count).map(a_frame).collect();
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
    let mut media = DpdkMedia::open(&settings(&capture), 1).expect("the media opens");

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

    let pair = media.take_pair().expect("the pair it was opened with");
    assert_eq!(0, media.pairs_left(), "and there was one of them");

    let binding = Binding {
        slot: 1,
        port: 40500,
        generation: 1,
    };
    let mut port = DpdkRxPort::new(pair, pool);
    let mut parked = Vec::new();
    assert_eq!(
        3,
        port.rx_burst(3, &mut |_| Disposition::Keep(binding), &mut parked),
        "three frames were in the capture"
    );
    assert_eq!(
        whole - 3,
        pool.available(),
        "and three frames are out of the pool while a transport holds them"
    );

    drop(parked);
    drop(port);
    drop(media);

    assert_eq!(
        whole,
        pool.available(),
        "every frame and the receive queue's own descriptors are back: a pool \
         short here is a frame the close path forgot, or a port that was never \
         closed"
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
    let capture = std::env::var(RX_PCAP).expect("the parent passes the capture's path");
    let _media = DpdkMedia::open(&settings(&capture), 1).expect("the media opens");

    assert_eq!(
        1,
        deepmsg_dpdk::Eal::lcore_count(),
        "one lcore is the process's own thread: a higher number is the EAL's \
         own workers, which is what a driver that pins its threads to CPUs \
         cannot have"
    );
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

    let refused = DpdkMedia::open(&settings(&capture), 3).expect_err("net_pcap has one queue");
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
    let mut media = DpdkMedia::open(&settings(&capture), 1).expect("the first one opens");
    let pool = media.pool();
    let whole = media.pool_size();

    let refused = DpdkMedia::open(&settings(&capture), 1).expect_err("the second one does not");
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
    let pair = media
        .take_pair()
        .expect("the first media still has its pair");
    let mut port = DpdkRxPort::new(pair, pool);
    let mut parked = Vec::new();
    assert_eq!(
        1,
        port.rx_burst(1, &mut |_| Disposition::Consumed, &mut parked),
        "and still runs"
    );

    drop(port);
    drop(media);
    assert_eq!(whole, pool.available());
}
