//! The library half of `aeron.threading.mode`: a driver the caller drives.
//!
//! `INVOKER` is the mode that gives the driver no runner threads at all — the
//! reference builds the same single runner for it as for `SHARED`
//! (`aeron_driver.c:1003-1022`, one `case` arm for both) — and what makes it a
//! *working* driver is a caller with a manual main loop
//! (`aeron_driver_main_do_work`, `:1252-1261`). This file is that loop, over a
//! client of this build's, with both ends in one process: it is the one place
//! where [`Driver::do_work`] is called by something other than [`Driver::run`].
//!
//! The process half — which threads each mode starts, and what it names them —
//! is `tests/integration/driver_process_contract.rs`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_core::clock;
use deepmsg_driver::config::{DriverConfig, ThreadingMode};
use deepmsg_driver::dir;
use deepmsg_driver::driver::Driver;

/// The stream the round trip happens on.
const STREAM_ID: i32 = 2001;

/// How long the pair is given to finish the exchange.
const TIMEOUT: Duration = Duration::from_secs(30);

/// A directory of our own in the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-embedded-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Publish one message and read it back, through a client of ours.
fn round_trip(dir: PathBuf) -> bool {
    let mut client = Client::connect(&dir).expect("connect to the driver this process owns");

    let publication = client
        .add_publication("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an IPC publication");
    let subscription = client
        .add_subscription("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an IPC subscription");

    let payload = b"an embedded driver answers its caller";
    let deadline = Instant::now() + TIMEOUT;
    let mut received = None;

    while received.is_none() && Instant::now() < deadline {
        let _ = client.offer(publication, payload);
        client.poll();
        client.poll_subscription(subscription, 10, |message| {
            received = Some(message.payload.to_vec());
        });
    }

    received.as_deref() == Some(payload.as_slice())
}

/// A driver configured `INVOKER` answers a client while its caller drives it.
///
/// The client runs on a thread of its own because its calls **wait** for the
/// driver's replies: one thread would deadlock, and the thing under test here
/// is the driver's pass, not the client's patience. The driver's loop is the
/// reference's (`aeron_driver_main_do_work`, `aeron_driver.c:1252-1261`), which
/// under `INVOKER` is a pass of all four pieces — sender, receiver, native
/// resource agent, conductor (`:723-734`).
#[test]
fn an_invoker_driver_is_driven_by_its_caller() {
    let temp = TempDir::new();

    let config = DriverConfig {
        aeron_dir: temp.0.clone(),
        threading_mode: ThreadingMode::Invoker,
        ..DriverConfig::default()
    };

    // The directory tree first — `publications/` and `images/` are where the
    // agents map log buffers into, and a CnC file on its own is a driver that
    // cannot serve a publication. This is the binary's own order
    // (`bin/deepmsg-driver.rs`): settle the directory, then take the file over.
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

    let mut driver = Driver::new(
        cnc,
        &config,
        deepmsg_driver::cpuset::CpuAssignment::default(),
    )
    .expect("the driver takes the file over");

    let client = std::thread::spawn({
        let dir = config.aeron_dir.clone();
        move || round_trip(dir)
    });

    let deadline = Instant::now() + TIMEOUT;
    while !client.is_finished() && Instant::now() < deadline {
        driver.do_work();
        std::thread::sleep(Duration::from_micros(200));
    }

    assert!(
        client.join().expect("the client thread"),
        "the message came back"
    );

    driver.close().expect("the driver closes");
    assert!(!driver.is_running(), "and it is closed");

    prepared.remove().expect("and the directory goes with it");
}
