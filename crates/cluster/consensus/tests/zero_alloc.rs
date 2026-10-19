//! R1 (performance spec §2): a single-member cluster's consensus module does
//! **not** call the allocator in steady state.
//!
//! The counting allocator is in this test binary and nowhere else, which is why
//! this is its own file: a `#[global_allocator]` is per binary, and a counter in
//! the shared one would count every other test's setup.
//!
//! The reading is the CM thread's own count — `alloc`, `alloc_zeroed` and
//! `realloc` — taken by a thread-local in the allocator. Setup (launch,
//! recovery, session open) allocates by design (performance spec §2, the
//! "allowed exceptions"), so the count is reset after the warm-up: the run is
//! 10 000 messages to settle every lazily grown buffer, then a reset, then
//! 100 000 messages, and the assertion is **zero**.
//!
//! What this covers on a single member: segment 2 (ingress poll →
//! `appendMessage`) and segment 8 (leader position / commit). Segments 9 and 10
//! are the container's, and the container is `P3-2c`; when it lands this binary
//! grows the second thread and counts both. `P3-6` extends the topology to
//! three members, where segments 6 and 7 appear.
//!
//! Runs against the workspace's own archiving media driver, spawned as a
//! process — the same arrangement every other cluster integration test uses.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig};
use deepmsg_cluster_common::config::Properties;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};

// -- the counting allocator -------------------------------------------------

thread_local! {
    /// One thread's allocation count. `const` so reading it cannot itself
    /// allocate before the counter exists.
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

impl Counting {
    fn count_one() {
        // `try_with`: an allocation during thread teardown (or before this
        // thread's TLS exists) must not panic inside the allocator.
        let _ = ALLOCATIONS.try_with(|cell| cell.set(cell.get() + 1));
    }
}

// SAFETY: every method forwards to `System`, which is the allocator this
// wrapper replaces; the only added work is a thread-local counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::count_one();
        // SAFETY: `System.alloc` has the same contract as this method's, and
        // the layout is the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::count_one();
        // SAFETY: as in `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::count_one();
        // SAFETY: as in `alloc`; the pointer and sizes are the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as in `alloc`; the pointer and layout are the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

fn reset_allocations() {
    ALLOCATIONS.with(|cell| cell.set(0));
}

// -- the harness ------------------------------------------------------------

const DEADLINE: Duration = Duration::from_secs(60);
const INGRESS_STREAM_ID: i32 = 101;
const EGRESS_STREAM_ID: i32 = 102;
const CLUSTER_ID: i32 = 7;
const WARM_UP_MESSAGES: usize = 10_000;
const MEASURED_MESSAGES: usize = 100_000;

/// 0 = warm-up, 1 = reset and collect, 2 = stop and report.
static PHASE: AtomicI32 = AtomicI32::new(0);
static RESET_DONE: AtomicBool = AtomicBool::new(false);

struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("the temp directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The workspace's archiving media driver, beside this test binary.
fn archiving_media_driver() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("DEEPMSG_ARCHIVING_MEDIA_DRIVER") {
        return Some(PathBuf::from(path));
    }

    let candidate = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../target/debug/deepmsg-archiving-media-driver");
    candidate.is_file().then_some(candidate)
}

struct DriverProcess {
    child: Child,
    aeron_dir: TempDir,
    archive_dir: TempDir,
}

impl DriverProcess {
    fn start() -> Option<Self> {
        let binary = archiving_media_driver()?;
        let aeron_dir = TempDir::new("cluster-zero-alloc-aeron");
        let archive_dir = TempDir::new("cluster-zero-alloc-archive");

        let child = Command::new(binary)
            .arg(format!("-Daeron.dir={}", aeron_dir.path().display()))
            .arg(format!(
                "-Daeron.archive.dir={}",
                archive_dir.path().display()
            ))
            .arg("-Daeron.threading.mode=SHARED")
            .arg("-Daeron.archive.threading.mode=SHARED")
            .arg("-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0")
            .arg("-Daeron.print.configuration=false")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        let driver = Self {
            child,
            aeron_dir,
            archive_dir,
        };
        driver.await_ready();
        Some(driver)
    }

    fn await_ready(&self) {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let cnc_ready = deepmsg_cnc::CncFile::try_open(self.aeron_dir.path()).is_ok();
            let archive_ready =
                deepmsg_archive::mark_file::ArchiveMarkFile::open(self.archive_dir.path())
                    .ok()
                    .is_some_and(|mark| mark.version().is_some());

            if cnc_ready && archive_ready {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the archiving media driver never became ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn aeron_dir(&self) -> &Path {
        self.aeron_dir.path()
    }
}

impl Drop for DriverProcess {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn properties(aeron_dir: &Path, cluster_dir: &Path) -> Properties {
    let mut properties = Properties::new();
    properties
        .apply_args([
            format!("-Daeron.dir={}", aeron_dir.display()),
            format!("-Daeron.cluster.dir={}", cluster_dir.display()),
            format!("-Daeron.cluster.id={CLUSTER_ID}"),
            "-Daeron.cluster.member.id=0".to_owned(),
            "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0"
                .to_owned(),
            "-Daeron.cluster.ingress.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.consensus.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.log.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k".to_owned(),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            "-Daeron.cluster.service.count=0".to_owned(),
        ])
        .expect("the properties parse");
    properties
}

fn connect_request() -> Vec<u8> {
    let mut buffer = vec![0_u8; 256];
    let encoder = SessionConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(1)
        .response_stream_id(EGRESS_STREAM_ID)
        .version(3 << 8)
        .response_channel(b"aeron:ipc")
        .encoded_credentials(b"")
        .client_info(b"zero-alloc");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

fn session_message(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
    let mut buffer = vec![0_u8; 64];
    let encoder = SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(1);
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

fn offer(client: &Client, registration_id: i64, bytes: &[u8]) -> bool {
    matches!(
        client.offer(registration_id, bytes),
        Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
    )
}

/// Open one session and answer its id from the OK event.
fn open_session(
    agent: &mut ConsensusModuleAgent,
    client: &mut Client,
    ingress_publication: i64,
    egress_subscription: i64,
) -> i64 {
    let connect = connect_request();
    let deadline = Instant::now() + DEADLINE;
    let mut session_id = 0;
    let mut sent = false;
    while Instant::now() < deadline && session_id == 0 {
        agent.do_work().expect("the duty cycle is healthy");
        client.poll();
        if !sent && offer(client, ingress_publication, &connect) {
            sent = true;
        }
        if sent {
            client.poll_subscription(egress_subscription, 10, |message| {
                let payload = message.payload;
                if payload.len() < MESSAGE_HEADER_LENGTH {
                    return;
                }
                let header =
                    deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder::default()
                        .wrap(ReadBuf::new(payload), 0);
                if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
                    || header.template_id()
                        != deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID
                {
                    return;
                }
                let decoder = SessionEventDecoder::default().header(header, 0);
                if decoder.code() == EventCode::OK {
                    session_id = decoder.cluster_session_id();
                }
            });
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_ne!(0, session_id, "the session did not open");
    session_id
}

#[test]
fn a_single_member_consensus_module_does_not_allocate_in_steady_state() {
    let Some(driver) = DriverProcess::start() else {
        eprintln!("SKIPPED: not verified -- no deepmsg archiving media driver binary");
        return;
    };

    let cluster_dir = TempDir::new("cluster-zero-alloc-cluster");
    let mut config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration parses");
    // This build has no class loader; the flag is the programmatic
    // extension seam a zero-service cluster needs (`ConsensusModule.java:2158`).
    config.consensus_module_extension = true;

    let mut client = Client::connect(driver.aeron_dir()).expect("the test client connects");
    let ingress_publication = client
        .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
        .expect("ingress publication");
    let egress_subscription = client
        .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
        .expect("egress subscription");

    let mut agent = ConsensusModuleAgent::launch(config).expect("the module launches");
    let session_id = open_session(
        &mut agent,
        &mut client,
        ingress_publication,
        egress_subscription,
    );
    let message = session_message(agent.leadership_term_id(), session_id);

    // The sender has its own client: the module is not `Send` (its timer
    // service, authenticator and authorisation service are trait objects), so
    // the thread that owns it stays the thread that launched it, and the
    // measured messages come from the other side.
    let sender = std::thread::spawn(move || {
        let mut client = Client::connect(driver.aeron_dir()).expect("the sender's client connects");
        let publication = client
            .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
            .expect("the sender's ingress publication");

        send(&mut client, publication, &message, WARM_UP_MESSAGES);

        PHASE.store(1, Ordering::Release);
        let deadline = Instant::now() + DEADLINE;
        while !RESET_DONE.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "the CM thread never reset the counter"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        send(&mut client, publication, &message, MEASURED_MESSAGES);
        PHASE.store(2, Ordering::Release);
    });

    // The CM's duty cycle on this thread, and the thread-local count read here.
    let deadline = Instant::now() + DEADLINE;
    let mut was_collecting = false;
    loop {
        if !was_collecting && PHASE.load(Ordering::Acquire) == 1 {
            // Between two duty cycles, so the reset cannot land mid-cycle.
            reset_allocations();
            RESET_DONE.store(true, Ordering::Release);
            was_collecting = true;
        }
        if PHASE.load(Ordering::Acquire) == 2 {
            break;
        }
        agent.do_work().expect("the duty cycle is healthy");
        assert!(Instant::now() < deadline, "the measured run never finished");
    }

    // Drain what the sender left in the ingress publication: the last offers
    // return before the module has polled them, and the cost of applying them
    // is part of the window.
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        agent.do_work().expect("the duty cycle is healthy");
        std::thread::sleep(Duration::from_millis(1));
    }

    sender.join().expect("the sender thread joins");
    let count = allocations();
    assert_eq!(
        0, count,
        "the consensus module allocated {count} times over {MEASURED_MESSAGES} steady-state messages"
    );

    let _ = agent.close();
}

/// Offer `count` messages, polling the client while the publication is full.
fn send(client: &mut Client, registration_id: i64, message: &[u8], count: usize) {
    for _ in 0..count {
        let deadline = Instant::now() + DEADLINE;
        while !offer(client, registration_id, message) {
            assert!(
                Instant::now() < deadline,
                "the ingress publication stayed full"
            );
            client.poll();
            std::thread::yield_now();
        }
    }
}
