//! G0-1: the process contract the reference's own system tests assume.
//!
//! The acceptance criterion for the whole of G0 is that this driver can stand
//! in for `aeronmd` under the reference's own OSS system tests. That harness is
//! the reference's `CTestMediaDriver`, which the tests select with
//! `-Daeron.test.system.aeronmd.path=<binary>`; nothing on the Java side is
//! replaced, so **everything the harness assumes about the process is a
//! contract this driver has to keep**. This file is that contract, one test per
//! clause, written against our own binary so it needs no reference checkout and
//! runs in CI.
//!
//! The clauses, and where each is decided:
//!
//! 1. **A configuration arrives as environment variables**, not as `-D`
//!    arguments — `CTestMediaDriver.java:218-260` turns a `MediaDriver.Context`
//!    into `pb.environment()` entries (`:332-334`). [`READ`] and [`IGNORED`]
//!    together are every name it can send.
//! 2. **A name this driver has never heard of must not stop it.** The
//!    deployment this driver is meant to slot into carries settings it has no
//!    use for, and a driver that refused to start over one would be worse than
//!    useless. Forty of the harness's names are in that position today.
//! 3. **stderr is empty on a clean run.** Every test not annotated
//!    `@IgnoreStdErr` asserts the driver's stderr file is zero bytes long
//!    (`MediaDriverTestUtil.java:128-132`, from `:95-99`), so one diagnostic
//!    line fails every test at once.
//! 4. **`TERMINATE_DRIVER` stops it promptly and cleanly.** The harness writes
//!    that command from a process that never registered as a client
//!    (`CommonContext.java:1140-1165`, token `null` and length 0) and then waits
//!    ten seconds (`CTestMediaDriver.java:593`) before falling back to
//!    `destroyForcibly()` (`:599`). The fallback is not a failure — the exit
//!    code is recorded and never asserted (`MediaDriverTestUtil.java:148`) —
//!    but it costs ten seconds per test and throws away the only evidence that
//!    the driver shut down rather than was killed.
//!
//! What is deliberately *not* here: `AERON_THREADING_MODE`. The harness sends
//! it and 58 of the 82 tests set `SHARED`, which this driver does not have. It
//! is in [`IGNORED`] like the rest, and the consequence — the baseline runs
//! under `DEDICATED` and is therefore a weaker signal than it looks — belongs
//! with the baseline, not here.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::CncFile;
use deepmsg_cnc::command::{TERMINATE_DRIVER_TYPE_ID, TerminateDriver};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver;
use deepmsg_tests::temp::TempDir;

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// How long a driver is given to publish its CnC file.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a driver is given to stop once the command has been written.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// The budget the harness gives a driver before it kills it
/// (`CTestMediaDriver.java:593` is a `waitFor(10, SECONDS)`).
///
/// Not a limit this test may approach: it is the number the driver has to stay
/// **well** inside, because those ten seconds are paid by every test in the
/// suite. Two is loose enough not to be flaky on a loaded machine and tight
/// enough that a driver needing the fallback cannot pass.
const EXIT_BUDGET: Duration = Duration::from_secs(2);

/// The variables the harness can send **that this driver reads**, each with a
/// non-default value it accepts.
///
/// Every value here has to be one the reference would take too: a value this
/// driver refused would prove nothing about the unknown-name clause and would
/// leave the driver unable to start at all. `AERON_DIR` is not in the list —
/// the fixture owns it, because every test needs it to point at its own
/// directory.
///
/// `AERON_MTU_LENGTH` is absent on purpose and would be a mistake to add:
/// `CTestMediaDriver` sets `publicationTermBufferLength()` and no MTU at all.
const READ: &[(&str, &str)] = &[
    ("AERON_CLIENT_LIVENESS_TIMEOUT", "15s"),
    ("AERON_DIR_DELETE_ON_START", "true"),
    ("AERON_DIR_DELETE_ON_SHUTDOWN", "true"),
    ("AERON_DRIVER_TERMINATION_VALIDATOR", "allow"),
    ("AERON_FILE_PAGE_SIZE", "8192"),
    ("AERON_IPC_TERM_BUFFER_LENGTH", "1m"),
    ("AERON_PERFORM_STORAGE_CHECKS", "false"),
    ("AERON_PUBLICATION_LINGER_TIMEOUT", "1s"),
    ("AERON_RCV_INITIAL_WINDOW_LENGTH", "64k"),
    ("AERON_RCV_STATUS_MESSAGE_TIMEOUT", "1s"),
    ("AERON_SOCKET_SO_RCVBUF", "1m"),
    ("AERON_SOCKET_SO_SNDBUF", "512k"),
    ("AERON_SPIES_SIMULATE_CONNECTION", "true"),
    ("AERON_TERM_BUFFER_LENGTH", "1m"),
    ("AERON_TERM_BUFFER_SPARSE_FILE", "false"),
    ("AERON_TIMER_INTERVAL", "100ms"),
    ("AERON_UNTETHERED_LINGER_TIMEOUT", "2s"),
    ("AERON_UNTETHERED_RESTING_TIMEOUT", "2s"),
    ("AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT", "2s"),
];

/// The variables the harness can send **that this driver does not read**, at
/// values that say so.
///
/// The values are deliberately unhelpful — a name-resolution table that does
/// not exist, cycle thresholds in a unit nothing here consults, a threading
/// mode this build does not have. That is the point: if a later slice binds one
/// of these names, the value below becomes wrong and this test is where it
/// shows up.
///
/// Four are worth naming, because each is a place a reader might expect a
/// different answer:
///
/// - `AERON_PUBLICATION_CONNECTION_TIMEOUT` is a **real gap**: the driver has
///   the setting, with the reference's five-second default, and no name bound
///   to it — so the harness's value is silently dropped. It is listed here
///   rather than in [`READ`] because that is the truth today.
/// - `AERON_IMAGE_LIVENESS_TIMEOUT` is set by **39 of the system tests**, more
///   than any other name this driver does not read. If the baseline has a
///   cluster of timeouts, this is the first name to suspect.
/// - `AERON_EVENT_LOG` is set **unconditionally** to `"admin"` (`:459-462`),
///   so the reference's instrumentation layer is on in every single test
///   whether the test asked for it or not. Accepting the name is what this
///   clause requires; what its absence means for the baseline is a separate
///   question with its own answer in G0-2.
/// - `AERON_THREADING_MODE` is the one whose *absence* is a known weakness of
///   the baseline rather than of this driver — see the module docs.
const IGNORED: &[(&str, &str)] = &[
    ("AERON_CONDUCTOR_IDLE_STRATEGY", "sleeping"),
    ("AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD", "1ms"),
    (
        "AERON_DRIVER_DYNAMIC_LIBRARIES",
        "/nonexistent/libaeron_ats.so",
    ),
    ("AERON_DRIVER_NAME_RESOLVER_THRESHOLD", "1ms"),
    ("AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD", "1ms"),
    ("AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR", "127.0.0.1:5000"),
    (
        "AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL",
        "5s",
    ),
    ("AERON_DRIVER_RESOLVER_INTERFACE", "lo"),
    ("AERON_DRIVER_RESOLVER_NAME", "aeron:net-driver"),
    ("AERON_DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL", "5s"),
    ("AERON_DRIVER_RESOLVER_NEIGHBOR_TIMEOUT", "5s"),
    ("AERON_DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL", "5s"),
    ("AERON_DRIVER_SENDER_CYCLE_THRESHOLD", "1ms"),
    ("AERON_DRIVER_STREAM_SESSION_LIMIT", "65536"),
    ("AERON_ENABLE_EXPERIMENTAL_FEATURES", "true"),
    ("AERON_EVENT_LOG", "admin"),
    ("AERON_EVENT_LOG_DISABLE", ""),
    ("AERON_FLOW_CONTROL_GROUP_MIN_SIZE", "3"),
    ("AERON_FLOW_CONTROL_GROUP_TAG", "7"),
    ("AERON_IMAGE_LIVENESS_TIMEOUT", "15s"),
    (
        "AERON_MULTICAST_FLOWCONTROL_SUPPLIER",
        "aeron_max_multicast_flow_control_strategy_supplier",
    ),
    ("AERON_NAME_RESOLVER_INIT_ARGS", "/nonexistent/resolver.csv"),
    ("AERON_NAME_RESOLVER_SUPPLIER", "csv_table"),
    ("AERON_NATIVE_RESOURCE_AGENT_IDLE_STRATEGY", "sleeping"),
    ("AERON_NAK_UNICAST_DELAY", "0"),
    ("AERON_PRINT_CONFIGURATION", "true"),
    ("AERON_PUBLICATION_CONNECTION_TIMEOUT", "5s"),
    ("AERON_PUBLICATION_UNBLOCK_TIMEOUT", "15s"),
    ("AERON_RECEIVER_GROUP_TAG", "42"),
    ("AERON_RECEIVER_IDLE_STRATEGY", "sleeping"),
    ("AERON_RECEIVER_WILDCARD_PORT_RANGE", "30000-30010"),
    ("AERON_SENDER_IDLE_STRATEGY", "sleeping"),
    ("AERON_SENDER_WILDCARD_PORT_RANGE", "30010-30020"),
    ("AERON_SHAREDNETWORK_IDLE_STRATEGY", "sleeping"),
    ("AERON_SHARED_IDLE_STRATEGY", "sleeping"),
    ("AERON_THREADING_MODE", "SHARED"),
    ("AERON_TRANSPORT_SECURITY_CONF_DIR", "/nonexistent/ats-conf"),
    (
        "AERON_TRANSPORT_SECURITY_CONF_FILE",
        "/nonexistent/ats.conf",
    ),
    (
        "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS",
        "aeron_transport_security_channel_interceptor_load",
    ),
    (
        "AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS",
        "aeron_transport_security_channel_interceptor_load",
    ),
    (
        "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_FIXED_LOSS_ARGS",
        "term-id=0|term-offset=0|length=1024",
    ),
    (
        "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS",
        "rate=0.1|seed=7",
    ),
    (
        "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MULTI_GAP_LOSS_ARGS",
        "rate=0.1|seed=7",
    ),
    (
        "AERON_UNICAST_FLOWCONTROL_SUPPLIER",
        "aeron_max_unicast_flow_control_strategy_supplier",
    ),
];

/// A driver of ours in a directory of its own, started the way the harness
/// starts one: **environment variables**, a directory that does not exist yet,
/// and stdout and stderr kept apart.
///
/// `deepmsg_tests::driver::OwnDriver` cannot be used here. It passes `-D`
/// arguments, which is this driver's *own* syntax but not the one under test,
/// and it merges the two output streams into one file — which would make the
/// stderr clause unassertable, since that is the thing being measured.
struct Fixture {
    child: Child,
    aeron_dir: PathBuf,
    stderr: PathBuf,
    exit: Option<ExitStatus>,
    /// Removed on drop, and it holds the aeron directory inside it.
    _dir: TempDir,
}

impl Fixture {
    /// Start a driver with `env` on top of the configuration every test needs.
    fn start(name: &str, env: &[(&str, &str)]) -> Option<Self> {
        let binary = driver::locate_own()?;
        let dir = TempDir::new(&format!("deepmsg-g0-1-{name}"));
        let aeron_dir = dir.path().join("aeron");

        let stdout = std::fs::File::create(dir.path().join("stdout")).expect("create stdout");
        let stderr_path = dir.path().join("stderr");
        let stderr = std::fs::File::create(&stderr_path).expect("create stderr");

        let mut command = Command::new(binary);
        command
            .env("AERON_DIR", &aeron_dir)
            .envs(env.iter().copied())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));

        // The ambient environment is not part of the contract, and inheriting
        // it would make these tests depend on the shell they run in.
        for (key, _) in std::env::vars_os() {
            let key = key.to_string_lossy();
            if key.starts_with("AERON_") || key.starts_with("DEEPMSG_") {
                command.env_remove(key.as_ref());
            }
        }

        let child = command.spawn().expect("start our own driver");

        Some(Self {
            child,
            aeron_dir,
            stderr: stderr_path,
            exit: None,
            _dir: dir,
        })
    }

    /// The aeron directory this driver owns.
    fn aeron_dir(&self) -> &Path {
        &self.aeron_dir
    }

    /// Wait until the driver has published a CnC file this build can read.
    fn await_cnc(&self) -> CncFile {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut last;

        loop {
            match CncFile::try_open(self.aeron_dir()) {
                Ok(cnc) => return cnc,
                Err(error) => last = error.to_string(),
            }

            assert!(
                Instant::now() < deadline,
                "the driver published no usable CnC file within {READY_TIMEOUT:?}: {last}"
            );

            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Write the harness's `TERMINATE_DRIVER` and report how long the driver
    /// took to go.
    ///
    /// This is `CommonContext.requestDriverTermination(directory, null, 0, 0)`
    /// byte for byte (`CommonContext.java:1140-1165`): the command is written
    /// straight into the CnC file by a process that has **registered nothing**
    /// — no client record, no heartbeat counter, no keepalive — which is the
    /// one thing a client-shaped helper would get wrong.
    ///
    /// The shape is not the C client's. C fills both id fields from the ring's
    /// correlation counter (`aeron-client/src/main/c/aeron_context.c:661-662`);
    /// the Java harness fills `client_id` that way and then writes
    /// `Aeron.NULL_VALUE` for the correlation (`DriverProxy.java:492-493`).
    /// The driver reads neither, so both work — and this is the one the suite
    /// actually sends, so it is the one worth pinning.
    fn terminate(&mut self) -> Duration {
        let written = Instant::now();

        let cnc = CncFile::try_open_writable(self.aeron_dir()).expect("reopen the CnC file");
        let ring = cnc.to_driver_ring().expect("the to-driver ring");

        let command = TerminateDriver {
            client_id: ring
                .next_correlation_id()
                .expect("the ring's correlation counter"),
            correlation_id: deepmsg_cnc::layout::NULL_VALUE,
            token: &[],
        };

        let mut payload = vec![0_u8; command.encoded_length()];
        assert!(command.encode_into(&mut payload), "the command encodes");
        ring.write(TERMINATE_DRIVER_TYPE_ID, &payload)
            .expect("the command is written");

        self.wait(written)
    }

    /// Ask the driver to stop on `SIGTERM` and wait for it.
    ///
    /// `Child::kill` is `SIGKILL`, and this crate is `#![forbid(unsafe_code)]`,
    /// so `kill(1)` is the only way to send `SIGTERM` — the same way
    /// `deepmsg_tests::driver::ReferenceDriver::stop` does it.
    fn stop_on_signal(&mut self) -> Duration {
        let written = Instant::now();

        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status();

        self.wait(written)
    }

    /// Wait for the process to exit, and say how long it took.
    fn wait(&mut self, since: Instant) -> Duration {
        let deadline = Instant::now() + STOP_TIMEOUT;

        loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                let elapsed = since.elapsed();
                self.exit = Some(status);

                return elapsed;
            }

            assert!(
                Instant::now() < deadline,
                "the driver did not stop within {STOP_TIMEOUT:?}"
            );

            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// How many bytes the driver wrote to stderr.
    ///
    /// Read after the process has exited, which is when the file is complete —
    /// the harness reads it the same way, from the file it redirected the child
    /// into (`CTestMediaDriver.java:332-336`).
    fn stderr_len(&self) -> u64 {
        std::fs::metadata(&self.stderr)
            .expect("the stderr file exists")
            .len()
    }

    /// The status the driver exited with.
    fn exit(&self) -> ExitStatus {
        self.exit.expect("the driver has exited")
    }
}

impl Drop for Fixture {
    /// Never leave a process behind, however the test ended. The directory
    /// removes itself; a running driver would keep a `cnc.dat` mapped inside
    /// it and make that removal fail.
    fn drop(&mut self) {
        if self.exit.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Publish one message on an IPC stream and read it back, through a client of
/// ours.
///
/// This is the "the configuration did not break the driver" half of the
/// acceptance: a driver that starts and does nothing is not a driver that
/// works, and every clause above would pass for one. It is deliberately the
/// smallest exchange that is not a no-op — a publication, a subscription, one
/// message, and the same bytes on both sides.
fn publish_and_read(fixture: &Fixture) {
    let mut client = Client::connect(fixture.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_publication("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an IPC publication");
    let subscription = client
        .add_subscription("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an IPC subscription");

    let payload = b"the configuration did not break this driver";
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut received = None;

    while received.is_none() {
        if let Some(Appended::Ok { .. }) = client.offer(publication, payload) {}
        client.poll();
        client.poll_subscription(subscription, 10, |message| {
            received = Some(message.payload.to_vec());
        });

        assert!(
            Instant::now() < deadline,
            "the message never came back, so the driver is not serving IPC"
        );

        std::thread::sleep(Duration::from_millis(2));
    }

    assert_eq!(payload.to_vec(), received.expect("a message arrived"));
}

#[test]
fn a_clean_run_writes_nothing_to_stderr() {
    let Some(mut fixture) = Fixture::start("clean", READ) else {
        driver::announce_own_skip();
        return;
    };

    // Read only to prove the file is there; the driver's own view is not what
    // this test is about.
    let _ = fixture.await_cnc();
    publish_and_read(&fixture);

    fixture.terminate();

    assert_eq!(
        0,
        fixture.stderr_len(),
        "stderr is a contract: the reference's own harness asserts this file is empty \
         (MediaDriverTestUtil.java:128-132), so a single diagnostic line here fails every \
         system test at once"
    );
}

#[test]
fn every_variable_the_reference_harness_can_send_is_accepted() {
    // Both tables at once: the names this driver reads and the names it does
    // not. The ones it does not are the ones this test exists for — an unknown
    // property is ignored by design (`config.rs` module docs), and this is
    // where that stops being a claim.
    let mut all: Vec<(&str, &str)> = READ.to_vec();
    all.extend_from_slice(IGNORED);

    let Some(mut fixture) = Fixture::start("all-variables", &all) else {
        driver::announce_own_skip();
        return;
    };

    let _ = fixture.await_cnc();
    publish_and_read(&fixture);

    fixture.terminate();

    assert_eq!(
        0,
        fixture.stderr_len(),
        "one of the {} variables produced a diagnostic",
        all.len()
    );
}

#[test]
fn a_signalled_driver_is_clean_too() {
    // Not a clause of the harness's contract — it stops drivers with
    // `TERMINATE_DRIVER` and falls back to `SIGKILL`, which reaches no handler
    // at all. It is here because `bin/deepmsg-driver.rs` prints on this path
    // and that print is the one a reader is most likely to move back to stderr
    // "because it is about shutdown".
    let Some(mut fixture) = Fixture::start("signal", READ) else {
        driver::announce_own_skip();
        return;
    };

    let _ = fixture.await_cnc();
    fixture.stop_on_signal();

    // The reference's `aeronmd` exits with the *signal number*, not zero
    // (`aeron-driver/src/main/c/aeronmd.c:41,112-113,185`), and this driver
    // does the same on purpose.
    assert_eq!(Some(15), fixture.exit().code(), "SIGTERM is recorded");
    assert_eq!(0, fixture.stderr_len());
}

#[test]
fn the_harness_shaped_terminate_command_stops_the_driver() {
    let Some(mut fixture) = Fixture::start("terminate-shape", READ) else {
        driver::announce_own_skip();
        return;
    };

    let _ = fixture.await_cnc();
    publish_and_read(&fixture);

    assert!(
        fixture.exit.is_none(),
        "the driver is still running before the command is written"
    );

    fixture.terminate();

    // Zero, and that is the assertion that carries the weight: the harness's
    // fallback is `destroyForcibly`, whose exit value records the signal, not
    // this driver's clean path. A driver that had to be killed would not be
    // zero here.
    assert_eq!(
        Some(0),
        fixture.exit().code(),
        "the driver ran its own shutdown path rather than being killed"
    );
    assert_eq!(0, fixture.stderr_len());
}

#[test]
fn the_driver_stops_well_inside_the_harness_budget() {
    let Some(mut fixture) = Fixture::start("terminate-budget", READ) else {
        driver::announce_own_skip();
        return;
    };

    let _ = fixture.await_cnc();
    let elapsed = fixture.terminate();

    assert!(
        elapsed < EXIT_BUDGET,
        "the driver took {elapsed:?} to stop after the command; the harness waits 10s and then \
         kills it (CTestMediaDriver.java:593-599), which it pays for once per test"
    );
    assert_eq!(Some(0), fixture.exit().code(), "and it exits cleanly");
}
