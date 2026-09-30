//! The live harness: a round trip through a media driver, measured.
//!
//! `harness = false`, and it parses its own arguments, because what it does is
//! not a `bench_function`: it starts a **driver process**, and then a second
//! process of itself to echo, and measures a message that goes out and comes
//! back through both. criterion would time the setup, which is not the number
//! anyone wants.
//!
//! # Why two processes
//!
//! The reference's own instrument is `Ping` and `Pong`
//! (`aeron-samples/src/main/c/cping.c`, `cpong.c`): one process publishes and
//! waits, another echoes, and the number is the round trip between them. An
//! echo living inside the measuring process would remove a process switch and a
//! scheduling hop from every sample, and the result would not be comparable
//! with the reference's — so this harness spawns itself with `--echo` and
//! measures the real thing. The child is the same binary, so the two ends
//! cannot drift apart.
//!
//! The two directions use two channels and two stream ids, which is the
//! reference's own arrangement (`samples_configuration.h:21-25`): on a unicast
//! channel the subscriber binds the endpoint, so one endpoint cannot carry both
//! directions without two binders fighting over one port.
//!
//! # What the number is
//!
//! The caller writes `monotonic_nano_time()` into the first eight bytes of the
//! message, the echo sends the payload back unchanged, and the caller reads the
//! clock again on receipt. What is recorded is therefore the whole loop: the
//! publisher's append, the driver's send path, the echo's poll and append, the
//! driver's send path back, and the caller's poll — the same thing the
//! reference measures, and the same way (`cping.c:109`, `:79-88`).
//!
//! # Failing loudly
//!
//! A tool that measures air is worse than one that refuses to run: every
//! precondition here (no driver binary, no CnC file, a stream that never
//! connects, a size too small to carry a stamp, no reply to a stamp) ends in a
//! non-zero exit and a sentence saying what was missing. The one thing it never
//! does is print a table of zeros.

#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use deepmsg_bench::{Channel, MachineFingerprint, Scenario, Summary, histogram};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::clock::monotonic_nano_time;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{OWN_DRIVER_ENV, ReferenceDriver};

/// The ping stream, the reference's own default (`samples_configuration.h:24`).
const PING_STREAM_ID: i32 = 1002;

/// The pong stream, likewise (`samples_configuration.h:25`).
const PONG_STREAM_ID: i32 = 1003;

/// How long any one step of the setup may take before the run is a failure.
const SETUP_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a single round trip may take before the run is a failure.
///
/// Generous: this is a liveness check, not a latency budget. A round trip that
/// takes a second means something is wrong with the loop, not that the machine
/// is slow.
const ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(1);

/// The number of messages one poll takes.
///
/// One: the echo is a mirror, and a poll that took more would let it work
/// through a backlog before the caller's next sample, hiding that backlog from
/// the measurement instead of exposing it as latency.
const MESSAGE_LIMIT: usize = 1;

fn main() -> ExitCode {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}\n\n{}", Args::USAGE);
            return ExitCode::FAILURE;
        }
    };

    let result = if let Some(aeron_dir) = args.echo.as_deref() {
        echo(&args, aeron_dir)
    } else {
        measure(&args)
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("latency: {message}");
            ExitCode::FAILURE
        }
    }
}

/// The measuring side: start a driver, start the echo, measure round trips.
fn measure(args: &Args) -> Result<(), String> {
    let binary = resolve_driver(args.driver_binary.as_deref())?;
    let mut driver = ReferenceDriver::start_with(&binary, "bench-latency", &[])
        .map_err(|error| format!("could not start {}: {error}", binary.display()))?;

    // The driver is stopped however the run ends: a failed benchmark that
    // leaves a driver process behind is a failed benchmark that also costs the
    // next one its ports.
    let outcome = run(args, &binary, &mut driver);
    let _ = driver.stop();

    outcome
}

/// The measuring side's body, with the driver already started.
fn run(args: &Args, binary: &Path, driver: &mut ReferenceDriver) -> Result<(), String> {
    let scenario = Scenario {
        channel: args.channel,
        length: args.length,
    };

    driver
        .await_cnc(SETUP_TIMEOUT)
        .map_err(|error| format!("the driver never published a usable CnC file: {error}"))?;

    let aeron_dir = driver.aeron_dir().to_path_buf();
    let mut client = Client::connect(&aeron_dir)
        .map_err(|error| format!("could not connect to {}: {error}", aeron_dir.display()))?;

    let (ping_uri, pong_uri) = args.uris();
    let publication = client
        .add_publication(&ping_uri, PING_STREAM_ID, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no publication on {ping_uri}: {error}"))?;
    let subscription = client
        .add_subscription(&pong_uri, PONG_STREAM_ID, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no subscription on {pong_uri}: {error}"))?;

    let mut echo = Echo::start(&aeron_dir, &ping_uri, &pong_uri)?;

    // Both ends say they are up: the echo has an image to read, and this side
    // has one to read back. Either alone leaves the first offer with nowhere to
    // go, and the offers that follow would be counted as latency.
    echo.await_ready(SETUP_TIMEOUT)?;
    await_image(&mut client, subscription, SETUP_TIMEOUT).ok_or_else(|| {
        format!("no image on {pong_uri} — the echo never published anything this side could read")
    })?;

    for _ in 0..args.warmup {
        round_trip(&mut client, publication, subscription, &scenario)?;
    }

    let mut samples = histogram();
    let started = Instant::now();
    for _ in 0..args.messages {
        let elapsed_ns = round_trip(&mut client, publication, subscription, &scenario)?;
        samples
            .record(elapsed_ns)
            .map_err(|error| format!("a sample fell outside the histogram's bounds: {error}"))?;
    }
    let wall = started.elapsed();

    println!(
        "{} round trip, {} B payload, {} measured (+{} warm-up), {:.0}/s wall clock",
        scenario.channel.name(),
        scenario.length,
        args.messages,
        args.warmup,
        args.messages as f64 / wall.as_secs_f64(),
    );
    println!("  {}", Summary::of(&samples));
    println!(
        "  driver {} ({})",
        binary.display(),
        built_when(binary).unwrap_or_else(|| "build time unknown".to_owned())
    );
    println!("  {}", MachineFingerprint::capture());

    echo.stop();

    Ok(())
}

/// The echoing side: publish back whatever arrives, unchanged.
///
/// Run as a child of [`measure`] rather than as a binary of its own so that the
/// two ends are one build, and so the arguments that pair them are written
/// once. It spins rather than sleeping: a mirror that slept would add its sleep
/// to every sample, and the sample is supposed to be the machine's, not the
/// harness's.
fn echo(args: &Args, aeron_dir: &Path) -> Result<(), String> {
    let (ping_uri, pong_uri) = args.uris();
    let mut client = Client::connect(aeron_dir)
        .map_err(|error| format!("could not connect to {}: {error}", aeron_dir.display()))?;

    let subscription = client
        .add_subscription(&ping_uri, PING_STREAM_ID, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no subscription on {ping_uri}: {error}"))?;
    let publication = client
        .add_publication(&pong_uri, PONG_STREAM_ID, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no publication on {pong_uri}: {error}"))?;

    await_image(&mut client, subscription, SETUP_TIMEOUT)
        .ok_or_else(|| format!("no image on {ping_uri} — nothing is publishing to echo"))?;

    println!("echo ready");
    std::io::stdout()
        .flush()
        .map_err(|error| error.to_string())?;

    // Reused across messages, so a mirror does not allocate per round trip —
    // an allocator pause inside the loop would land in the sample as latency.
    let mut payload = Vec::new();

    loop {
        client.poll();

        let mut arrived = false;
        client.poll_subscription(subscription, MESSAGE_LIMIT, |message| {
            payload.clear();
            payload.extend_from_slice(message.payload);
            arrived = true;
        });

        if arrived {
            offer(&client, publication, &payload)?;
        }
    }
}

/// Offer until the driver's window allows it, or the round trip is a failure.
///
/// The window is closed until the far end's status message has arrived, so the
/// first attempts of a cold run come back `NotConnected`; after the warm-up that
/// is a fault rather than a startup, and it is reported as one instead of being
/// counted as a slow sample.
fn offer(client: &Client, publication: i64, payload: &[u8]) -> Result<(), String> {
    let deadline = Instant::now() + ROUND_TRIP_TIMEOUT;

    loop {
        match client.offer(publication, payload) {
            Some(Appended::Ok { .. }) => return Ok(()),
            Some(outcome) => {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "the publication never accepted an offer: {outcome:?}"
                    ));
                }
            }
            None => return Err("the publication is gone".to_owned()),
        }
    }
}

/// One round trip, in nanoseconds.
///
/// The stamp is written into the payload and read back off the reply, so what
/// arrives is the answer to what went out: with only the arrival timed, a reply
/// to an *earlier* message would be recorded as a very fast one.
fn round_trip(
    client: &mut Client,
    publication: i64,
    subscription: i64,
    scenario: &Scenario,
) -> Result<u64, String> {
    let stamp = monotonic_nano_time();
    let payload = scenario.payload(stamp);

    offer(client, publication, &payload)?;

    let deadline = Instant::now() + ROUND_TRIP_TIMEOUT;
    let mut replied = false;

    while Instant::now() < deadline {
        client.poll();
        client.poll_subscription(subscription, MESSAGE_LIMIT, |message| {
            if Scenario::stamp(message.payload) == Some(stamp) {
                replied = true;
            }
        });

        if replied {
            let elapsed = monotonic_nano_time() - stamp;

            return u64::try_from(elapsed)
                .map_err(|_| format!("a round trip came back before it was sent ({elapsed} ns)"));
        }
    }

    Err(format!(
        "no reply to the stamp {stamp} within {ROUND_TRIP_TIMEOUT:?}"
    ))
}

/// Wait until a subscription has an image, which is what "the far end is
/// publishing" looks like from here.
fn await_image(client: &mut Client, subscription: i64, within: Duration) -> Option<i64> {
    let deadline = Instant::now() + within;

    while Instant::now() < deadline {
        client.poll();

        if let Some(image) = client
            .subscription(subscription)
            .and_then(|subscription| subscription.images().first())
        {
            return Some(image.registration_id());
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    None
}

/// A started echo, killed when it goes out of scope.
struct Echo {
    child: Child,
    ready: mpsc::Receiver<String>,
}

impl Echo {
    /// Start the echo as a child of this process, reading its stdout.
    ///
    /// The two channel URIs are passed down rather than recomputed: they carry
    /// ports derived from the *parent's* process id, and a child that derived
    /// its own would listen somewhere else entirely.
    fn start(aeron_dir: &Path, ping_uri: &str, pong_uri: &str) -> Result<Self, String> {
        let exe = std::env::current_exe()
            .map_err(|error| format!("could not find this harness's own binary: {error}"))?;

        let mut child = Command::new(exe)
            .arg("--echo")
            .arg(aeron_dir)
            .arg("--ping")
            .arg(ping_uri)
            .arg("--pong")
            .arg(pong_uri)
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|error| format!("could not start the echo: {error}"))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "the echo has no stdout to read".to_owned())?;

        let (sender, ready) = mpsc::channel();

        // Reading blocks until the echo says something, so it cannot run on
        // this thread — [`Echo::await_ready`]'s timeout is what bounds it.
        std::thread::spawn(move || {
            let mut lines = BufReader::new(stdout).lines();

            if let Some(Ok(line)) = lines.next() {
                let _ = sender.send(line);
            }
        });

        Ok(Self { child, ready })
    }

    /// Wait for the echo to say it has an image to read.
    fn await_ready(&mut self, within: Duration) -> Result<(), String> {
        match self.ready.recv_timeout(within) {
            Ok(line) if line == "echo ready" => Ok(()),
            Ok(line) => Err(format!("the echo said {line:?} instead of being ready")),
            Err(_) => Err(format!("the echo was not ready within {within:?}")),
        }
    }

    /// Stop the echo and reap it.
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Echo {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Where the driver binary is: the **release** one, because `cargo bench` is a
/// release build, and a release client measured against a debug driver is a
/// number about nothing.
///
/// The order is: what the caller named, then `DEEPMSG_DRIVER` (the name the
/// interop suite uses for the same purpose), then this workspace's own release
/// output. A debug binary is deliberately not a fallback — the interop suite's
/// default is `target/debug`, and silently taking it here would mix profiles.
fn resolve_driver(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }

    if let Some(path) = std::env::var_os(OWN_DRIVER_ENV) {
        return Ok(PathBuf::from(path));
    }

    let release = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/release/deepmsg-driver");

    if release.exists() {
        // Resolved, because the path this is built from walks up out of the
        // crate and is printed in the run's own record — and a record whose
        // path has to be read with a map is a record nobody checks.
        return Ok(std::fs::canonicalize(&release).unwrap_or(release));
    }

    Err(format!(
        "no driver binary at {} — build it with `cargo build --release -p deepmsg-driver`, \
         or name one with --driver-binary",
        release.display()
    ))
}

/// How long ago the driver binary was built.
///
/// Two runs whose numbers differ are usually two different builds, and a
/// fingerprint that could not say *which* build it measured would leave that
/// indistinguishable from noise.
fn built_when(binary: &Path) -> Option<String> {
    let modified = std::fs::metadata(binary).ok()?.modified().ok()?;
    let age = SystemTime::now().duration_since(modified).ok()?;

    Some(format!("built {}s ago", age.as_secs()))
}

/// What the harness was asked to do.
struct Args {
    channel: Channel,
    length: usize,
    messages: u64,
    warmup: u64,
    driver_binary: Option<PathBuf>,
    /// Set only in the child: the aeron directory to connect the echo to.
    echo: Option<PathBuf>,
    /// Set only in the child: the pair of channels the parent chose.
    echoed_uris: Option<(String, String)>,
}

impl Args {
    /// The harness's own usage, kept next to the parser so they cannot drift.
    const USAGE: &'static str = "\
usage: cargo bench -p deepmsg-bench --bench latency -- [options]

  --channel ipc|udp     transport to measure (default: ipc)
  --length N            payload bytes (default: 32)
  --messages N          measured round trips (default: 20000)
  --warmup N            unmeasured round trips first (default: 2000)
  --driver-binary PATH  driver to start (default: target/release/deepmsg-driver)
  --echo DIR --ping URI --pong URI
                        internal: run as the echo peer on these channels";

    /// Parse the arguments, or say what was wrong with them.
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut parsed = Self {
            channel: Channel::Ipc,
            length: 32,
            messages: 20_000,
            warmup: 2_000,
            driver_binary: None,
            echo: None,
            echoed_uris: None,
        };
        let mut ping = None;
        let mut pong = None;

        while let Some(arg) = args.next() {
            let mut value = || args.next().ok_or_else(|| format!("{arg} needs a value"));

            match arg.as_str() {
                "--channel" => {
                    parsed.channel = match value()?.as_str() {
                        "ipc" => Channel::Ipc,
                        "udp" => Channel::Udp,
                        other => return Err(format!("unknown channel {other:?}")),
                    };
                }
                "--length" => parsed.length = parse_number(&value()?, &arg)?,
                "--messages" => parsed.messages = parse_number(&value()?, &arg)?,
                "--warmup" => parsed.warmup = parse_number(&value()?, &arg)?,
                "--driver-binary" => parsed.driver_binary = Some(PathBuf::from(value()?)),
                "--echo" => parsed.echo = Some(PathBuf::from(value()?)),
                "--ping" => ping = Some(value()?),
                "--pong" => pong = Some(value()?),
                // `cargo bench` appends its own flags to whatever the caller
                // wrote after `--`, and `--bench` is one of them. It means
                // "you are the benchmark", which this target already knows; a
                // harness that failed on it could not be run through cargo at
                // all.
                "--bench" | "--test" => {}
                other => return Err(format!("unknown argument {other:?}")),
            }
        }

        parsed.echoed_uris = match (ping, pong) {
            (Some(ping), Some(pong)) => Some((ping, pong)),
            (None, None) => None,
            _ => return Err("--ping and --pong go together".to_owned()),
        };

        if parsed.length < size_of::<i64>() {
            return Err(format!(
                "--length {} is too short to carry the round trip's own stamp ({} bytes)",
                parsed.length,
                size_of::<i64>()
            ));
        }

        if parsed.messages == 0 {
            return Err("--messages 0 would measure nothing".to_owned());
        }

        Ok(parsed)
    }

    /// The two channel URIs a run pairs, one per direction.
    ///
    /// The ports are derived from this process's id rather than fixed, the same
    /// way `tests/interop/udp_transport.rs` picks its own: two runs on one
    /// machine collide only if something else chose exactly this pair, and
    /// nothing has to be cleaned up between runs.
    fn uris(&self) -> (String, String) {
        if let Some((ping, pong)) = &self.echoed_uris {
            return (ping.clone(), pong.clone());
        }

        let base = 20_000 + (std::process::id() % 20_000) as u16;

        (
            self.channel.uri(base),
            self.channel.uri(base.saturating_add(1)),
        )
    }
}

/// A number argument, or why it is not one.
fn parse_number<T: std::str::FromStr>(text: &str, name: &str) -> Result<T, String> {
    text.parse()
        .map_err(|_| format!("{name} needs a number, got {text:?}"))
}
