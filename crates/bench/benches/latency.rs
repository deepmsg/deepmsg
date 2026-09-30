//! The live harness: what a round trip through a media driver costs, measured.
//!
//! `harness = false`, and it parses its own arguments, because what it does is
//! not a `bench_function`: it starts a **driver process**, and then a second
//! process of itself at the other end, and measures messages that go between
//! them. criterion would time the setup, which is not the number anyone wants.
//!
//! # Two instruments, one table
//!
//! Ours (`--peer ours`, the default) measures with this build's client. The
//! reference's own `Ping` and `Pong` (`--peer reference`, or `both`) measure
//! the same scenarios with code that shares nothing with this build, which is
//! what makes the numbers comparable rather than merely reported. `--peer
//! both` puts them in one table, and `--driver own|reference` chooses which
//! driver both of them run against — four configurations in all, the same
//! matrix `docs/benchmarks.md` records.
//!
//! # Why two processes
//!
//! The reference's instrument is two processes: one publishes and waits,
//! another echoes. An echo living inside the measuring process would remove a
//! process switch and a scheduling hop from every sample, and the result would
//! not be comparable with the reference's — so this harness spawns itself and
//! measures the real thing. The child is the same binary, so the two ends
//! cannot drift apart.
//!
//! The two directions use two channels and two stream ids, which is the
//! reference's own arrangement (`samples_configuration.h:21-25`): on a unicast
//! channel the subscriber binds the endpoint, so one endpoint cannot carry both
//! directions without two binders fighting over one port.
//!
//! # What the numbers are
//!
//! **Ping-pong.** The caller writes `monotonic_nano_time()` into the first
//! eight bytes of the message, the peer sends the payload back unchanged, and
//! the caller reads the clock again on receipt. What is recorded is the whole
//! loop — the publisher's append, the driver's send path, the peer's poll and
//! append, the driver's send path back, and the caller's poll — the same thing
//! the reference measures, and the same way (`cping.c:109`, `:79-88`).
//!
//! **Throughput.** One direction, no reply: the caller publishes `messages` as
//! fast as the window allows, a second process counts what arrives, and the
//! number is what got there. The attempts it took to publish that many is
//! reported next to it, because "N per second" means something different when
//! the publisher spent half its time being back-pressured.
//!
//! # Failing loudly
//!
//! A tool that measures air is worse than one that refuses to run: every
//! precondition here (no driver binary, no CnC file, a stream that never
//! connects, a size too small to carry a stamp, no reply to a stamp, a
//! reference tool that is not built when one was asked for) ends in a non-zero
//! exit and a sentence saying what was missing. The one thing it never does is
//! print a table of zeros.

#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child as Process, Command, ExitCode, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use deepmsg_bench::{
    Args, Channel, ChildArgs, Invocation, MachineFingerprint, Mode, Scenario, Summary, WhichDriver,
    histogram, summary_from_hdr_table,
};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::clock::monotonic_nano_time;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{OWN_DRIVER_ENV, ReferenceDriver};
use deepmsg_tests::samples::{self, Sample};

/// The ping stream, the reference's own default (`samples_configuration.h:24`).
///
/// A row's own stream id is this plus twice its index, and for an IPC matrix
/// that is not decoration: every IPC row's *channel* is the same string
/// (`aeron:ipc`, where the port means nothing), so two rows sharing a stream
/// would have the second publication link to the first row's subscription —
/// which is gone, but whose reader position the driver still holds. The window
/// then closes and stays closed. Two rounds of the same channel found this.
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

/// How long the last message of a throughput run may take to arrive.
///
/// Longer than a round trip by orders of magnitude, because this covers the
/// pipeline draining rather than one message: the publisher stops as soon as
/// the driver has the last one, and what is being waited for is the subscriber
/// having polled it.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the reference's own instrument may run before it is a failure.
///
/// `Ping` runs `-m` messages and exits; this bounds a run that hung instead.
const PEER_TIMEOUT: Duration = Duration::from_secs(600);

/// The number of messages one poll takes.
///
/// One: the echo is a mirror, and a poll that took more would let it work
/// through a backlog before the caller's next sample, hiding that backlog from
/// the measurement instead of exposing it as latency.
const MESSAGE_LIMIT: usize = 1;

fn main() -> ExitCode {
    let invocation = match Invocation::parse(std::env::args().skip(1)) {
        Ok(invocation) => invocation,
        Err(message) => {
            eprintln!("{message}\n\n{}", Invocation::USAGE);
            return ExitCode::FAILURE;
        }
    };

    let result = match &invocation {
        Invocation::Echo(child) => echo(child),
        Invocation::Sink(child) => sink(child),
        Invocation::Measure(args) => measure(args),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("latency: {message}");
            ExitCode::FAILURE
        }
    }
}

/// The measuring side: start a driver, then measure every requested scenario.
fn measure(args: &Args) -> Result<(), String> {
    let mut driver = Driver::start(args)?;
    let outcome = run(args, &mut driver);
    let _ = driver.process.stop();

    outcome
}

/// The measuring side's body, with the driver already started.
fn run(args: &Args, driver: &mut Driver) -> Result<(), String> {
    let mut rows = Vec::new();

    for (index, pairing) in pairings(args).into_iter().enumerate() {
        let first = args.peer.measures_ours();
        let second = args.peer.measures_reference();
        let third = args.peer.measures_reference_java();

        // Each row gets its own ports, so a row that is still tearing down
        // cannot collide with the next one's bind.
        if first {
            let scenario = pairing.at(index * 2);
            let measured = if args.mode == Mode::PingPong {
                ours(&scenario, driver)
            } else {
                ours_throughput(args, &scenario, driver)
            };

            rows.push(measured.map_err(|error| format!("{}: {error}", scenario.label()))?);
        }

        if second {
            if args.mode != Mode::PingPong {
                return Err(
                    "the reference's own instrument measures round trips only: --mode \
                     throughput has no reference row yet, so ask for --peer ours"
                        .to_owned(),
                );
            }

            let scenario = pairing.at(index * 2 + 1);
            rows.push(
                reference(args, &scenario, driver)
                    .map_err(|error| format!("{}: {error}", scenario.label()))?,
            );
        }

        if third {
            if args.mode != Mode::PingPong {
                return Err(
                    "the reference's Java instrument measures round trips only: --mode \
                     throughput has no Java row"
                        .to_owned(),
                );
            }

            let scenario = pairing.at(index * 2 + 1);
            rows.push(
                reference_java(args, &scenario, driver)
                    .map_err(|error| format!("{}: {error}", scenario.label()))?,
            );
        }
    }

    report(args, &rows, driver);

    Ok(())
}

/// Our own instrument, ping-pong.
fn ours(pairing: &Pairing, driver: &mut Driver) -> Result<Row, String> {
    driver.await_cnc()?;

    let mut client = Client::connect(driver.aeron_dir())
        .map_err(|error| format!("could not connect: {error}"))?;

    let publication = client
        .add_publication(&pairing.ping_uri, pairing.ping_stream_id, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no publication on {}: {error}", pairing.ping_uri))?;
    let subscription = client
        .add_subscription(&pairing.pong_uri, pairing.pong_stream_id, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no subscription on {}: {error}", pairing.pong_uri))?;

    let mut echo = FarEnd::echo(driver.aeron_dir(), pairing)?;

    // Both ends say they are up: the echo has an image to read, and this side
    // has one to read back. Either alone leaves the first offer with nowhere to
    // go, and the offers that follow would be counted as latency.
    echo.await_line("echo ready", SETUP_TIMEOUT)?;
    await_image(&mut client, subscription, SETUP_TIMEOUT).ok_or_else(|| {
        format!(
            "no image on {} — the echo never published anything this side could read",
            pairing.pong_uri
        )
    })?;

    for _ in 0..pairing.warmup {
        round_trip(&mut client, publication, subscription, pairing)?;
    }

    let mut samples = histogram();
    for _ in 0..pairing.messages {
        let elapsed_ns = round_trip(&mut client, publication, subscription, pairing)?;
        samples
            .record(elapsed_ns)
            .map_err(|error| format!("a sample fell outside the histogram's bounds: {error}"))?;
    }

    echo.stop();

    Ok(Row::Latency {
        client: "ours",
        driver: driver.name(),
        channel: pairing.channel,
        length: pairing.length,
        summary: Summary::of(&samples),
    })
}

/// Our own instrument, throughput: publish as fast as the window allows while a
/// second process counts what arrives.
fn ours_throughput(args: &Args, pairing: &Pairing, driver: &mut Driver) -> Result<Row, String> {
    driver.await_cnc()?;

    let mut client = Client::connect(driver.aeron_dir())
        .map_err(|error| format!("could not connect: {error}"))?;

    let publication = client
        .add_publication(&pairing.ping_uri, pairing.ping_stream_id, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no publication on {}: {error}", pairing.ping_uri))?;

    let mut sink = FarEnd::sink(driver.aeron_dir(), pairing, args.messages)?;
    sink.await_line("sink ready", SETUP_TIMEOUT)?;

    let payload = pairing.payload(0);
    let started = Instant::now();
    let mut accepted = 0_u64;
    let mut attempts = 0_u64;

    while accepted < args.messages {
        attempts += 1;

        match client.offer(publication, &payload) {
            Some(Appended::Ok { .. }) => accepted += 1,
            // Everything else means "not now": the window is closed until the
            // sink's status messages have been through, or the term rotated
            // under this call. Polling is what lets this client's own side of
            // that conversation happen.
            Some(_) => {}
            None => return Err("the publication is gone".to_owned()),
        }

        client.poll();

        if started.elapsed() > DRAIN_TIMEOUT {
            return Err(format!(
                "the publication took only {accepted} of {} messages in {DRAIN_TIMEOUT:?}",
                args.messages
            ));
        }
    }

    // What arrived, not what was handed over: the sink says it has all of them,
    // and the time is from the first offer to that line.
    let line = sink.await_line("received", DRAIN_TIMEOUT)?;
    let seconds = started.elapsed().as_secs_f64();
    let received = line
        .split_whitespace()
        .nth(1)
        .and_then(|count| count.parse::<u64>().ok())
        .ok_or_else(|| format!("the sink's report did not name a count: {line:?}"))?;

    if received != args.messages {
        return Err(format!(
            "the sink received {received} of {} messages",
            args.messages
        ));
    }

    Ok(Row::Throughput {
        client: "ours",
        driver: driver.name(),
        channel: pairing.channel,
        length: pairing.length,
        messages: received,
        seconds,
        attempts,
    })
}

/// The reference's own instrument: `Ping` and `Pong`, on the same driver.
fn reference(args: &Args, pairing: &Pairing, driver: &mut Driver) -> Result<Row, String> {
    driver.await_cnc()?;

    let pong = samples::locate("Pong").ok_or_else(|| missing_sample("Pong"))?;
    let ping = samples::locate("Ping").ok_or_else(|| missing_sample("Ping"))?;
    let dir = driver.aeron_dir();

    let ping_stream_id = pairing.ping_stream_id.to_string();
    let pong_stream_id = pairing.pong_stream_id.to_string();
    let channels = [
        "-c",
        pairing.ping_uri.as_str(),
        "-C",
        pairing.pong_uri.as_str(),
        "-s",
        ping_stream_id.as_str(),
        "-S",
        pong_stream_id.as_str(),
    ];

    let mut echo = Sample::start_silent(&pong, "bench-pong", dir, &channels);

    let length = pairing.length.to_string();
    let messages = args.messages.to_string();
    let warmup = pairing.warmup.to_string();
    let mut measure = Sample::start_silent(
        &ping,
        "bench-ping",
        dir,
        &[
            channels.as_slice(),
            &[
                "-L",
                length.as_str(),
                "-m",
                messages.as_str(),
                "-w",
                warmup.as_str(),
            ],
        ]
        .concat(),
    );

    let exited = measure.await_exit(PEER_TIMEOUT);

    // Both are stopped whatever happened: a `Ping` that hung still leaves a
    // `Pong` holding a port, and the next row needs one.
    let output = measure.output();
    echo.terminate(Duration::from_secs(5));

    if exited.is_none() {
        return Err(format!(
            "the reference's Ping did not finish within {PEER_TIMEOUT:?}:\n{output}"
        ));
    }

    Ok(Row::Latency {
        client: "reference Ping/Pong",
        driver: driver.name(),
        channel: pairing.channel,
        length: pairing.length,
        summary: summary_from_hdr_table(&output)?,
    })
}

/// The reference's Java pair: its `Ping` and `Pong` samples against its Java
/// media driver.
///
/// A different *instrument*, not a different build of the same one. The samples
/// read their settings from `aeron.sample.*` **system properties** rather than
/// from a command line (`SampleConfiguration.java:28-45`), and the driver is
/// started by class name — so both ends go through a generated script, which is
/// also what keeps the arguments that pair them written once.
///
/// `aeron.sample.exclusive.publications` is set to `true` deliberately: the
/// C++ `Ping` this table also holds publishes through an exclusive publication,
/// and the Java sample's own default is a plain one. Setting it leaves the
/// languages as the only difference between those two rows.
fn reference_java(args: &Args, pairing: &Pairing, driver: &mut Driver) -> Result<Row, String> {
    driver.await_cnc()?;

    let scripts = JavaScripts::write(pairing, args, driver.aeron_dir(), &java_classpath()?)?;

    let mut echo = scripts.start_pong()?;
    let mut measure = scripts.start_ping()?;

    let exited = measure.await_exit(PEER_TIMEOUT);

    // Both are stopped whatever happened: a `Ping` that hung still leaves a
    // `Pong` holding a port, and the next row needs one.
    let output = measure.output();
    echo.stop();

    if exited.is_none() {
        return Err(format!(
            "the reference's Java Ping did not finish within {PEER_TIMEOUT:?}:\n{output}"
        ));
    }

    Ok(Row::Latency {
        client: "reference Ping/Pong (Java)",
        driver: driver.name(),
        channel: pairing.channel,
        length: pairing.length,
        summary: summary_from_hdr_table(&output)?,
    })
}

/// The scripts the Java pair is launched through, and their output files.
struct JavaScripts {
    ping: PathBuf,
    ping_output: PathBuf,
    pong: PathBuf,
    pong_output: PathBuf,
}

impl JavaScripts {
    /// Write a wrapper per end into the temporary directory.
    ///
    /// Not into the aeron directory: that one is created by the driver's spawn,
    /// and these have to exist before it does.
    fn write(
        pairing: &Pairing,
        args: &Args,
        aeron_dir: &Path,
        classpath: &str,
    ) -> Result<Self, String> {
        let base = std::env::temp_dir().join(format!(
            "deepmsg-bench-java-{}-{}-{}",
            std::process::id(),
            pairing.channel.name().replace(':', "-"),
            pairing.length
        ));

        let stdin = base.with_extension("stdin");
        std::fs::write(&stdin, "n\n").map_err(|error| error.to_string())?;

        let ping = base.with_extension("ping.sh");
        let pong = base.with_extension("pong.sh");

        script(
            &ping,
            &format!(
                "exec java {JVM_OPENS} -cp {classpath} {{properties}} io.aeron.samples.Ping < {}",
                stdin.display()
            ),
            pairing,
            args,
            aeron_dir,
        )?;
        script(
            &pong,
            &format!("exec java {JVM_OPENS} -cp {classpath} {{properties}} io.aeron.samples.Pong"),
            pairing,
            args,
            aeron_dir,
        )?;

        Ok(Self {
            ping_output: base.with_extension("ping.out"),
            pong_output: base.with_extension("pong.out"),
            ping,
            pong,
        })
    }

    /// Start the echoing end, wait for the measuring one, read its table.
    fn start_ping(&self) -> Result<JavaSample, String> {
        spawn_script(&self.ping, &self.ping_output)
    }

    fn start_pong(&self) -> Result<JavaSample, String> {
        spawn_script(&self.pong, &self.pong_output)
    }
}

/// Write one wrapper script.
fn script(
    path: &Path,
    body: &str,
    pairing: &Pairing,
    args: &Args,
    aeron_dir: &Path,
) -> Result<(), String> {
    // Every setting the samples would otherwise take from their own defaults —
    // ten million messages, ten thousand warm-up iterations — is written down,
    // so a row means what the command line says it means.
    let properties = format!(
        "-Daeron.dir={} -Daeron.sample.embeddedMediaDriver=false \
         -Daeron.sample.exclusive.publications=true \
         -Daeron.sample.ping.channel='{}' -Daeron.sample.pong.channel='{}' \
         -Daeron.sample.ping.streamId={} -Daeron.sample.pong.streamId={} \
         -Daeron.sample.messageLength={} -Daeron.sample.messages={} \
         -Daeron.sample.warmup.messages={} -Daeron.sample.warmup.iterations=1",
        aeron_dir.display(),
        pairing.ping_uri,
        pairing.pong_uri,
        pairing.ping_stream_id,
        pairing.pong_stream_id,
        pairing.length,
        args.messages,
        pairing.warmup,
    );

    let text = format!("#!/bin/sh\n{body}\n").replace("{properties}", &properties);
    std::fs::write(path, text).map_err(|error| format!("{}: {error}", path.display()))?;

    let mut permissions = std::fs::metadata(path)
        .map_err(|error| error.to_string())?
        .permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    std::fs::set_permissions(path, permissions).map_err(|error| error.to_string())
}

/// The script the Java media driver is started through.
///
/// The property arguments go **before** the class name: a JVM takes `-D` before
/// its main class, and the driver's spawner hands them to whatever it starts as
/// plain arguments.
fn java_driver_script(classpath: &str) -> Result<PathBuf, String> {
    let path = std::env::temp_dir().join(format!(
        "deepmsg-bench-java-driver-{}.sh",
        std::process::id()
    ));
    let text = format!(
        "#!/bin/sh\nexec java {JVM_OPENS} -cp {classpath} \"$@\" io.aeron.driver.MediaDriver\n"
    );

    std::fs::write(&path, text).map_err(|error| format!("{}: {error}", path.display()))?;

    let mut permissions = std::fs::metadata(&path)
        .map_err(|error| error.to_string())?
        .permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    std::fs::set_permissions(&path, permissions).map_err(|error| error.to_string())?;

    Ok(path)
}

/// The module opens every Aeron Java program needs on a modern JDK.
///
/// `agrona` reads a `ByteBuffer`'s address through `jdk.internal.misc.Unsafe`,
/// which a JDK has not opened to unnamed modules since 17 — the reference's own
/// Gradle build passes exactly these to every task it runs
/// (`build.gradle:225-226`), so a run without them is a run in a configuration
/// the reference does not use either.
const JVM_OPENS: &str = "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
                         --add-opens java.base/java.util.zip=ALL-UNNAMED";

/// The classpath the reference's Java build needs.
///
/// Three jars from the checkout's own build output, and two from the Gradle
/// cache the build resolved them from: `agrona`, which every Aeron Java program
/// uses, and `HdrHistogram`, which is what the samples' percentile table comes
/// from.
fn java_classpath() -> Result<String, String> {
    let reference = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../aeron");
    let mut jars = Vec::new();

    for name in ["aeron-client", "aeron-driver", "aeron-samples"] {
        jars.push(newest_jar(
            &reference.join(name).join("build").join("libs"),
            name,
        )?);
    }

    jars.push(cached_jar("org.agrona", "agrona")?);
    jars.push(cached_jar("org.hdrhistogram", "HdrHistogram")?);

    Ok(jars
        .iter()
        .map(|jar| jar.display().to_string())
        .collect::<Vec<_>>()
        .join(":"))
}

/// The one jar in a directory, or a message naming what was looked for.
fn newest_jar(directory: &Path, name: &str) -> Result<PathBuf, String> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(directory)
        .map_err(|error| format!("{}: {error}", directory.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().is_some_and(|extension| extension == "jar")
                && !path.to_string_lossy().contains("-sources")
                && !path.to_string_lossy().contains("-javadoc")
        })
        .collect();
    found.sort();

    found
        .pop()
        .ok_or_else(|| format!("no {name} jar in {}", directory.display()))
}

/// A jar the reference's Java build resolved from its Gradle cache.
///
/// The path is `…/modules-2/files-2.1/<group>/<artifact>/<version>/<hash>/…`,
/// so the two levels under the artifact are walked rather than guessed at.
fn cached_jar(group: &str, artifact: &str) -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME is not set".to_owned())?;
    let artifact_dir = PathBuf::from(home)
        .join(".gradle/caches/modules-2/files-2.1")
        .join(group)
        .join(artifact);

    let versions = std::fs::read_dir(&artifact_dir)
        .map_err(|error| format!("{}: {error}", artifact_dir.display()))?;

    for version in versions.filter_map(Result::ok) {
        let hashes = std::fs::read_dir(version.path()).map_err(|error| error.to_string())?;

        for hash in hashes.filter_map(Result::ok) {
            if let Ok(jar) = newest_jar(&hash.path(), artifact) {
                return Ok(jar);
            }
        }
    }

    Err(format!(
        "no {artifact} jar under {} — the reference's Java build resolves it there",
        artifact_dir.display()
    ))
}

/// A child process of a script, with its output in a file.
struct JavaSample {
    child: std::process::Child,
    output: PathBuf,
}

impl JavaSample {
    /// Wait for it to finish on its own.
    fn await_exit(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + within;

        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }

            if Instant::now() >= deadline {
                self.stop();
                return None;
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Everything it has written so far.
    fn output(&self) -> String {
        std::fs::read_to_string(&self.output).unwrap_or_default()
    }

    /// Stop it and reap it.
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a script, with its output in a file.
fn spawn_script(script: &Path, output: &Path) -> Result<JavaSample, String> {
    let log = std::fs::File::create(output).map_err(|error| error.to_string())?;
    let log_err = log.try_clone().map_err(|error| error.to_string())?;

    let child = Command::new(script)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()
        .map_err(|error| format!("could not start {}: {error}", script.display()))?;

    Ok(JavaSample {
        child,
        output: output.to_path_buf(),
    })
}

/// Every scenario a run measures, before ports are assigned.
fn pairings(args: &Args) -> Vec<Pairing> {
    let mut pairings = Vec::new();

    for channel in &args.channels {
        for length in &args.lengths {
            pairings.push(Pairing {
                channel: *channel,
                length: *length,
                ping_uri: String::new(),
                pong_uri: String::new(),
                ping_stream_id: PING_STREAM_ID,
                pong_stream_id: PONG_STREAM_ID,
                messages: args.messages,
                warmup: args.warmup,
            });
        }
    }

    pairings
}

/// One measurement, and who made it.
///
/// The two modes are two tables, not two shapes of one: a latency row is a
/// distribution with percentiles, a throughput row is a rate with the attempts
/// it took to reach it, and printing them as one table would leave every row
/// half empty.
enum Row {
    Latency {
        client: &'static str,
        driver: &'static str,
        channel: Channel,
        length: usize,
        summary: Summary,
    },
    Throughput {
        client: &'static str,
        driver: &'static str,
        channel: Channel,
        length: usize,
        messages: u64,
        seconds: f64,
        attempts: u64,
    },
}

impl Row {
    /// The columns every row starts with.
    fn leading(&self) -> (&'static str, &'static str, Channel, usize) {
        match self {
            Self::Latency {
                client,
                driver,
                channel,
                length,
                ..
            }
            | Self::Throughput {
                client,
                driver,
                channel,
                length,
                ..
            } => (client, driver, *channel, *length),
        }
    }

    /// The row as it goes into `docs/benchmarks.md`.
    fn markdown(&self) -> String {
        let (client, driver, channel, length) = self.leading();
        let label = format!("{length} B");

        match self {
            Self::Latency { summary, .. } => {
                summary.markdown_row(&[client, driver, channel.name(), &label])
            }
            Self::Throughput {
                messages,
                seconds,
                attempts,
                ..
            } => {
                // Bytes, not the label's width: `length` is the payload size the
                // run used, and a column computed from the *text* of the size is
                // a number that looks plausible and is not one.
                let megabytes = *messages as f64 * length as f64 / (1024.0 * 1024.0);
                let [rate, bandwidth, per_message] = [
                    *messages as f64 / seconds,
                    megabytes / seconds,
                    *attempts as f64 / *messages as f64,
                ];

                format!(
                    "| {client} | {driver} | {} | {label} | {rate:.0} | {bandwidth:.0} | \
                     {per_message:.2} | {messages} |",
                    channel.name()
                )
            }
        }
    }

    /// The row as it reads to a person.
    fn line(&self) -> String {
        let (client, driver, channel, length) = self.leading();

        match self {
            Self::Latency { summary, .. } => format!(
                "{client}, {driver} on {}, {length} B\n  {summary}",
                channel.name()
            ),
            Self::Throughput {
                messages,
                seconds,
                attempts,
                ..
            } => format!(
                "{client}, {driver} on {}, {length} B\n  {:.0} msg/s, {:.0} MB/s, \
                 {:.2} attempts per message, {messages} measured",
                channel.name(),
                *messages as f64 / seconds,
                *messages as f64 * length as f64 / seconds / (1024.0 * 1024.0),
                *attempts as f64 / *messages as f64,
            ),
        }
    }

    /// What the table's header is, for the mode these rows are.
    fn markdown_header(rows: &[Row]) -> (&'static str, &'static str) {
        if rows.iter().any(|row| matches!(row, Row::Throughput { .. })) {
            (
                "| client | driver | channel | length | messages/s | MB/s | attempts/msg | n |",
                "|---|---|---|---|---|---|---|---|",
            )
        } else {
            (
                "| client | driver | channel | length | p50 | p90 | p99 | p99.9 | max | n |",
                "|---|---|---|---|---|---|---|---|---|---|",
            )
        }
    }
}

/// Print what was measured, in the shape the caller asked for.
fn report(args: &Args, rows: &[Row], driver: &Driver) {
    if args.markdown {
        let (header, rule) = Row::markdown_header(rows);
        println!("{header}");
        println!("{rule}");
    }

    for row in rows {
        if args.markdown {
            println!("{}", row.markdown());
        } else {
            println!("{}", row.line());
        }
    }

    // The driver binary is part of the record, not a detail: `cargo bench` is
    // a release build, and the interop suite's driver is a debug one, so which
    // binary produced these numbers is the difference between a measurement
    // and a mistake.
    println!();
    println!(
        "driver {} ({}, {})",
        driver.binary.display(),
        driver.name(),
        built_when(&driver.binary).unwrap_or_else(|| "build time unknown".to_owned())
    );
    println!("{}", MachineFingerprint::capture());
}

/// The echoing side: publish back whatever arrives, unchanged.
///
/// Run as a child of the measuring side rather than as a binary of its own so
/// that the two ends are one build, and so the arguments that pair them are
/// written once. It spins rather than sleeping: a mirror that slept would add
/// its sleep to every sample, and the sample is supposed to be the machine's,
/// not the harness's.
fn echo(child: &ChildArgs) -> Result<(), String> {
    let (mut client, subscription, publication) = connect(child)?;

    await_image(&mut client, subscription, SETUP_TIMEOUT).ok_or_else(|| {
        format!(
            "no image on {} — nothing is publishing to echo",
            child.ping_uri
        )
    })?;

    announce("echo ready")?;

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

/// The counting side: publish nothing, report how much arrives.
fn sink(child: &ChildArgs) -> Result<(), String> {
    let count = child
        .count
        .ok_or_else(|| "--sink needs a --count".to_owned())?;
    let (mut client, subscription, _) = connect(child)?;

    await_image(&mut client, subscription, SETUP_TIMEOUT)
        .ok_or_else(|| format!("no image on {} — nothing is publishing", child.ping_uri))?;

    announce("sink ready")?;

    let mut received = 0_u64;

    while received < count {
        client.poll();
        client.poll_subscription(subscription, MESSAGE_LIMIT, |_| received += 1);
    }

    announce(&format!("received {received}"))?;

    Ok(())
}

/// Connect a child to the driver, and create the two resources it needs.
///
/// Both modes create both a subscription and a publication even though a sink
/// publishes nothing: the publication is what makes the *other* side's status
/// messages have somewhere to come from, and a sink that did not create one
/// would leave the publisher's window closed forever.
fn connect(child: &ChildArgs) -> Result<(Client, i64, i64), String> {
    let mut client = Client::connect(&child.aeron_dir).map_err(|error| {
        format!(
            "could not connect to {}: {error}",
            child.aeron_dir.display()
        )
    })?;

    let subscription = client
        .add_subscription(&child.ping_uri, child.ping_stream_id, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no subscription on {}: {error}", child.ping_uri))?;
    let publication = client
        .add_publication(&child.pong_uri, child.pong_stream_id, DEFAULT_TIMEOUT)
        .map_err(|error| format!("no publication on {}: {error}", child.pong_uri))?;

    Ok((client, subscription, publication))
}

/// Say one line on stdout, and make sure it left.
fn announce(line: &str) -> Result<(), String> {
    println!("{line}");
    std::io::stdout().flush().map_err(|error| error.to_string())
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
    pairing: &Pairing,
) -> Result<u64, String> {
    let stamp = monotonic_nano_time();
    let payload = pairing.payload(stamp);

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

/// A process of this same binary, at the other end of the measurement.
struct FarEnd {
    process: Process,
    lines: mpsc::Receiver<String>,
}

impl FarEnd {
    /// Start the echoing peer.
    fn echo(aeron_dir: &Path, pairing: &Pairing) -> Result<Self, String> {
        Self::spawn(aeron_dir, pairing, "--echo", None)
    }

    /// Start the counting peer.
    fn sink(aeron_dir: &Path, pairing: &Pairing, count: u64) -> Result<Self, String> {
        Self::spawn(aeron_dir, pairing, "--sink", Some(count))
    }

    /// Start a peer and a thread that reads its lines.
    ///
    /// The two channel URIs are passed down rather than recomputed: they carry
    /// ports derived from the *parent's* process id, and a child that derived
    /// its own would listen somewhere else entirely.
    fn spawn(
        aeron_dir: &Path,
        pairing: &Pairing,
        mode: &str,
        count: Option<u64>,
    ) -> Result<Self, String> {
        let exe = std::env::current_exe()
            .map_err(|error| format!("could not find this harness's own binary: {error}"))?;

        let mut command = Command::new(exe);
        command
            .arg(mode)
            .arg(aeron_dir)
            .arg("--ping")
            .arg(&pairing.ping_uri)
            .arg("--pong")
            .arg(&pairing.pong_uri)
            .arg("--ping-stream")
            .arg(pairing.ping_stream_id.to_string())
            .arg("--pong-stream")
            .arg(pairing.pong_stream_id.to_string());

        if let Some(count) = count {
            command.arg("--count").arg(count.to_string());
        }

        let mut process = command
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|error| format!("could not start the {mode} peer: {error}"))?;

        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| format!("the {mode} peer has no stdout to read"))?;
        let (sender, lines) = mpsc::channel();

        // Reading blocks until the peer says something, so it cannot run on
        // this thread — [`Peer::await_line`]'s timeout is what bounds it.
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });

        Ok(Self { process, lines })
    }

    /// Wait for a line beginning with `prefix`.
    fn await_line(&mut self, prefix: &str, within: Duration) -> Result<String, String> {
        match self.lines.recv_timeout(within) {
            Ok(line) if line.starts_with(prefix) => Ok(line),
            Ok(line) => Err(format!("the peer said {line:?}, not {prefix:?}")),
            Err(_) => Err(format!("the peer never said {prefix:?} within {within:?}")),
        }
    }

    /// Stop the peer and reap it.
    fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

impl Drop for FarEnd {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A driver this run started, and which build it is.
struct Driver {
    process: ReferenceDriver,
    binary: PathBuf,
    which: WhichDriver,
}

impl Driver {
    /// Start the driver the caller asked for.
    fn start(args: &Args) -> Result<Self, String> {
        let binary = match args.driver {
            WhichDriver::Own => resolve_own_driver(args.driver_binary.as_deref())?,
            WhichDriver::Reference => {
                deepmsg_tests::driver::locate_verified().ok_or_else(missing_reference_driver)?
            }
            // The Java driver is launched by class name, so what the spawner is
            // given is a script that puts the property arguments where a JVM
            // wants them — before the class name, not after it.
            WhichDriver::ReferenceJava => java_driver_script(&java_classpath()?)?,
        };

        // The reference driver is not spawned by name on its own: it is the
        // same spawner either way, which is what keeps the two drivers'
        // configuration from drifting apart.
        let process = ReferenceDriver::start_with(&binary, "bench-latency", &[])
            .map_err(|error| format!("could not start {}: {error}", binary.display()))?;

        Ok(Self {
            process,
            binary,
            which: args.driver,
        })
    }

    /// The name the driver is reported under.
    fn name(&self) -> &'static str {
        match self.which {
            WhichDriver::Own => "ours",
            WhichDriver::Reference => "reference",
            WhichDriver::ReferenceJava => "reference (Java)",
        }
    }

    /// The aeron directory it owns.
    fn aeron_dir(&self) -> &Path {
        self.process.aeron_dir()
    }

    /// Wait for its CnC file.
    fn await_cnc(&mut self) -> Result<(), String> {
        self.process
            .await_cnc(SETUP_TIMEOUT)
            .map(|_| ())
            .map_err(|error| {
                format!(
                    "{} never published a usable CnC file: {error}",
                    self.binary.display()
                )
            })
    }
}

/// Where our own driver binary is: the **release** one, because `cargo bench`
/// is a release build, and a release client measured against a debug driver is
/// a number about nothing.
///
/// The order is: what the caller named, then `DEEPMSG_DRIVER` (the name the
/// interop suite uses for the same purpose), then this workspace's own release
/// output. A debug binary is deliberately not a fallback — the interop suite's
/// default is `target/debug`, and silently taking it here would mix profiles.
fn resolve_own_driver(explicit: Option<&Path>) -> Result<PathBuf, String> {
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

/// What is missing when the reference's own instrument was asked for.
fn missing_sample(name: &str) -> String {
    format!(
        "the reference's {name} was not found — it is built beside the reference driver \
         ({}; see docs/reference.md), or named with DEEPMSG_REF_{}",
        samples::SAMPLES_DIR,
        name.to_uppercase()
    )
}

/// What is missing when the reference driver was asked for.
fn missing_reference_driver() -> String {
    "no reference driver was found — the Aeron 1.53.2 checkout beside this one is \
     expected to have a built `aeronmd` (see docs/reference.md)"
        .to_owned()
}

/// How long ago a binary was built.
///
/// Two runs whose numbers differ are usually two different builds, and a
/// fingerprint that could not say *which* build it measured would leave that
/// indistinguishable from noise.
fn built_when(binary: &Path) -> Option<String> {
    let modified = std::fs::metadata(binary).ok()?.modified().ok()?;
    let age = SystemTime::now().duration_since(modified).ok()?;

    Some(format!("built {}s ago", age.as_secs()))
}

/// One scenario, on two channels of its own.
///
/// The pair matters as much as the scenario: a round trip needs a channel in
/// each direction, because a unicast subscriber binds the endpoint it is given
/// and one port cannot be bound twice.
struct Pairing {
    channel: Channel,
    length: usize,
    ping_uri: String,
    pong_uri: String,
    ping_stream_id: i32,
    pong_stream_id: i32,
    messages: u64,
    warmup: u64,
}

impl Pairing {
    /// The same scenario on a different pair of ports.
    ///
    /// Ports come from a range derived from this process's id rather than fixed
    /// ones, the same way `tests/interop/udp_transport.rs` picks its own: two
    /// runs on one machine collide only if something else chose exactly this
    /// range, and nothing has to be cleaned up between runs.
    fn at(&self, index: usize) -> Self {
        let base = 20_000 + (std::process::id() % 20_000) as u16;
        let first = base.saturating_add((index * 2) as u16);

        Self {
            channel: self.channel,
            length: self.length,
            ping_uri: self.channel.uri(first),
            pong_uri: self.channel.uri(first.saturating_add(1)),
            // Streams vary per row for the reason the constant's own note
            // gives: an IPC row's channel is always `aeron:ipc`.
            ping_stream_id: PING_STREAM_ID + (index * 2) as i32,
            pong_stream_id: PONG_STREAM_ID + (index * 2) as i32,
            messages: self.messages,
            warmup: self.warmup,
        }
    }

    /// How a failure names this row.
    fn label(&self) -> String {
        format!("{} {} B", self.channel.name(), self.length)
    }

    /// A payload of this scenario's length, with `stamp` in its first bytes.
    fn payload(&self, stamp: i64) -> Vec<u8> {
        Scenario {
            channel: self.channel,
            length: self.length,
        }
        .payload(stamp)
    }
}
