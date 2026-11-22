//! The rig, as a program: the reference's `LoadTestRig.main`.
//!
//! Arguments are the reference's, because a run is described to both sides the
//! same way: `-Dname=value` for what a JVM would call a system property, and
//! properties file paths for the rest. The files are read in order — a later one
//! wins — and the `-D` arguments win over all of them, which is what
//! `mergeWithSystemProperties(PRESERVE, …)` means in `LoadTestRig.java:405`.
//!
//! One argument is this build's own: `--print-config` parses everything, prints
//! the configuration and stops. It exists so that a command line can be checked
//! against the reference's without starting a driver, and
//! `analysis/bench/config-spike/compare-config.sh` is what uses it.
//!
//! The exit status is zero even when the run's status is [`Status::Fail`], which
//! is the reference's behaviour: a failed run is marked in the *file name* —
//! `<prefix>.hdr.FAIL` — and a caller reads that rather than the exit code.

use std::io::Write as _;
use std::path::Path;
use std::process::ExitCode;

use deepmsg_bench::loadtest::config::{Configuration, Properties, Transceiver};
use deepmsg_bench::loadtest::in_memory::InMemoryTransceiver;
use deepmsg_bench::loadtest::progress::Reporter;
use deepmsg_bench::loadtest::recorder::{self, Recorder};
use deepmsg_bench::loadtest::result;
use deepmsg_bench::loadtest::rig::LoadTestRig;
use deepmsg_bench::loadtest::transceiver::{MessageTransceiver, SystemClock};
use deepmsg_bench::loadtest::transport::echo::EchoTransceiver;
use deepmsg_bench::loadtest::transport::live_recording::LiveRecordingTransceiver;
use deepmsg_bench::loadtest::transport::live_replay::LiveReplayTransceiver;
use deepmsg_bench::loadtest::transport::util::{ChannelSettings, PollGate, archive_context};

/// What this program accepts.
const USAGE: &str = "\
usage: loadtest-rig [--print-config] [-Dname=value ...] [file.properties ...]

  --print-config   parse the arguments, print the configuration, and stop
  -Dname=value     a setting, which wins over any properties file given here
  file.properties  settings from a file, later files winning over earlier ones

  The settings and their defaults are the reference's, and
  analysis/bench/deepmsg-rust-loadtestrig-plan.md §2.5 lists them.
";

/// The shortest interval between the inline client's duty cycles, as an
/// environment variable, in nanoseconds.
///
/// This build's own switch, and deliberately not a setting: it changes what the
/// rig *is* rather than what it measures, and keeping it out of the property
/// namespace means a properties file cannot turn it on by accident.
///
/// * **unset or empty** — the default: the duty cycle at most once a
///   millisecond ([`DEFAULT_GATE_NS`]), which is the shape the reference's rig
///   has because its conductor runs on a thread of its own;
/// * **`0`** — [`PollGate::Every`]: `Client::poll` on every `receive()`, the
///   shape the rig ran before the switch existed;
/// * **a positive integer `N`** — [`PollGate::EveryInterval`]: at most once
///   every `N` nanoseconds of the measured thread's monotonic clock;
/// * **anything else** — an error, so a mistyped switch cannot silently name
///   the wrong arm.
///
/// It is a measurement and not a setting — `poll` carries liveness, the driver's
/// replies and `expire_pending` (`crates/client/src/client.rs:970-986`), so a
/// run with this set is a run that runs the duty cycle less often than the rig
/// has always run it.
const CLIENT_POLL_MIN_NS: &str = "DEEPMSG_RIG_CLIENT_POLL_MIN_NS";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("loadtest-rig: {message}");
            ExitCode::FAILURE
        }
    }
}

/// What a command line asked for.
#[derive(Debug)]
enum Asked {
    /// Print the usage and stop.
    Help,
    /// Print the configuration the settings describe and stop.
    PrintConfig(Configuration),
    /// Run it, with the settings it was built from — a transceiver that talks
    /// to a driver needs the channel settings too, and those are read on the way
    /// in rather than eagerly: an in-memory run needs no driver and no
    /// directory.
    Run(Configuration, Properties),
}

/// Everything a command line says.
///
/// Separated from [`run`] so that it can be tested: it is the whole of this
/// program's interface to the scripts that drive it, and a mistake in it looks
/// exactly like a mistake in a setting.
fn parse(arguments: impl Iterator<Item = String>) -> Result<Asked, String> {
    let mut command_line = Properties::new();
    let mut from_files = Properties::new();
    let mut print_config = false;

    for argument in arguments {
        match argument.as_str() {
            "--print-config" => print_config = true,
            "--help" | "-h" => return Ok(Asked::Help),
            _ if argument.starts_with("-D") => {
                let (name, value) = argument[2..]
                    .split_once('=')
                    .ok_or_else(|| format!("-D wants name=value, and '{argument}' has no '='"))?;
                command_line.set(name, value);
            }
            _ if argument.starts_with('-') => {
                return Err(format!("unknown option '{argument}'\n\n{USAGE}"));
            }
            _ => from_files
                .load_file(Path::new(&argument))
                .map_err(|error| error.to_string())?,
        }
    }

    command_line.merge_keeping_existing(&from_files);

    let configuration =
        Configuration::from_properties(&command_line).map_err(|error| error.to_string())?;

    Ok(if print_config {
        Asked::PrintConfig(configuration)
    } else {
        Asked::Run(configuration, command_line)
    })
}

fn run() -> Result<(), String> {
    let configuration = match parse(std::env::args().skip(1))? {
        Asked::Help => {
            print!("{USAGE}");
            return Ok(());
        }
        Asked::PrintConfig(configuration) => {
            println!("{configuration}");
            return Ok(());
        }
        Asked::Run(configuration, settings) => (configuration, settings),
    };
    let (configuration, settings) = configuration;

    // Everything the configuration decides is read off it before it is moved
    // into the transceiver or the rig.
    let idle = configuration.idle_strategy();
    let kind = configuration.transceiver();
    let logs_directory = configuration.logs_directory().to_path_buf();

    match kind {
        Transceiver::InMemory => measure(configuration, InMemoryTransceiver::new())?,
        Transceiver::LiveReplay => {
            let channels =
                ChannelSettings::from_properties(&settings).map_err(|error| error.to_string())?;
            let context = archive_context(&settings, &channels.directory);

            let transceiver = LiveReplayTransceiver::new(channels, idle, &context)
                .map_err(|error| error.to_string())?;

            measure(configuration, transceiver)?;
        }
        Transceiver::LiveRecording => {
            let channels =
                ChannelSettings::from_properties(&settings).map_err(|error| error.to_string())?;
            let context = archive_context(&settings, &channels.directory);

            let transceiver =
                LiveRecordingTransceiver::new(channels, idle, &context, configured_gate()?)
                    .map_err(|error| error.to_string())?;

            measure(configuration, transceiver)?;
        }
        Transceiver::Echo => {
            let channels =
                ChannelSettings::from_properties(&settings).map_err(|error| error.to_string())?;

            let transceiver =
                EchoTransceiver::new(channels, idle, logs_directory, configured_gate()?)
                    .map_err(|error| error.to_string())?;

            measure(configuration, transceiver)?;
        }
    }

    std::io::stdout().flush().map_err(|error| error.to_string())
}

/// The interval an unset switch gives, in nanoseconds.
///
/// The reference's conductor runs on a thread of its own (`Aeron.java:174`,
/// `AgentRunner.startOnThread`), so its measured thread never runs a duty cycle
/// at all; this is the interval that gives this build's inline conductor the
/// same shape without taking it off the measured thread. A literal `0` asks for
/// the rig's old shape — every `receive()` — instead.
const DEFAULT_GATE_NS: u64 = 1_000_000;

/// The gate a value of [`CLIENT_POLL_MIN_NS`] asks for.
///
/// Unset or empty is [`DEFAULT_GATE_NS`]; a literal decimal `0` is
/// [`PollGate::Every`]; a positive decimal `N` is
/// [`PollGate::EveryInterval`]. Anything else is an error: a mistyped switch
/// silently naming the wrong arm is exactly what this switch exists to avoid.
///
/// # Errors
///
/// When the value is not a decimal number of nanoseconds.
fn poll_gate(value: Option<&str>) -> Result<PollGate, String> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(PollGate::EveryInterval(DEFAULT_GATE_NS));
    };

    // A literal decimal number and nothing else: `str::parse` would also take a
    // leading `+`, and a switch that is a number with a sign is a value no doc
    // here names.
    let parsed = value
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| value.parse::<u64>())
        .and_then(Result::ok);
    let Some(min_ns) = parsed else {
        return Err(format!(
            "{CLIENT_POLL_MIN_NS}={value} is not a decimal number of nanoseconds"
        ));
    };

    Ok(if min_ns == 0 {
        PollGate::Every
    } else {
        PollGate::EveryInterval(min_ns)
    })
}

/// The gate the environment asks for, read once for both transceivers.
///
/// `LiveRecordingTransceiver` needs the same gate as `EchoTransceiver`: its
/// `receive` is the only duty cycle the measured thread runs in that shape, and
/// the driver reaps a client whose heartbeat stops. Reading it here keeps the
/// two branches from drifting apart.
fn configured_gate() -> Result<PollGate, String> {
    poll_gate(std::env::var(CLIENT_POLL_MIN_NS).ok().as_deref())
}

/// Run one measurement, whoever is under test.
///
/// A function rather than a value so that the two transceivers do not need a
/// wrapper to share the rig: the rig is generic and this is the one place that
/// has to name a clock.
fn measure<T>(configuration: Configuration, transceiver: T) -> Result<(), String>
where
    T: MessageTransceiver<SystemClock>,
{
    let idle = configuration.idle_strategy();
    let progress = Reporter::of(&configuration, std::io::stdout());

    let mut rig = LoadTestRig::new(
        configuration,
        transceiver,
        Recorder::new(result::histogram(), recorder::checksum(), SystemClock),
        idle,
        progress,
        std::io::stdout(),
    );

    rig.run().map_err(|error| error.to_string())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asked(arguments: &[&str]) -> Result<Asked, String> {
        parse(arguments.iter().map(|argument| (*argument).to_owned()))
    }

    fn settings() -> Vec<&'static str> {
        vec![
            "-Dio.aeron.benchmarks.message.transceiver=in-memory",
            "-Dio.aeron.benchmarks.message.rate=1M",
            "-Dio.aeron.benchmarks.output.file=test",
            "-Dio.aeron.benchmarks.output.directory=/tmp/deepmsg-bench-cli",
        ]
    }

    fn configuration(arguments: &[&str]) -> Configuration {
        match asked(arguments).expect("the arguments are usable") {
            Asked::Run(configuration, _) | Asked::PrintConfig(configuration) => configuration,
            Asked::Help => panic!("these arguments do not ask for help"),
        }
    }

    #[test]
    fn an_echo_transceiver_is_named_and_named_by_one_name() {
        let mut arguments = settings();
        arguments[0] = "-Dio.aeron.benchmarks.message.transceiver=echo";

        assert_eq!(configuration(&arguments).transceiver(), Transceiver::Echo);
        assert_eq!(Transceiver::Echo.name(), "echo");
        assert!(
            Transceiver::parse("echo-ipc").is_err(),
            "the transport is the channel's business"
        );
    }

    #[test]
    fn a_command_line_of_settings_is_a_run() {
        let configuration = configuration(&settings());

        assert_eq!(configuration.message_rate(), 1_000_000);
        assert_eq!(
            configuration.output_file_name_prefix(),
            "test_rate=1M_batch=1_length=16"
        );
    }

    #[test]
    fn print_config_is_a_configuration_and_no_run() {
        let mut arguments = vec!["--print-config"];
        arguments.extend(settings());

        assert!(matches!(asked(&arguments), Ok(Asked::PrintConfig(_))));
    }

    #[test]
    fn help_is_help_wherever_it_appears() {
        assert!(matches!(
            asked(&["-Dio.aeron.benchmarks.message.rate=1", "--help"]),
            Ok(Asked::Help)
        ));
        assert!(matches!(asked(&["-h"]), Ok(Asked::Help)));
    }

    /// A `-D` wins over a properties file beside it, which is
    /// `mergeWithSystemProperties(PRESERVE, …)` and is the order the reference's
    /// own scripts rely on.
    #[test]
    fn a_setting_on_the_command_line_beats_the_file_it_was_given_with() {
        let directory =
            std::env::temp_dir().join(format!("deepmsg-bench-cli-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("the scratch directory can be made");
        let file = directory.join("benchmark.properties");
        std::fs::write(
            &file,
            "io.aeron.benchmarks.message.transceiver=in-memory\n\
             io.aeron.benchmarks.message.rate=2K\n\
             io.aeron.benchmarks.output.file=test\n\
             io.aeron.benchmarks.output.directory=/tmp/deepmsg-bench-cli\n",
        )
        .expect("written");

        let configuration = configuration(&[
            &file.to_string_lossy(),
            "-Dio.aeron.benchmarks.message.rate=1M",
        ]);

        assert_eq!(configuration.message_rate(), 1_000_000, "the -D wins");
        assert_eq!(
            configuration.output_file_name_prefix(),
            "test_rate=1M_batch=1_length=16"
        );

        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The switch's four shapes: unset/empty is the 1 ms default, `0` is every
    /// turn, a positive integer is that interval, and anything else is refused
    /// rather than silently becoming a different arm.
    #[test]
    fn the_interval_switch_names_the_gate_the_docs_do() {
        assert_eq!(Ok(PollGate::Every), poll_gate(Some("0")));
        assert_eq!(Ok(PollGate::Every), poll_gate(Some("000")));
        assert_eq!(
            Ok(PollGate::EveryInterval(1_000_000)),
            poll_gate(Some("1000000"))
        );
        assert_eq!(
            Ok(PollGate::EveryInterval(DEFAULT_GATE_NS)),
            poll_gate(None)
        );
        assert_eq!(
            Ok(PollGate::EveryInterval(DEFAULT_GATE_NS)),
            poll_gate(Some(""))
        );

        for value in [
            " 1000000",
            "1000000 ",
            "+1000000",
            "-1",
            "1.0",
            "0x10",
            "one",
            "1ms",
            "99999999999999999999999",
        ] {
            let error = poll_gate(Some(value)).expect_err("refused");
            assert!(error.contains(CLIENT_POLL_MIN_NS), "{error}");
            assert!(error.contains(value), "{error}");
        }
    }

    #[test]
    fn a_malformed_setting_says_what_it_wanted() {
        let error = asked(&["-Dmessage.rate"]).expect_err("refused");

        assert!(error.contains("name=value"), "{error}");
    }

    #[test]
    fn an_unknown_option_is_refused_rather_than_ignored() {
        let error = asked(&["--quiet"]).expect_err("refused");

        assert!(error.contains("unknown option '--quiet'"), "{error}");
    }

    #[test]
    fn a_missing_required_setting_names_it() {
        let error = asked(&["-Dio.aeron.benchmarks.message.rate=1M"]).expect_err("refused");

        assert!(error.contains("message.transceiver"), "{error}");
    }
}
