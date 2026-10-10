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
use deepmsg_bench::loadtest::transport::echo::{EchoTransceiver, PollGate};
use deepmsg_bench::loadtest::transport::util::ChannelSettings;

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
/// namespace means a properties file cannot turn it on by accident. Unset means
/// the rig as it has always been — `Client::poll` on every `receive()`. A value
/// of `N` where `N > 0` runs the duty cycle **at most once every N nanoseconds**
/// of the measured thread's monotonic clock, which is the shape the A/B's
/// "after" arm asks for: a `receive()` where the driver half of the cycle is not
/// run on every turn.
///
/// It is a measurement and not a setting — `poll` carries liveness, the driver's
/// replies and `expire_pending` (`crates/client/src/client.rs:970-986`), so a
/// run with this set is a run that runs the duty cycle less often than the rig
/// has always run it — and an unset, empty, non-numeric or zero value is the
/// rig's own shape, so the only way to change a run is to say so.
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
        Transceiver::Echo => {
            let channels =
                ChannelSettings::from_properties(&settings).map_err(|error| error.to_string())?;
            let client_poll_min_ns = poll_min_ns(std::env::var(CLIENT_POLL_MIN_NS).ok().as_deref());
            let gate = if client_poll_min_ns > 0 {
                PollGate::EveryInterval(client_poll_min_ns)
            } else {
                PollGate::Every
            };

            let transceiver = EchoTransceiver::new(channels, idle, logs_directory, gate)
                .map_err(|error| error.to_string())?;

            measure(configuration, transceiver)?;
        }
    }

    std::io::stdout().flush().map_err(|error| error.to_string())
}

/// The interval between the client's duty cycles, in nanoseconds.
///
/// A value of [`CLIENT_POLL_MIN_NS`], which is a literal decimal number and
/// nothing else: an unset, empty, non-numeric or zero value is `0` — the duty
/// cycle on every `receive()` — so a mistyped switch leaves the rig exactly as
/// it was and the only way to change a run is to say so.
fn poll_min_ns(value: Option<&str>) -> u64 {
    match value {
        Some(value) if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
            value.parse::<u64>().unwrap_or(0)
        }
        _ => 0,
    }
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

    /// The interval is 0 — the rig's own shape — unless a literal number says
    /// otherwise. An unset variable, anything a caller might mean by "off", and
    /// anything that is not a plain decimal number all leave the duty cycle on
    /// every `receive()`.
    #[test]
    fn the_interval_switch_is_zero_unless_a_number_says_otherwise() {
        for (value, expected) in [
            (None, 0),
            (Some(""), 0),
            (Some("0"), 0),
            (Some("1"), 1),
            (Some("1000000"), 1_000_000),
            (Some(" 1000000"), 0),
            (Some("1000000 "), 0),
            (Some("+1000000"), 0),
            (Some("-1"), 0),
            (Some("1.0"), 0),
            (Some("0x10"), 0),
            (Some("one"), 0),
            (Some("99999999999999999999999"), 0),
        ] {
            assert_eq!(poll_min_ns(value), expected, "for {value:?}");
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
