//! Inspect a CnC file — live or from a driver that has crashed.
//!
//! The counterpart of the reference's CnC inspection tooling (M15). It is the
//! first consumer of `deepmsg-cnc`'s reader API, so it is also a check on that
//! API: a tool asks for the things an operator wants, which is not quite the
//! order the tests ask for them in.
//!
//! Reads only. It never opens the CnC file for writing, and it never needs the
//! driver to be alive: a crashed driver leaves its file behind, and inspecting
//! that is half the point.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use deepmsg_cnc::{CNC_FILE_NAME, CncFile, CncOpenError};

const USAGE: &str = "\
usage: cnc-dump [OPTIONS] [AERON_DIR]

Inspect the CnC file in an aeron directory.

  AERON_DIR        directory holding cnc.dat
                   (default: $AERON_DIR, else /dev/shm/aeron-$USER)

  --counters       list the counter catalogue
  --errors         read the distinct error log
  --all            both of the above
  -h, --help       show this message

Exits 0 on success, 1 if the CnC file cannot be read, 2 on a usage error.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let options = match Options::parse(&args) {
        Ok(Some(options)) => options,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("cnc-dump: {message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    match dump(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("cnc-dump: {message}");
            ExitCode::FAILURE
        }
    }
}

struct Options {
    aeron_dir: PathBuf,
    counters: bool,
    errors: bool,
}

impl Options {
    /// `Ok(None)` means the help text was asked for.
    fn parse(args: &[String]) -> Result<Option<Self>, String> {
        let mut aeron_dir = None;
        let mut counters = false;
        let mut errors = false;

        for arg in args {
            match arg.as_str() {
                "-h" | "--help" => return Ok(None),
                "--counters" => counters = true,
                "--errors" => errors = true,
                "--all" => {
                    counters = true;
                    errors = true;
                }
                other if other.starts_with('-') => {
                    return Err(format!("unknown option `{other}`"));
                }
                other => {
                    if aeron_dir.is_some() {
                        return Err("more than one aeron directory given".to_string());
                    }
                    aeron_dir = Some(PathBuf::from(other));
                }
            }
        }

        Ok(Some(Self {
            aeron_dir: aeron_dir.unwrap_or_else(default_aeron_dir),
            counters,
            errors,
        }))
    }
}

/// Where the driver would have put its directory if nobody said otherwise.
///
/// `AERON_DIR` first, because that is what the driver itself honours; then the
/// reference's own default, `/dev/shm/aeron-<user>`
/// (`aeron-client/src/main/c/util/aeron_fileutil.c:1474`). Falling back to the
/// username *we* are running as is not exactly what the driver does — it uses
/// the user that started *it* — but the two agree whenever a tool is run by
/// the same person who started the driver, which is the case worth defaulting
/// for.
fn default_aeron_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("AERON_DIR") {
        return PathBuf::from(dir);
    }

    let user = std::env::var("USER").unwrap_or_else(|_| "default".to_string());
    PathBuf::from(format!("/dev/shm/aeron-{user}"))
}

fn dump(options: &Options) -> Result<(), String> {
    // `try_open` and not `open`: waiting for a driver that is not coming is
    // the wrong behaviour for a tool someone typed at a shell.
    let cnc = CncFile::try_open(&options.aeron_dir)
        .map_err(|error| describe_open_failure(&options.aeron_dir, &error))?;

    let metadata = cnc.metadata();
    let now = now_ms();

    println!("{}", cnc.path().display());
    field("length", format!("{} bytes", cnc.file_length()));
    field(
        "cnc version",
        format!(
            "{} ({})",
            deepmsg_core::version::format_version(cnc.cnc_version()),
            cnc.cnc_version()
        ),
    );
    field("driver pid", metadata.pid);
    field(
        "started",
        format!("{} ms since the epoch", metadata.start_timestamp_ms),
    );
    field(
        "liveness timeout",
        format!("{} ns", metadata.client_liveness_timeout_ns),
    );
    field("page size", metadata.file_page_size);

    println!("\nregions");
    let regions = cnc.layout();
    for (name, range) in [
        ("to-driver", &regions.to_driver),
        ("to-clients", &regions.to_clients),
        ("counters-metadata", &regions.counters_metadata),
        ("counters-values", &regions.counters_values),
        ("error-log", &regions.error_log),
    ] {
        println!(
            "  {name:<18} {:>10} .. {:<10} {:>12} bytes",
            range.start,
            range.end,
            range.len()
        );
    }

    println!("\ndriver");
    match cnc.consumer_heartbeat_ms() {
        Some(heartbeat) => {
            field("heartbeat", format!("{} ms since the epoch", heartbeat));
            field("heartbeat age", describe_age(now - heartbeat));
        }
        None => field("heartbeat", "unreadable"),
    }
    let timeout_ms = metadata.client_liveness_timeout_ns / 1_000_000;
    field(
        "status",
        if cnc.driver_is_active(now, timeout_ms) {
            format!("ACTIVE (heartbeat within {timeout_ms} ms)")
        } else {
            format!("INACTIVE (heartbeat older than {timeout_ms} ms, or a clean shutdown)")
        },
    );

    if options.counters {
        dump_counters(&cnc);
    }

    if options.errors {
        dump_errors(&cnc);
    }

    Ok(())
}

fn dump_counters(cnc: &CncFile) {
    let Some(counters) = cnc.counters() else {
        println!("\ncounters\n  (the counter regions are unreachable)");
        return;
    };

    let mut rows: Vec<(i32, i32, i64, i64, String)> = Vec::new();
    let scan = counters.for_each(|counter| {
        rows.push((
            counter.counter_id,
            counter.type_id,
            counter.value,
            counter.registration_id,
            counter.label.clone(),
        ));
    });

    println!(
        "\ncounters\n  {} allocated, {} reclaimed, {} in an unknown state (max id {})",
        scan.allocated,
        scan.reclaimed,
        scan.unknown_state,
        counters.max_counter_id()
    );
    // Named inline arguments rather than format arguments: the column titles
    // are literals, and passing a literal to `{}` is `clippy::print_literal`.
    let (id, ty, val, reg) = ("id", "type", "value", "registration");
    println!("  {id:>4}  {ty:>4}  {val:>20}  {reg:>14}  label");

    for (id, type_id, value, registration_id, label) in &rows {
        println!("  {id:>4}  {type_id:>4}  {value:>20}  {registration_id:>14}  {label}");
    }
}

fn dump_errors(cnc: &CncFile) {
    let Some(reader) = cnc.error_log() else {
        println!("\nerrors\n  (the error-log region is unreachable)");
        return;
    };

    if !reader.has_entries() {
        println!("\nerrors\n  none recorded");
        return;
    }

    let mut entries = Vec::new();
    let scan = reader.read(i64::MIN, &mut entries);

    println!("\nerrors\n  {} entries", scan.entries);
    for entry in &entries {
        println!(
            "  [{} observation(s), last {} ms since the epoch]",
            entry.observation_count, entry.last_observation_timestamp_ms
        );
        for line in entry.text.lines() {
            println!("    {line}");
        }
    }

    if scan.malformed > 0 {
        println!(
            "  {} malformed entry -- the log is truncated",
            scan.malformed
        );
    }
    if scan.truncated {
        println!("  the log ends mid-entry, so the region is smaller than the writer believed");
    }
}

/// Turn an open failure into something worth reading.
///
/// The distinctions matter to whoever is holding the shell: "there is no
/// driver here" and "there is a driver here but its file is not ready" want
/// different reactions, and the reader already tells them apart.
fn describe_open_failure(aeron_dir: &std::path::Path, error: &CncOpenError) -> String {
    let path = aeron_dir.join(CNC_FILE_NAME);

    match error {
        CncOpenError::Io(io) if std::io::ErrorKind::NotFound == io.kind() => format!(
            "no CnC file at {} -- is a driver running with AERON_DIR={}?",
            path.display(),
            aeron_dir.display()
        ),
        CncOpenError::TooShort { length } => format!(
            "{} is {length} bytes, too short to be a CnC file",
            path.display()
        ),
        CncOpenError::NotReady => format!(
            "{} exists but its metadata is still unpublished -- the driver is starting up",
            path.display()
        ),
        CncOpenError::Incompatible(compatibility) => format!(
            "{} is a CnC file this build cannot read: {compatibility:?} \
             (this build speaks {})",
            path.display(),
            deepmsg_core::version::format_version(deepmsg_core::version::CNC_VERSION)
        ),
        CncOpenError::Malformed(inner) => format!("{} is not usable: {inner}", path.display()),
        CncOpenError::Io(io) => format!("{}: {io}", path.display()),
    }
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // A millisecond timestamp fits in an i64 for the next few hundred million
    // years, and this is a diagnostic tool rather than a contract.
    #[allow(clippy::cast_possible_truncation)]
    let millis = elapsed.as_millis() as i64;
    millis
}

/// A duration in milliseconds, rendered at whatever scale reads best.
fn describe_age(millis: i64) -> String {
    match millis {
        ..0 => format!("{millis} ms in the future"),
        0..1_000 => format!("{millis} ms ago"),
        1_000..60_000 => format!("{:.1} s ago", millis as f64 / 1_000.0),
        60_000..3_600_000 => format!("{:.1} min ago", millis as f64 / 60_000.0),
        _ => format!("{:.1} h ago", millis as f64 / 3_600_000.0),
    }
}

/// One `key: value` line, with the keys aligned.
fn field(key: &str, value: impl std::fmt::Display) {
    println!("  {key:<18} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn help_is_not_an_error() {
        assert!(matches!(Options::parse(&args(&["--help"])), Ok(None)));
        assert!(matches!(Options::parse(&args(&["-h"])), Ok(None)));
    }

    #[test]
    fn parses_flags_and_a_directory() {
        let options = Options::parse(&args(&["/tmp/somewhere", "--counters"]))
            .expect("parses")
            .expect("not help");
        assert_eq!(PathBuf::from("/tmp/somewhere"), options.aeron_dir);
        assert!(options.counters);
        assert!(!options.errors);

        let options = Options::parse(&args(&["--all"]))
            .expect("parses")
            .expect("not help");
        assert!(options.counters && options.errors);
    }

    #[test]
    fn rejects_what_it_does_not_understand() {
        assert!(Options::parse(&args(&["--nope"])).is_err());
        assert!(
            Options::parse(&args(&["one", "two"])).is_err(),
            "two directories is a typo, not a request"
        );
    }

    #[test]
    fn falls_back_to_the_default_directory() {
        // Not asserted against a literal: the default reads the environment,
        // so the only stable property is that it produces *a* path.
        let options = Options::parse(&args(&[]))
            .expect("parses")
            .expect("not help");
        assert!(!options.aeron_dir.as_os_str().is_empty());
    }

    #[test]
    fn renders_ages_at_a_readable_scale() {
        assert_eq!("500 ms ago", describe_age(500));
        assert_eq!("1.5 s ago", describe_age(1_500));
        assert_eq!("2.0 min ago", describe_age(120_000));
        assert_eq!("3.0 h ago", describe_age(10_800_000));
        assert_eq!("-50 ms in the future", describe_age(-50));
    }
}
