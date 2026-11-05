//! The CnC counters, as Prometheus text.
//!
//! An independent process beside the driver: it maps whatever `cnc.dat` is at
//! the configured directory on each scrape and renders the counters the
//! pipeline plan names. Opening per scrape is what makes a driver restart and a
//! reclaimed directory visible without any re-map bookkeeping — the next scrape
//! is the new file.
//!
//! Counters are read through [`deepmsg_cnc::CncFile::counters`]; the stream and
//! recording labels come from the counter **key**, not its label. That is why
//! [`deepmsg_cnc::position_key::StreamPositionKey`] lives in `cnc`: this binary
//! does not link the driver.
//!
//! No third-party dependency, no async runtime: one blocking `TcpListener`,
//! one request per connection.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use deepmsg_archive::server::recording_pos::parse_key as parse_archive_key;
use deepmsg_cnc::CncFile;
use deepmsg_cnc::loss_report::LossReportFile;
use deepmsg_cnc::position_key::StreamPositionKey;
use deepmsg_core::clock;

const USAGE: &str = "\
Usage: deepmsg-counters-exporter [options]

  --dir <path>          the aeron directory (default: DEEPMSG_AERON_DIR)
  --listen <addr>       listen address (default: 127.0.0.1:9101)
  --node-id <name>      node_id label (default: DEEPMSG_NODE_ID)
  -h, --help            print this message
";

const DEFAULT_LISTEN: &str = "127.0.0.1:9101";
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// The system counter ids this build names (`crates/driver/src/system_counters.rs`).
mod system {
    pub const NAK_MESSAGES_SENT: i32 = 5;
    pub const NAK_MESSAGES_RECEIVED: i32 = 6;
    pub const ERRORS: i32 = 15;
    pub const LOSS_GAP_FILLS: i32 = 23;
}

struct Options {
    dir: PathBuf,
    listen: SocketAddr,
    node_id: String,
}

impl Options {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut dir = std::env::var("DEEPMSG_AERON_DIR").ok();
        let mut listen = DEFAULT_LISTEN.to_owned();
        let mut node_id = std::env::var("DEEPMSG_NODE_ID").ok();
        let mut args = args;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    print!("{USAGE}");
                    std::process::exit(0);
                }
                "--dir" => dir = Some(args.next().ok_or("--dir needs a path")?),
                "--listen" => listen = args.next().ok_or("--listen needs an address")?,
                "--node-id" => node_id = Some(args.next().ok_or("--node-id needs a name")?),
                other => return Err(format!("unknown option '{other}'\n\n{USAGE}")),
            }
        }

        let dir = dir.ok_or("no aeron directory: pass --dir or set DEEPMSG_AERON_DIR")?;
        let node_id = node_id.ok_or("no node id: pass --node-id or set DEEPMSG_NODE_ID")?;
        let listen: SocketAddr = listen
            .parse()
            .map_err(|_| format!("--listen is not an address: {listen}"))?;

        Ok(Self {
            dir: PathBuf::from(dir),
            listen,
            node_id,
        })
    }
}

fn main() -> ExitCode {
    let options = match Options::from_args(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("deepmsg-counters-exporter: {message}");
            return ExitCode::FAILURE;
        }
    };

    match serve(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "deepmsg-counters-exporter: could not listen on {}: {error}",
                options.listen
            );
            ExitCode::FAILURE
        }
    }
}

fn serve(options: &Options) -> io::Result<()> {
    let listener = TcpListener::bind(options.listen)?;
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else {
            continue;
        };
        let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
        let _ = handle(&mut stream, options);
    }
    Ok(())
}

fn handle(stream: &mut TcpStream, options: &Options) -> io::Result<()> {
    let mut buffer = [0_u8; 2048];
    let read = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..read]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("");

    let (status, body) = match path {
        "/metrics" => ("200 OK", render(&options.dir, &options.node_id)),
        "/health" => ("200 OK", "ok\n".to_owned()),
        _ => ("404 Not Found", "not found\n".to_owned()),
    };

    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The body of `/metrics`. Public to the tests in this file, not to other
/// binaries.
fn render(dir: &Path, node_id: &str) -> String {
    render_at(dir, node_id, clock::epoch_millis())
}

fn render_at(dir: &Path, node_id: &str, now: i64) -> String {
    let mut families = Families::default();
    let node = escape_label(node_id);

    let Ok(cnc) = CncFile::try_open(dir) else {
        families.sample(
            "deepmsg_driver_up",
            "1 when a CnC file is readable at the configured directory",
            "gauge",
            format!("deepmsg_driver_up{{node_id=\"{node}\"}} 0"),
        );
        return families.render();
    };

    families.sample(
        "deepmsg_driver_up",
        "1 when a CnC file is readable at the configured directory",
        "gauge",
        format!("deepmsg_driver_up{{node_id=\"{node}\"}} 1"),
    );

    if let Some(heartbeat) = cnc.consumer_heartbeat_ms() {
        if heartbeat >= 0 {
            let age = (now - heartbeat) as f64 / 1000.0;
            families.sample(
                "deepmsg_driver_heartbeat_age_seconds",
                "Age of the driver's consumer heartbeat",
                "gauge",
                format!("deepmsg_driver_heartbeat_age_seconds{{node_id=\"{node}\"}} {age:.3}"),
            );
        }
    }

    let Some(counters) = cnc.counters() else {
        return families.render();
    };

    let mut systems: BTreeMap<i32, i64> = BTreeMap::new();
    let mut streams: BTreeMap<(i64, i32, i32), StreamSamples> = BTreeMap::new();
    let mut clusters: BTreeMap<i32, ClusterSamples> = BTreeMap::new();
    let mut recordings: BTreeMap<i64, i64> = BTreeMap::new();
    let mut unknown = 0_u32;

    counters.for_each(|counter| {
        let value = counter.value;
        let Some(key) = counters.key(counter.counter_id) else {
            unknown += 1;
            return;
        };

        match counter.type_id {
            0 => {
                // System counters share type 0; the key is the counter id.
                if let Ok(bytes) = <[u8; 4]>::try_from(&key[..4]) {
                    systems.insert(i32::from_le_bytes(bytes), value);
                } else {
                    unknown += 1;
                }
            }
            1 | 2 | 3 | 4 | 5 | 9 | 12 | 19 | 20 => {
                let Some(key) = StreamPositionKey::decode(&key) else {
                    unknown += 1;
                    return;
                };
                let sample = streams
                    .entry((key.registration_id, key.session_id, key.stream_id))
                    .or_default();
                sample.set(counter.type_id, value);
            }
            201 | 203 | 239 => {
                if let Ok(bytes) = <[u8; 4]>::try_from(&key[..4]) {
                    let cluster = clusters.entry(i32::from_le_bytes(bytes)).or_default();
                    cluster.set(counter.type_id, value);
                } else {
                    unknown += 1;
                }
            }
            100 => {
                if let Some(identity) = parse_archive_key(&key) {
                    recordings.insert(identity.recording_id, value);
                } else {
                    unknown += 1;
                }
            }
            _ => unknown += 1,
        }
    });

    for (id, value) in systems {
        let Some(name) = system_name(id) else {
            continue;
        };
        families.sample(
            name.0,
            name.1,
            name.2,
            format!("{}{{node_id=\"{node}\"{}}} {value}", name.0, name.3),
        );
    }

    for ((_, session_id, stream_id), sample) in streams {
        let labels =
            format!("node_id=\"{node}\",stream_id=\"{stream_id}\",session_id=\"{session_id}\"");
        if let Some(position) = sample.pub_pos.filter(|value| *value >= 0) {
            families.sample(
                "deepmsg_publication_position",
                "The publication's position",
                "gauge",
                format!("deepmsg_publication_position{{{labels}}} {position}"),
            );
        }
        if let Some(position) = sample.sub_pos.filter(|value| *value >= 0) {
            families.sample(
                "deepmsg_subscription_position",
                "The subscription's position",
                "gauge",
                format!("deepmsg_subscription_position{{{labels}}} {position}"),
            );
        }
        if let Some(limit) = sample.snd_lmt.filter(|value| *value >= 0) {
            families.sample(
                "deepmsg_sender_limit",
                "The sender's flow-control limit",
                "gauge",
                format!("deepmsg_sender_limit{{{labels}}} {limit}"),
            );
        }
        if let Some(backlog) = sample.backlog() {
            families.sample(
                "deepmsg_publication_backlog",
                "The publication's limit minus its position",
                "gauge",
                format!("deepmsg_publication_backlog{{{labels}}} {backlog}"),
            );
        }
        if let Some(window) = sample.receiver_window() {
            families.sample(
                "deepmsg_receiver_window",
                "The receiver's high-water mark minus its position",
                "gauge",
                format!("deepmsg_receiver_window{{{labels}}} {window}"),
            );
        }
        if let Some(naks) = sample.naks_received {
            families.sample(
                "deepmsg_nak_total",
                "NAKs observed on a stream",
                "counter",
                format!("deepmsg_nak_total{{{labels},direction=\"received\"}} {naks}"),
            );
        }
        if let Some(naks) = sample.naks_sent {
            families.sample(
                "deepmsg_nak_total",
                "NAKs observed on a stream",
                "counter",
                format!("deepmsg_nak_total{{{labels},direction=\"sent\"}} {naks}"),
            );
        }
    }

    for (cluster_id, sample) in clusters {
        let labels = format!("node_id=\"{node}\",cluster_id=\"{cluster_id}\"");
        if let Some(role) = sample.role {
            families.sample(
                "deepmsg_cluster_is_leader",
                "1 when the node's role is leader (counter 201)",
                "gauge",
                format!(
                    "deepmsg_cluster_is_leader{{{labels}}} {}",
                    u8::from(role == 2)
                ),
            );
        }
        if let Some(term) = sample.term {
            families.sample(
                "deepmsg_cluster_term",
                "The leadership term id (counter 239)",
                "gauge",
                format!("deepmsg_cluster_term{{{labels}}} {term}"),
            );
        }
        if let Some(commit) = sample.commit {
            families.sample(
                "deepmsg_cluster_commit_position",
                "The commit position (counter 203)",
                "gauge",
                format!("deepmsg_cluster_commit_position{{{labels}}} {commit}"),
            );
        }
    }

    for (recording_id, position) in recordings {
        families.sample(
            "deepmsg_archive_recording_position",
            "The recording's position (counter 100)",
            "gauge",
            format!(
                "deepmsg_archive_recording_position{{node_id=\"{node}\",recording_id=\"{recording_id}\"}} {position}"
            ),
        );
    }

    families.sample(
        "deepmsg_unknown_counters",
        "Allocated counters this exporter does not name",
        "gauge",
        format!("deepmsg_unknown_counters{{node_id=\"{node}\"}} {unknown}"),
    );

    // The per-stream loss the driver reports is in its own file, not in a
    // counter: the system counter is a global count of gap fills.
    if let Ok(report) = LossReportFile::open_readonly(dir) {
        for entry in report.entries() {
            families.sample(
                "deepmsg_loss_total",
                "Bytes lost on a stream",
                "counter",
                format!(
                    "deepmsg_loss_total{{node_id=\"{node}\",stream_id=\"{}\",session_id=\"{}\"}} {}",
                    entry.stream_id, entry.session_id, entry.total_bytes_lost
                ),
            );
        }
    }

    families.render()
}

/// The metric a system counter id maps to: `(name, help, type, extra labels)`.
fn system_name(id: i32) -> Option<(&'static str, &'static str, &'static str, &'static str)> {
    match id {
        system::NAK_MESSAGES_SENT => Some((
            "deepmsg_nak_total",
            "NAKs observed on a stream",
            "counter",
            ",direction=\"sent\"",
        )),
        system::NAK_MESSAGES_RECEIVED => Some((
            "deepmsg_nak_total",
            "NAKs observed on a stream",
            "counter",
            ",direction=\"received\"",
        )),
        system::LOSS_GAP_FILLS => Some((
            "deepmsg_loss_total",
            "Loss gap fills the driver performed",
            "counter",
            "",
        )),
        system::ERRORS => Some((
            "deepmsg_error_total",
            "Errors the driver recorded",
            "counter",
            ",component=\"driver\"",
        )),
        _ => None,
    }
}

#[derive(Default)]
struct StreamSamples {
    pub_pos: Option<i64>,
    pub_lmt: Option<i64>,
    snd_pos: Option<i64>,
    snd_lmt: Option<i64>,
    sub_pos: Option<i64>,
    rcv_hwm: Option<i64>,
    rcv_pos: Option<i64>,
    naks_received: Option<i64>,
    naks_sent: Option<i64>,
}

impl StreamSamples {
    fn set(&mut self, type_id: i32, value: i64) {
        match type_id {
            12 => self.pub_pos = Some(value),
            1 => self.pub_lmt = Some(value),
            2 => self.snd_pos = Some(value),
            9 => self.snd_lmt = Some(value),
            4 => self.sub_pos = Some(value),
            3 => self.rcv_hwm = Some(value),
            5 => self.rcv_pos = Some(value),
            19 => self.naks_received = Some(value),
            20 => self.naks_sent = Some(value),
            _ => {}
        }
    }

    fn backlog(&self) -> Option<i64> {
        match (self.pub_pos, self.pub_lmt) {
            (Some(position), Some(limit)) if position >= 0 && limit >= position => {
                Some(limit - position)
            }
            _ => match (self.snd_pos, self.snd_lmt) {
                (Some(position), Some(limit)) if position >= 0 && limit >= position => {
                    Some(limit - position)
                }
                _ => None,
            },
        }
    }

    fn receiver_window(&self) -> Option<i64> {
        match (self.rcv_hwm, self.rcv_pos) {
            (Some(hwm), Some(position)) if hwm >= 0 && position >= 0 && hwm >= position => {
                Some(hwm - position)
            }
            _ => None,
        }
    }
}

#[derive(Default)]
struct ClusterSamples {
    role: Option<i64>,
    commit: Option<i64>,
    term: Option<i64>,
}

impl ClusterSamples {
    fn set(&mut self, type_id: i32, value: i64) {
        match type_id {
            201 => self.role = Some(value),
            203 => self.commit = Some(value),
            239 => self.term = Some(value),
            _ => {}
        }
    }
}

#[derive(Default)]
struct Families {
    map: BTreeMap<&'static str, (&'static str, &'static str, Vec<String>)>,
}

impl Families {
    fn sample(&mut self, name: &'static str, help: &'static str, kind: &'static str, line: String) {
        self.map
            .entry(name)
            .or_insert((help, kind, Vec::new()))
            .2
            .push(line);
    }

    fn render(mut self) -> String {
        let mut out = String::new();
        for (name, (help, kind, mut samples)) in std::mem::take(&mut self.map) {
            samples.sort();
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            for sample in samples {
                let _ = writeln!(out, "{sample}");
            }
        }
        out
    }
}

fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::{Options, render, render_at};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;

    use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
    use deepmsg_cnc::loss_report::{LossReportEntry, LossReportFile};
    use deepmsg_cnc::position_key::StreamPositionKey;
    use deepmsg_cnc::{CncFile, CncIdentity, CncLayout, CounterManager};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU32, Ordering};
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "deepmsg-counters-exporter-{}-{suffix}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp directory");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const NOW_MS: i64 = 1_700_000_000_000;

    fn synthetic(dir: &std::path::Path) {
        let layout = CncLayout {
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN.max(64 * 1024),
            ..CncLayout::default()
        };
        let mut cnc = CncFile::create(
            dir,
            &layout,
            &CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: NOW_MS,
                pid: 4242,
            },
        )
        .expect("create the CnC file");

        {
            let regions = cnc.counter_regions().expect("counter regions");
            let mut manager =
                CounterManager::new(layout.counters_values_length, 1000).expect("manager");

            let mut add = |type_id: i32, key: &[u8], value: i64| {
                let id = manager
                    .allocate(&regions, type_id, key, b"label", NOW_MS)
                    .expect("allocate");
                manager.set_value(&regions, id, value).expect("set value");
            };

            // System counters: type 0, key is the id.
            add(0, &5_i32.to_le_bytes(), 11);
            add(0, &6_i32.to_le_bytes(), 12);
            add(0, &23_i32.to_le_bytes(), 13);
            add(0, &15_i32.to_le_bytes(), 14);

            let key = StreamPositionKey {
                registration_id: 42,
                session_id: 7,
                stream_id: 1002,
                channel: b"aeron:ipc",
            }
            .encode();
            add(12, &key, 1000);
            add(1, &key, 2000);
            add(4, &key, 3000);
            add(3, &key, 5000);
            add(5, &key, 4500);
            add(19, &key, 3);
            add(20, &key, 4);

            let cluster = 7_i32.to_le_bytes();
            add(201, &cluster, 2);
            add(239, &cluster, 5);
            add(203, &cluster, 900);

            let recording = deepmsg_archive::server::recording_pos::key(9, 7, "src", -1);
            add(100, &recording, 1234);

            add(999, &[], 1);
        }

        cnc.write_consumer_heartbeat(NOW_MS - 1500)
            .expect("heartbeat");
        cnc.publish().expect("publish");

        let file = LossReportFile::create(dir, 4096).expect("loss report");
        let buffer = file.writable().expect("writable");
        LossReportEntry {
            observation_count: 2,
            total_bytes_lost: 77,
            first_observation_timestamp: NOW_MS,
            last_observation_timestamp: NOW_MS,
            session_id: 7,
            stream_id: 1002,
            channel: b"aeron:ipc".to_vec(),
            source: b"src".to_vec(),
        }
        .encode(&buffer, 0)
        .expect("encode the loss report");
    }

    #[test]
    fn a_synthetic_cnc_renders_the_first_batch() {
        let temp = TempDir::new();
        synthetic(temp.path());
        let text = render_at(temp.path(), "node-1", NOW_MS);

        for expected in [
            "deepmsg_driver_up{node_id=\"node-1\"} 1",
            "deepmsg_driver_heartbeat_age_seconds{node_id=\"node-1\"} 1.500",
            "deepmsg_publication_position{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\"} 1000",
            "deepmsg_subscription_position{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\"} 3000",
            "deepmsg_publication_backlog{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\"} 1000",
            "deepmsg_receiver_window{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\"} 500",
            "deepmsg_nak_total{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\",direction=\"received\"} 3",
            "deepmsg_nak_total{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\",direction=\"sent\"} 4",
            "deepmsg_nak_total{node_id=\"node-1\",direction=\"sent\"} 11",
            "deepmsg_nak_total{node_id=\"node-1\",direction=\"received\"} 12",
            "deepmsg_loss_total{node_id=\"node-1\"} 13",
            "deepmsg_loss_total{node_id=\"node-1\",stream_id=\"1002\",session_id=\"7\"} 77",
            "deepmsg_error_total{node_id=\"node-1\",component=\"driver\"} 14",
            "deepmsg_cluster_is_leader{node_id=\"node-1\",cluster_id=\"7\"} 1",
            "deepmsg_cluster_term{node_id=\"node-1\",cluster_id=\"7\"} 5",
            "deepmsg_cluster_commit_position{node_id=\"node-1\",cluster_id=\"7\"} 900",
            "deepmsg_archive_recording_position{node_id=\"node-1\",recording_id=\"9\"} 1234",
            "deepmsg_unknown_counters{node_id=\"node-1\"} 1",
        ] {
            assert!(text.contains(expected), "missing {expected}\n{text}");
        }
    }

    #[test]
    fn no_cnc_is_up_zero() {
        let temp = TempDir::new();
        let text = render(temp.path(), "node-1");
        assert!(text.contains("deepmsg_driver_up{node_id=\"node-1\"} 0"));
        assert!(!text.contains("deepmsg_publication_position"));
    }

    /// A CnC with one system counter (the errors id), for the re-read test.
    fn a_cnc_with_errors(dir: &std::path::Path, errors: i64) {
        let layout = CncLayout {
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN.max(64 * 1024),
            ..CncLayout::default()
        };
        let mut cnc = CncFile::create(
            dir,
            &layout,
            &CncIdentity {
                liveness_timeout_ns: 10_000_000_000,
                start_timestamp_ms: NOW_MS,
                pid: 99,
            },
        )
        .expect("create the CnC file");

        {
            let regions = cnc.counter_regions().expect("counter regions");
            let mut manager =
                CounterManager::new(layout.counters_values_length, 1000).expect("manager");
            let id = manager
                .allocate(&regions, 0, &15_i32.to_le_bytes(), b"Errors", NOW_MS)
                .expect("allocate");
            manager.set_value(&regions, id, errors).expect("set value");
        }

        cnc.write_consumer_heartbeat(NOW_MS).expect("heartbeat");
        cnc.publish().expect("publish");
    }

    #[test]
    fn a_new_cnc_is_read_without_restarting_the_exporter() {
        let temp = TempDir::new();
        a_cnc_with_errors(temp.path(), 1);
        assert!(
            render_at(temp.path(), "node-1", NOW_MS)
                .contains("deepmsg_error_total{node_id=\"node-1\",component=\"driver\"} 1")
        );

        // Same path, new file: the next scrape is the new driver.
        std::fs::remove_dir_all(temp.path()).expect("remove the old directory");
        std::fs::create_dir_all(temp.path()).expect("the new directory");
        a_cnc_with_errors(temp.path(), 99);

        assert!(
            render_at(temp.path(), "node-1", NOW_MS)
                .contains("deepmsg_error_total{node_id=\"node-1\",component=\"driver\"} 99")
        );
    }

    #[test]
    fn label_values_are_escaped() {
        assert_eq!("a\\\"b\\\\c\\nd", super::escape_label("a\"b\\c\nd"));
    }

    #[test]
    fn options_read_arguments_and_environment() {
        // The environment is not touched here: `from_args` takes its defaults
        // from it, and a test that rewrote the process environment would race
        // every other test in this binary.
        let options = Options::from_args(
            [
                "--dir".to_owned(),
                "/tmp/aeron".to_owned(),
                "--listen".to_owned(),
                "127.0.0.1:0".to_owned(),
                "--node-id".to_owned(),
                "node-9".to_owned(),
            ]
            .into_iter(),
        )
        .expect("valid options");
        assert_eq!(PathBuf::from("/tmp/aeron"), options.dir);
        assert_eq!("node-9", options.node_id);
    }

    #[test]
    fn the_endpoint_answers_metrics_and_404() {
        let temp = TempDir::new();
        let options = Options {
            dir: temp.path().to_path_buf(),
            listen: "127.0.0.1:0".parse().expect("an address"),
            node_id: "node-http".to_owned(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("an address");

        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept");
                super::handle(&mut stream, &options).expect("handle");
            }
        });

        for (request, expected) in [
            ("GET /metrics HTTP/1.1\r\n\r\n", "200 OK"),
            ("GET /nope HTTP/1.1\r\n\r\n", "404 Not Found"),
        ] {
            let mut stream = TcpStream::connect(address).expect("connect");
            stream.write_all(request.as_bytes()).expect("write");
            let mut response = String::new();
            stream.read_to_string(&mut response).expect("read");
            assert!(response.contains(expected), "{response}");
        }

        server.join().expect("server thread");
    }
}
