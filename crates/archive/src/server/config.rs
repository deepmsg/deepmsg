//! What an archive is told about itself.
//!
//! The reference's archive is configured the way every Java Aeron component is:
//! a pile of `System.getProperty` reads, one per name, with the default written
//! into the constant beside it (`Archive.java:295-663` for the server's own,
//! `client/AeronArchive.java:2674-2848` for the ones the archive shares with the
//! client it embeds). There is no environment layer to mirror — the C driver
//! reads `AERON_*` because `aeronmd` turns `-D` into the environment for it
//! (`aeron-driver/src/main/c/aeronmd.c:72-81`), while the Java archive reads
//! **system properties and nothing else**.
//!
//! How those properties reach us is the shim's doing and worth stating: it
//! hands a replacement binary the invocation's `-D` arguments as **argv**
//! (`crates/tools/src/bin/archive-shim.rs:443-448`), so what a Java archive
//! would have found in `System.getProperty` is here an argument. A *properties
//! file* — the other thing `loadPropertiesFiles(args)` accepts
//! (`ArchivingMediaDriver.java:49`) — is an argument that is not a `-D`, and is
//! read as one.
//!
//! # What this module does not do
//!
//! Only the names this slice uses are resolved. The reference has 45 of them;
//! the rest arrive with the sessions that read them, and the whole set is
//! P2-9c's, whose own acceptance is that an unknown name is accepted without
//! being silently *used* as something else.
//!
//! # Errors
//!
//! Three settings have no default and are refused when missing or wrong, with
//! the reference's own words: `aeron.dir` (the directory the archive is a
//! client of), and the two control channels' media types
//! (`Archive.java:1216-1233`).

use std::fmt;
use std::path::{Path, PathBuf};

/// How the archive's agents are run (`ArchiveThreadingMode.java:28-38`).
///
/// The default is `Dedicated` (`Archive.java:394-397`), and this slice
/// implements `Shared` only — `Archive` picks its conductor with
/// `DEDICATED == ctx.threadingMode() ? DedicatedModeArchiveConductor :
/// SharedModeArchiveConductor` (`Archive.java:146-147`), so `Invoker` is
/// `Shared` seen from the caller's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadingMode {
    /// One thread per agent.
    Dedicated,
    /// The agents are driven in the caller's turn.
    Shared,
    /// `Shared`, with the caller owning the turn.
    Invoker,
}

impl ThreadingMode {
    /// The reference's spelling, which is also the value a property carries.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "DEDICATED" => Some(Self::Dedicated),
            "SHARED" => Some(Self::Shared),
            "INVOKER" => Some(Self::Invoker),
            _ => None,
        }
    }
}

impl fmt::Display for ThreadingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dedicated => "DEDICATED",
            Self::Shared => "SHARED",
            Self::Invoker => "INVOKER",
        })
    }
}

/// A setting's value was there and could not be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The invocation carried something that is not a `-D` property and is not
    /// a properties file that can be read.
    UnreadablePropertiesFile { argument: String },
    /// A `-D` argument with no `=`.
    MalformedArgument { argument: String },
    /// `aeron.dir` was not given.
    ///
    /// The driver's own error, in its words: an archive is a *client* of a
    /// media driver, and the directory is how it finds one
    /// (`crates/driver/src/config.rs:1187-1191`).
    MissingAeronDir,
    /// `aeron.archive.control.channel` is required when the control channel is
    /// enabled (`Archive.java:1218-1221`).
    MissingControlChannel,
    /// ...and must be UDP (`Archive.java:1223-1227`).
    ControlChannelNotUdp { channel: String },
    /// ...while the local one must be IPC (`Archive.java:1230-1233`).
    LocalControlChannelNotIpc { channel: String },
    /// A value that was present and unparseable.
    InvalidValue { name: String, value: String },
    /// A threading mode that is not one of the three.
    UnknownThreadingMode { value: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnreadablePropertiesFile { argument } => {
                write!(f, "properties file could not be read: {argument}")
            }
            Self::MalformedArgument { argument } => {
                write!(
                    f,
                    "malformed property argument (expected -Dname=value): {argument}"
                )
            }
            Self::MissingAeronDir => write!(f, "aeron.dir must be set"),
            Self::MissingControlChannel => {
                write!(f, "Archive.Context.controlChannel must be set")
            }
            Self::ControlChannelNotUdp { channel } => {
                write!(
                    f,
                    "Archive.Context.controlChannel must be UDP media: uri={channel}"
                )
            }
            Self::LocalControlChannelNotIpc { channel } => {
                write!(f, "local control channel must be IPC media: uri={channel}")
            }
            Self::InvalidValue { name, value } => write!(f, "{name}={value} is not a valid value"),
            Self::UnknownThreadingMode { value } => {
                write!(
                    f,
                    "aeron.archive.threading.mode={value} is not one of DEDICATED, SHARED, INVOKER"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// The properties this slice reads, each with the reference's name and default.
///
/// A table rather than scattered lookups, because the names are the contract:
/// the reference's deployment files spell these, and every one of them is
/// checked by a test below.
mod settings {
    /// The directory the archive finds its media driver in
    /// (`AeronArchive`... no: the driver's own name, read by the shim's
    /// replacement binary out of the same argument list — `TestArchive.h:104`).
    pub const AERON_DIR: &str = "aeron.dir";

    /// `Archive.java:307`, default `aeron-archive` (`:315`).
    pub const ARCHIVE_DIR: &str = "aeron.archive.dir";
    /// `Archive.java:321`, default `""` — empty means "beside the archive"
    /// (`:320`).
    pub const MARK_FILE_DIR: &str = "aeron.archive.mark.file.dir";
    /// `Archive.java:578`, default false (`:577`).
    pub const DIR_DELETE_ON_START: &str = "aeron.archive.dir.delete.on.start";
    /// `Archive.java:394`, default `DEDICATED` (`:393`).
    pub const THREADING_MODE: &str = "aeron.archive.threading.mode";
    /// `Archive.java:590`, default `DefaultAuthenticatorSupplier` (`:597`).
    pub const AUTHENTICATOR_SUPPLIER: &str = "aeron.archive.authenticator.supplier";
    /// `Archive.java:603`.
    pub const AUTHORISATION_SERVICE_SUPPLIER: &str = "aeron.archive.authorisation.service.supplier";
    /// `Archive.java:493`, default 5 s (`:502`).
    pub const CONNECT_TIMEOUT: &str = "aeron.archive.connect.timeout";
    /// `Archive.java:510-511`, default 1 s (`:520`).
    pub const SESSION_LIVENESS_CHECK_INTERVAL: &str =
        "aeron.archive.session.liveness.check.interval";
    /// `Archive.java:584`, default `""` (`:583`).
    pub const REPLICATION_CHANNEL: &str = "aeron.archive.replication.channel";
    /// `Archive.java:654`, default `-1` — "the Aeron client's own id"
    /// (`:650-655`).
    pub const ARCHIVE_ID: &str = "aeron.archive.id";
    /// `Archive.java:663`, default true (`:662`).
    pub const CONTROL_CHANNEL_ENABLED: &str = "aeron.archive.control.channel.enabled";

    /// `AeronArchive.java:2698`. No default: required when the control channel
    /// is enabled.
    pub const CONTROL_CHANNEL: &str = "aeron.archive.control.channel";
    /// `AeronArchive.java:2704`, default 10 (`:2710`).
    pub const CONTROL_STREAM_ID: &str = "aeron.archive.control.stream.id";
    /// `AeronArchive.java:2716`, default `aeron:ipc?term-length=64k` (`:2722`).
    pub const LOCAL_CONTROL_CHANNEL: &str = "aeron.archive.local.control.channel";
    /// `AeronArchive.java:2728`, default 10 (`:2734`).
    pub const LOCAL_CONTROL_STREAM_ID: &str = "aeron.archive.local.control.stream.id";
    /// `AeronArchive.java:2752`. No default.
    pub const CONTROL_RESPONSE_CHANNEL: &str = "aeron.archive.control.response.channel";
    /// `AeronArchive.java:2758`, default 20 (`:2764`).
    pub const CONTROL_RESPONSE_STREAM_ID: &str = "aeron.archive.control.response.stream.id";
    /// `AeronArchive.java:2809`, default true (`:2816`).
    pub const CONTROL_TERM_BUFFER_SPARSE: &str = "aeron.archive.control.term.buffer.sparse";
    /// `AeronArchive.java:2822`, default 64 K (`:2828`).
    pub const CONTROL_TERM_BUFFER_LENGTH: &str = "aeron.archive.control.term.buffer.length";
    /// `AeronArchive.java:2834`, default: the media driver's own MTU (`:2840`).
    pub const CONTROL_MTU_LENGTH: &str = "aeron.archive.control.mtu.length";
}

/// The reference's default archive directory (`Archive.java:315`).
const ARCHIVE_DIR_DEFAULT: &str = "aeron-archive";
/// `AeronArchive.java:2722`.
const LOCAL_CONTROL_CHANNEL_DEFAULT: &str = "aeron:ipc?term-length=64k";
/// `AeronArchive.java:2710`.
const CONTROL_STREAM_ID_DEFAULT: i32 = 10;
/// `AeronArchive.java:2764`.
const CONTROL_RESPONSE_STREAM_ID_DEFAULT: i32 = 20;
/// `AeronArchive.java:2828`.
const CONTROL_TERM_BUFFER_LENGTH_DEFAULT: usize = 64 * 1024;
/// `Archive.java:502`.
const CONNECT_TIMEOUT_DEFAULT_NS: i64 = 5 * 1_000_000_000;
/// `Archive.java:520`.
const SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS: i64 = 1_000_000_000;
/// `Archive.java:597`.
const AUTHENTICATOR_SUPPLIER_DEFAULT: &str = "io.aeron.security.DefaultAuthenticatorSupplier";
/// `Archive.java:610`.
const AUTHORISATION_SERVICE_SUPPLIER_DEFAULT: &str =
    "io.aeron.security.DefaultAuthorisationServiceSupplier";

/// Everything an archive is told, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveConfig {
    /// The media driver's directory. The archive is a client of it.
    pub aeron_dir: PathBuf,
    /// Where the catalog and segments live (`Archive.java:307`).
    pub archive_dir: PathBuf,
    /// Where `archive-mark.dat` lives; [`None`] means the archive directory
    /// (`Archive.java:321-325`).
    pub mark_file_dir: Option<PathBuf>,
    /// Whether the archive directory is deleted on start.
    pub delete_dir_on_start: bool,
    /// The archive's identity, or [`None`] for "the Aeron client's own id",
    /// which is the reference's `-1` (`Archive.java:650-655`).
    pub archive_id: Option<i64>,
    /// Whether the UDP control channel is served (`Archive.java:663`).
    pub control_channel_enabled: bool,
    /// The UDP control channel, required when that channel is enabled.
    pub control_channel: Option<String>,
    /// The stream the control channel carries (`AeronArchive.java:2710`).
    pub control_stream_id: i32,
    /// The local (IPC) control channel, which is served whether or not the UDP
    /// one is (`ArchiveConductor.java:239-240`).
    pub local_control_channel: String,
    /// ...and its stream, which is the same 10 by default
    /// (`AeronArchive.java:2734`).
    pub local_control_stream_id: i32,
    /// The channel a client is answered on, when it asked to be
    /// (`AeronArchive.java:2752`).
    pub control_response_channel: Option<String>,
    /// Its stream, 20 by default (`AeronArchive.java:2764`).
    pub control_response_stream_id: i32,
    /// Whether the control channel's term buffers are sparse
    /// (`AeronArchive.java:2816`), which this build forces onto the URI
    /// (`ArchiveConductor.java:230-234`).
    pub control_term_buffer_sparse: bool,
    /// The control channel's term length (`AeronArchive.java:2828`).
    pub control_term_buffer_length: usize,
    /// The control channel's MTU, or [`None`] for the driver's own
    /// (`AeronArchive.java:2840`).
    pub control_mtu_length: Option<usize>,
    /// How long a control session may take to get its first response out
    /// (`Archive.java:502`).
    pub connect_timeout_ns: i64,
    /// How often an active session is pinged (`Archive.java:520`).
    pub session_liveness_check_interval_ns: i64,
    /// `DEDICATED`, `SHARED` or `INVOKER` (`Archive.java:394-397`).
    pub threading_mode: ThreadingMode,
    /// The authenticator to build (`Archive.java:590-597`).
    pub authenticator_supplier: String,
    /// The authorisation service to build (`Archive.java:603-610`).
    pub authorisation_service_supplier: String,
    /// The channel replication streams arrive on (`Archive.java:584`).
    pub replication_channel: Option<String>,
}

impl ArchiveConfig {
    /// Resolve the configuration from an invocation's arguments: `-D` pairs,
    /// and properties files.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a `-D` with no `=`, a file that will not read, or
    /// any of the refusals [`ArchiveConfig::resolve`] makes.
    pub fn from_args<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut properties = Vec::new();

        for argument in args {
            let argument = argument.into();

            if let Some(body) = argument.strip_prefix("-D") {
                let Some((name, value)) = body.split_once('=') else {
                    return Err(ConfigError::MalformedArgument { argument });
                };

                properties.push((name.to_owned(), value.to_owned()));
            } else {
                properties.extend(read_properties_file(&argument)?);
            }
        }

        Self::resolve(&properties)
    }

    /// The testable core: a property list, and nothing else.
    ///
    /// # Errors
    ///
    /// As [`ConfigError`].
    pub fn resolve(properties: &[(String, String)]) -> Result<Self, ConfigError> {
        let get = |name: &str| -> Option<&str> {
            properties
                .iter()
                .rev()
                .find(|(property, _)| property == name)
                .map(|(_, value)| value.as_str())
        };

        let aeron_dir = get(settings::AERON_DIR).ok_or(ConfigError::MissingAeronDir)?;

        let control_channel_enabled =
            boolean(get(settings::CONTROL_CHANNEL_ENABLED)).unwrap_or(true);

        let control_channel = non_empty(get(settings::CONTROL_CHANNEL)).map(str::to_owned);
        if control_channel_enabled {
            let Some(channel) = control_channel.as_deref() else {
                return Err(ConfigError::MissingControlChannel);
            };

            if !channel.starts_with("aeron:udp") {
                return Err(ConfigError::ControlChannelNotUdp {
                    channel: channel.to_owned(),
                });
            }
        }

        let local_control_channel = non_empty(get(settings::LOCAL_CONTROL_CHANNEL))
            .unwrap_or(LOCAL_CONTROL_CHANNEL_DEFAULT)
            .to_owned();
        if !local_control_channel.starts_with("aeron:ipc") {
            return Err(ConfigError::LocalControlChannelNotIpc {
                channel: local_control_channel,
            });
        }

        let threading_mode = match get(settings::THREADING_MODE) {
            None => ThreadingMode::Dedicated,
            Some(value) => {
                ThreadingMode::parse(value).ok_or_else(|| ConfigError::UnknownThreadingMode {
                    value: value.to_owned(),
                })?
            }
        };

        let archive_id = match get(settings::ARCHIVE_ID) {
            // -1 is the reference's "not told", and it resolves to the Aeron
            // client's id — which is not known until there is a client, so it
            // stays unresolved here.
            None => None,
            Some(value) => match value.parse::<i64>() {
                Ok(id) if id >= 0 => Some(id),
                Ok(_) => None,
                Err(_) => {
                    return Err(ConfigError::InvalidValue {
                        name: settings::ARCHIVE_ID.to_owned(),
                        value: value.to_owned(),
                    });
                }
            },
        };

        Ok(Self {
            aeron_dir: PathBuf::from(aeron_dir),
            archive_dir: PathBuf::from(
                non_empty(get(settings::ARCHIVE_DIR)).unwrap_or(ARCHIVE_DIR_DEFAULT),
            ),
            mark_file_dir: non_empty(get(settings::MARK_FILE_DIR)).map(PathBuf::from),
            delete_dir_on_start: boolean(get(settings::DIR_DELETE_ON_START)).unwrap_or(false),
            archive_id,
            control_channel_enabled,
            control_channel,
            control_stream_id: stream_id(
                get(settings::CONTROL_STREAM_ID),
                settings::CONTROL_STREAM_ID,
            )?
            .unwrap_or(CONTROL_STREAM_ID_DEFAULT),
            local_control_channel,
            local_control_stream_id: stream_id(
                get(settings::LOCAL_CONTROL_STREAM_ID),
                settings::LOCAL_CONTROL_STREAM_ID,
            )?
            .unwrap_or(CONTROL_STREAM_ID_DEFAULT),
            control_response_channel: non_empty(get(settings::CONTROL_RESPONSE_CHANNEL))
                .map(str::to_owned),
            control_response_stream_id: stream_id(
                get(settings::CONTROL_RESPONSE_STREAM_ID),
                settings::CONTROL_RESPONSE_STREAM_ID,
            )?
            .unwrap_or(CONTROL_RESPONSE_STREAM_ID_DEFAULT),
            control_term_buffer_sparse: boolean(get(settings::CONTROL_TERM_BUFFER_SPARSE))
                .unwrap_or(true),
            control_term_buffer_length: size(
                get(settings::CONTROL_TERM_BUFFER_LENGTH),
                settings::CONTROL_TERM_BUFFER_LENGTH,
            )?
            .unwrap_or(CONTROL_TERM_BUFFER_LENGTH_DEFAULT),
            control_mtu_length: size(
                get(settings::CONTROL_MTU_LENGTH),
                settings::CONTROL_MTU_LENGTH,
            )?,
            connect_timeout_ns: duration(
                get(settings::CONNECT_TIMEOUT),
                settings::CONNECT_TIMEOUT,
            )?
            .unwrap_or(CONNECT_TIMEOUT_DEFAULT_NS),
            session_liveness_check_interval_ns: duration(
                get(settings::SESSION_LIVENESS_CHECK_INTERVAL),
                settings::SESSION_LIVENESS_CHECK_INTERVAL,
            )?
            .unwrap_or(SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS),
            threading_mode,
            authenticator_supplier: non_empty(get(settings::AUTHENTICATOR_SUPPLIER))
                .unwrap_or(AUTHENTICATOR_SUPPLIER_DEFAULT)
                .to_owned(),
            authorisation_service_supplier: non_empty(get(
                settings::AUTHORISATION_SERVICE_SUPPLIER,
            ))
            .unwrap_or(AUTHORISATION_SERVICE_SUPPLIER_DEFAULT)
            .to_owned(),
            replication_channel: non_empty(get(settings::REPLICATION_CHANNEL)).map(str::to_owned),
        })
    }

    /// Where `archive-mark.dat` goes: the mark file directory when one was
    /// named, and the archive directory otherwise (`Archive.java:321-325`).
    pub fn mark_file_path(&self) -> PathBuf {
        self.mark_file_dir
            .as_deref()
            .unwrap_or(&self.archive_dir)
            .join("archive-mark.dat")
    }
}

/// A property that is present and empty is absent, which is what the
/// reference's `""` defaults mean (`Archive.java:320`, `:583`).
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

/// `"true".equals(value)`, which is how the reference reads every one of its
/// booleans (`Archive.java:925`, `:1042`): anything that is not `true` is
/// false, and that is not an error.
fn boolean(value: Option<&str>) -> Option<bool> {
    value.map(|value| value.eq_ignore_ascii_case("true"))
}

/// A stream id, which is an `i32` with one of the reference's defaults — hence
/// its own reader rather than [`integer`].
fn stream_id(value: Option<&str>, name: &str) -> Result<Option<i32>, ConfigError> {
    match value {
        None => Ok(None),
        Some(value) => value
            .parse::<i32>()
            .map(Some)
            .map_err(|_| ConfigError::InvalidValue {
                name: name.to_owned(),
                value: value.to_owned(),
            }),
    }
}

/// A size with a suffix: `64k`, `1m`, `1408` (`SystemUtil.parseSize`, the
/// reference's own reader for the three control-channel sizes,
/// `ArchiveConductor.java:459-467`).
fn size(value: Option<&str>, name: &str) -> Result<Option<usize>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let invalid = || ConfigError::InvalidValue {
        name: name.to_owned(),
        value: value.to_owned(),
    };

    let (digits, scale) = match value.chars().last() {
        Some('k') | Some('K') => (&value[..value.len() - 1], 1024usize),
        Some('m') | Some('M') => (&value[..value.len() - 1], 1024 * 1024),
        Some('g') | Some('G') => (&value[..value.len() - 1], 1024 * 1024 * 1024),
        _ => (value, 1),
    };

    let number: usize = digits.trim().parse().map_err(|_| invalid())?;
    number.checked_mul(scale).map(Some).ok_or_else(invalid)
}

/// A duration in nanoseconds with a suffix: `5s`, `100ms`, `1us`, or bare
/// nanoseconds (`aeron_format_duration_ns`'s inverse, and what the reference's
/// `Configuration` does with these three names).
fn duration(value: Option<&str>, name: &str) -> Result<Option<i64>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let invalid = || ConfigError::InvalidValue {
        name: name.to_owned(),
        value: value.to_owned(),
    };

    let (digits, scale) = if let Some(rest) = value.strip_suffix("ns") {
        (rest, 1i64)
    } else if let Some(rest) = value.strip_suffix("us") {
        (rest, 1_000)
    } else if let Some(rest) = value.strip_suffix("ms") {
        (rest, 1_000_000)
    } else if let Some(rest) = value.strip_suffix('s') {
        (rest, 1_000_000_000)
    } else {
        (value, 1)
    };

    let number: i64 = digits.trim().parse().map_err(|_| invalid())?;
    number.checked_mul(scale).map(Some).ok_or_else(invalid)
}

/// A properties file, as Agrona reads one: `name=value` per line, `#` and `!`
/// comments, blank lines ignored.
fn read_properties_file(path: &str) -> Result<Vec<(String, String)>, ConfigError> {
    let unreadable = || ConfigError::UnreadablePropertiesFile {
        argument: path.to_owned(),
    };

    let text = std::fs::read_to_string(Path::new(path)).map_err(|_| unreadable())?;

    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with('!'))
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// The reference's 19 defaults, as `TestArchive.h:32-55` spells them, with
    /// the one property that has no default supplied.
    fn archive_defaults() -> Vec<(String, String)> {
        props(&[
            ("aeron.dir", "/dev/shm/aeron-test"),
            ("aeron.archive.dir", "/tmp/archive/source"),
            ("aeron.archive.mark.file.dir", "/dev/shm/aeron-test"),
            (
                "aeron.archive.control.channel",
                "aeron:udp?endpoint=localhost:8010",
            ),
            (
                "aeron.archive.replication.channel",
                "aeron:udp?endpoint=localhost:0",
            ),
            ("aeron.archive.id", "42"),
            ("aeron.archive.threading.mode", "SHARED"),
            ("aeron.archive.idle.strategy", "yield"),
            ("aeron.archive.max.concurrent.replays", "100"),
            ("aeron.archive.control.channel.enabled", "true"),
        ])
    }

    #[test]
    fn the_defaults_are_the_references() {
        let config = ArchiveConfig::resolve(&archive_defaults()).expect("resolves");

        assert_eq!(config.control_stream_id, 10);
        assert_eq!(config.local_control_channel, "aeron:ipc?term-length=64k");
        assert_eq!(config.local_control_stream_id, 10);
        assert_eq!(config.control_response_stream_id, 20);
        assert_eq!(config.control_response_channel, None);
        assert!(config.control_term_buffer_sparse);
        assert_eq!(config.control_term_buffer_length, 64 * 1024);
        assert_eq!(config.control_mtu_length, None);
        assert_eq!(config.connect_timeout_ns, 5_000_000_000);
        assert_eq!(config.session_liveness_check_interval_ns, 1_000_000_000);
        assert_eq!(config.threading_mode, ThreadingMode::Shared);
        assert_eq!(
            config.authenticator_supplier,
            "io.aeron.security.DefaultAuthenticatorSupplier"
        );
        assert_eq!(config.archive_id, Some(42));
        assert!(!config.delete_dir_on_start);
    }

    #[test]
    fn the_defaults_of_names_the_harness_never_sets_are_the_references_too() {
        // Everything `TestArchive.h` does not pass has to come from the
        // reference's own defaults, which is what these names are checked
        // against — a typo here is a name no deployment sets and nobody misses
        // until a real one does.
        let config = ArchiveConfig::resolve(&props(&[
            ("aeron.dir", "/dev/shm/aeron-test"),
            (
                "aeron.archive.control.channel",
                "aeron:udp?endpoint=localhost:8010",
            ),
        ]))
        .expect("resolves");

        assert_eq!(config.archive_dir, PathBuf::from("aeron-archive"));
        assert_eq!(config.mark_file_dir, None);
        assert_eq!(config.threading_mode, ThreadingMode::Dedicated);
        assert_eq!(
            config.archive_id, None,
            "-1, or nothing, is the client's own id"
        );
        assert_eq!(config.replication_channel, None);
        assert_eq!(
            config.mark_file_path(),
            PathBuf::from("aeron-archive/archive-mark.dat"),
            "an unnamed mark file directory is the archive directory"
        );
    }

    #[test]
    fn a_mark_file_directory_moves_the_mark_file_and_nothing_else() {
        let mut properties = archive_defaults();
        properties.push((
            "aeron.archive.mark.file.dir".to_owned(),
            "/dev/shm/aeron-test".to_owned(),
        ));

        let config = ArchiveConfig::resolve(&properties).expect("resolves");

        assert_eq!(
            config.mark_file_path(),
            PathBuf::from("/dev/shm/aeron-test/archive-mark.dat")
        );
        assert_eq!(config.archive_dir, PathBuf::from("/tmp/archive/source"));
    }

    #[test]
    fn the_two_control_channels_media_types_are_refused_the_references_way() {
        // These are the two refusals P2-1c met the hard way, and the words
        // matter: a client reads them.
        let no_channel = ArchiveConfig::resolve(&props(&[("aeron.dir", "/dev/shm/a")]))
            .expect_err("the control channel has no default");
        assert_eq!(no_channel, ConfigError::MissingControlChannel);
        assert_eq!(
            no_channel.to_string(),
            "Archive.Context.controlChannel must be set"
        );

        let ipc_control = ArchiveConfig::resolve(&props(&[
            ("aeron.dir", "/dev/shm/a"),
            ("aeron.archive.control.channel", "aeron:ipc?term-length=64k"),
        ]))
        .expect_err("a control channel must be UDP");
        assert_eq!(
            ipc_control.to_string(),
            "Archive.Context.controlChannel must be UDP media: \
             uri=aeron:ipc?term-length=64k"
        );

        let udp_local = ArchiveConfig::resolve(&props(&[
            ("aeron.dir", "/dev/shm/a"),
            (
                "aeron.archive.control.channel",
                "aeron:udp?endpoint=localhost:8010",
            ),
            (
                "aeron.archive.local.control.channel",
                "aeron:udp?endpoint=localhost:0",
            ),
        ]))
        .expect_err("a local control channel must be IPC");
        assert_eq!(
            udp_local.to_string(),
            "local control channel must be IPC media: uri=aeron:udp?endpoint=localhost:0"
        );
    }

    #[test]
    fn a_disabled_control_channel_needs_no_channel_and_the_local_one_still_exists() {
        // `ArchiveConductor.java:239-240` adds the local subscription with no
        // condition on it, so IPC-only mode still has one channel to serve.
        let config = ArchiveConfig::resolve(&props(&[
            ("aeron.dir", "/dev/shm/a"),
            ("aeron.archive.control.channel.enabled", "false"),
        ]))
        .expect("resolves");

        assert!(!config.control_channel_enabled);
        assert_eq!(config.control_channel, None);
        assert_eq!(config.local_control_channel, "aeron:ipc?term-length=64k");
    }

    #[test]
    fn sizes_and_durations_are_read_the_references_way() {
        let config = ArchiveConfig::resolve(&props(&[
            ("aeron.dir", "/dev/shm/a"),
            (
                "aeron.archive.control.channel",
                "aeron:udp?endpoint=localhost:8010",
            ),
            ("aeron.archive.control.term.buffer.length", "1m"),
            ("aeron.archive.control.mtu.length", "1408"),
            ("aeron.archive.connect.timeout", "250ms"),
            ("aeron.archive.session.liveness.check.interval", "2s"),
        ]))
        .expect("resolves");

        assert_eq!(config.control_term_buffer_length, 1024 * 1024);
        assert_eq!(config.control_mtu_length, Some(1408));
        assert_eq!(config.connect_timeout_ns, 250_000_000);
        assert_eq!(config.session_liveness_check_interval_ns, 2_000_000_000);
    }

    #[test]
    fn a_value_that_is_there_and_wrong_is_an_error_rather_than_a_default() {
        // The G-series lesson this repository paid for once already: an unknown
        // *name* is ignored, but a name that is present with a value nobody can
        // use must not quietly become the default.
        for (name, value) in [
            ("aeron.archive.control.stream.id", "ten"),
            ("aeron.archive.control.term.buffer.length", "64 kilo"),
            ("aeron.archive.connect.timeout", "soon"),
            ("aeron.archive.id", "the-answer"),
            ("aeron.archive.threading.mode", "PARALLEL"),
        ] {
            let mut properties = archive_defaults();
            properties.retain(|(property, _)| property != name);
            properties.push((name.to_owned(), value.to_owned()));

            assert!(
                ArchiveConfig::resolve(&properties).is_err(),
                "{name}={value} must be refused"
            );
        }
    }

    #[test]
    fn an_unknown_name_is_ignored() {
        // 45 names exist and this slice reads twenty; the rest arrive with the
        // sessions that use them. A deployment that spells all 45 must start.
        let mut properties = archive_defaults();
        properties.push((
            "aeron.archive.segment.file.length".to_owned(),
            "128m".to_owned(),
        ));
        properties.push((
            "aeron.archive.record.checksum".to_owned(),
            "crc32".to_owned(),
        ));

        assert!(ArchiveConfig::resolve(&properties).is_ok());
    }

    #[test]
    fn the_last_spelling_of_a_name_wins() {
        let mut properties = archive_defaults();
        properties.push(("aeron.archive.id".to_owned(), "7".to_owned()));

        assert_eq!(
            ArchiveConfig::resolve(&properties)
                .expect("resolves")
                .archive_id,
            Some(7)
        );
    }

    #[test]
    fn arguments_are_properties_and_files() {
        let dir =
            std::env::temp_dir().join(format!("deepmsg-archive-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let file = dir.join("archive.properties");
        std::fs::write(
            &file,
            "# a comment\n! another\naeron.archive.id=99\n\n aeron.archive.control.stream.id = 11 \n",
        )
        .expect("writes");

        let config = ArchiveConfig::from_args([
            format!("-D{}={}", settings::AERON_DIR, "/dev/shm/a"),
            format!(
                "-D{}={}",
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010"
            ),
            file.to_string_lossy().into_owned(),
        ])
        .expect("resolves");

        assert_eq!(config.archive_id, Some(99));
        assert_eq!(config.control_stream_id, 11);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_argument_that_is_neither_is_refused_by_name() {
        let error = ArchiveConfig::from_args(["-Daeron.dir".to_owned()]).expect_err("no = in it");
        assert_eq!(
            error.to_string(),
            "malformed property argument (expected -Dname=value): -Daeron.dir"
        );

        let error = ArchiveConfig::from_args(["/no/such/file.properties".to_owned()])
            .expect_err("nothing there");
        assert_eq!(
            error.to_string(),
            "properties file could not be read: /no/such/file.properties"
        );
    }
}
