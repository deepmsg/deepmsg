//! Channel URI handling (`aeron:udp?...`, `aeron:ipc`, `aeron-spy:`).
//!
//! Reference behaviour that must carry over (M06):
//!
//! - the client core never parses URIs: parsing is a driver concern; a
//!   client-side string layer (in the spirit of the C++
//!   `ChannelUri`/`ChannelUriStringBuilder`) is provided as a convenience
//!   for higher-level clients such as archive,
//! - only the recognised keys are interpreted; unknown keys are preserved
//!   verbatim so the canonical form round-trips,
//! - the canonical channel identity uses the raw parameter text.
//!
//! Reference: `aeron-client/src/main/c/uri/aeron_uri.c`,
//! `aeron-driver/src/main/c/media/aeron_udp_channel.c`.
//!
//! # What is here, and what is deliberately not
//!
//! [`ChannelUri`] reads a URI into its media and its parameters; it **does not
//! validate**, and it is not the driver's parser. The driver parses every URI
//! itself because it is the one that has to act on the keys
//! (`deepmsg-driver`'s `channel_uri`), and a second validating parser here
//! would be a second answer to "is this channel legal". What this reads is
//! what a *client* needs: the media, and the raw text of a named parameter.
//!
//! [`ChannelUriStringBuilder`] is the write half — the reference's
//! `ChannelUriStringBuilder`, field for field and in its parameter order
//! (`ChannelUriStringBuilder.java:45-86`, `:2451-2512`). It is here because the
//! archive derives the channel it answers a client on by rebuilding the
//! client's own URI (`ArchiveConductor.java:458-481`), and that means writing
//! one.
//!
//! # One thing the reference's builder does and this does not
//!
//! `ChannelUriStringBuilder` **validates its arguments** and throws:
//! `build` refuses a URI with no media, or a `udp` one with neither `endpoint`
//! nor `control` (`:216-242`); `mtu` must be 32-65504 and frame-aligned
//! (`:610-615`), `ttl` 0-255 (`:550`), a term offset inside its term (`:838-843`).
//! This writes what it is given.
//!
//! The checks are not here because every value this build sets comes from a URI
//! the driver will parse and refuse anyway, and the refusal that means
//! something happens where the publication is made. What the reference's
//! throwing buys is a Java caller finding out at the call; a Rust caller that
//! cannot supply an invalid media — because it read one out of a URI that had
//! to have one — does not need telling. The verified part is the other half:
//! for every input both accept, the two produce **the same string**, byte for
//! byte, parameter order included.
//!
//! Sizes and durations are written the way agrona writes them
//! (`SystemUtil.formatSize` / `formatDuration`), which is a **suffix only when
//! the value divides evenly**: `65536` is `64k`, `1536` is `1536`, `1000ns` is
//! `1us` and `1001ns` stays in nanoseconds. Both halves of that rule are load
//! bearing — a driver parses `1536k` as 1536 bytes times nothing at all, since
//! `aeron_parse_size64` stops after the suffix (`aeron_parse_util.c:60-70`).

use std::fmt;

/// The scheme every channel URI starts with, after an optional prefix
/// (`ChannelUri.AERON_SCHEME`).
pub const SCHEME: &str = "aeron:";

/// The character that introduces a URI's parameters.
pub const PARAM_SEPARATOR: char = '?';

/// What separates one parameter from the next. **Not** `&`.
pub const PARAM_DELIMITER: char = '|';

/// The prefix a session id carries when it is a tag rather than a number
/// (`ChannelUriStringBuilder.TAG_PREFIX`).
pub const TAG_PREFIX: &str = "tag:";

/// Why a URI could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UriError {
    /// Not `aeron:<media>` after any prefix.
    InvalidScheme,
    /// A parameter with no `=`, which has no name and no value.
    MissingValue {
        /// The text that had neither.
        text: String,
    },
}

impl fmt::Display for UriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidScheme => write!(f, "not an aeron: channel URI"),
            Self::MissingValue { text } => write!(f, "parameter `{text}` has no value"),
        }
    }
}

impl std::error::Error for UriError {}

/// A channel URI read as text: its prefix, its media, and its parameters.
///
/// The parameters keep the order and the raw spelling they arrived in, because
/// a channel's identity is its text — the driver compares these strings, and a
/// reader that normalised them would make two different channels look alike.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelUri {
    prefix: Option<String>,
    media: String,
    params: Vec<(String, String)>,
}

impl ChannelUri {
    /// Read a URI. `aeron:udp?endpoint=localhost:40123`, `aeron:ipc`, and
    /// `aeron-spy:aeron:udp?...` are all well formed.
    ///
    /// # Errors
    ///
    /// [`UriError`] if there is no `aeron:` scheme, or a parameter has no `=`.
    pub fn parse(uri: &str) -> Result<Self, UriError> {
        let (prefix, rest) = match uri.find(SCHEME) {
            Some(0) => (None, uri),
            Some(at) => (Some(uri[..at - 1].to_owned()), &uri[at..]),
            None => return Err(UriError::InvalidScheme),
        };

        let rest = rest.strip_prefix(SCHEME).ok_or(UriError::InvalidScheme)?;

        let (media, params) = match rest.split_once(PARAM_SEPARATOR) {
            Some((media, params)) => (media, Some(params)),
            None => (rest, None),
        };

        // The media is what precedes the first `?`, and nothing else in the URI
        // may be empty: `aeron:` names no transport.
        if media.is_empty() {
            return Err(UriError::InvalidScheme);
        }

        let mut parsed = Vec::new();
        for param in params
            .into_iter()
            .flat_map(|text| text.split(PARAM_DELIMITER))
        {
            if param.is_empty() {
                continue;
            }

            let (key, value) = param
                .split_once('=')
                .ok_or_else(|| UriError::MissingValue {
                    text: param.to_owned(),
                })?;

            parsed.push((key.to_owned(), value.to_owned()));
        }

        Ok(Self {
            prefix,
            media: media.to_owned(),
            params: parsed,
        })
    }

    /// The part before `aeron:`, which only `aeron-spy` uses.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// `udp` or `ipc`.
    pub fn media(&self) -> &str {
        &self.media
    }

    /// A parameter's raw value, or `None` if the URI does not carry it.
    ///
    /// The **last** one wins, as it does in the reference: a URI is scanned
    /// into a map, so a parameter written twice is the later value.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.params
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    /// Every parameter, in the order it was written.
    pub fn params(&self) -> &[(String, String)] {
        &self.params
    }
}

/// The reference's `ChannelUriStringBuilder`: a channel URI written out rather
/// than read in.
///
/// Every setter is one field of the same name in
/// `ChannelUriStringBuilder.java:45-86`, and [`ChannelUriStringBuilder::build`]
/// writes them in the order `:2451-2512` writes them — which is the whole of
/// what makes two builders produce the same string.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChannelUriStringBuilder {
    prefix: Option<String>,
    media: Option<String>,
    tags: Option<String>,
    endpoint: Option<String>,
    network_interface: Option<String>,
    control_endpoint: Option<String>,
    control_mode: Option<String>,
    mtu: Option<i32>,
    term_length: Option<i32>,
    initial_term_id: Option<i32>,
    term_id: Option<i32>,
    term_offset: Option<i32>,
    session_id: Option<i64>,
    is_session_id_tagged: bool,
    ttl: Option<i32>,
    reliable: Option<bool>,
    linger_ns: Option<i64>,
    alias: Option<String>,
    congestion_control: Option<String>,
    flow_control: Option<String>,
    group_tag: Option<i64>,
    sparse: Option<bool>,
    eos: Option<bool>,
    tether: Option<bool>,
    group: Option<bool>,
    rejoin: Option<bool>,
    spies_simulate_connection: Option<bool>,
    socket_sndbuf_length: Option<i32>,
    socket_rcvbuf_length: Option<i32>,
    receiver_window_length: Option<i32>,
    media_receive_timestamp_offset: Option<String>,
    channel_receive_timestamp_offset: Option<String>,
    channel_send_timestamp_offset: Option<String>,
    response_endpoint: Option<String>,
    response_correlation_id: Option<String>,
    nak_delay_ns: Option<i64>,
    untethered_window_limit_timeout_ns: Option<i64>,
    untethered_linger_timeout_ns: Option<i64>,
    untethered_resting_timeout_ns: Option<i64>,
    max_resend: Option<i32>,
    stream_id: Option<i32>,
    publication_window_length: Option<i32>,
}

macro_rules! textual_setter {
    ($name:ident, $field:ident, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(&mut self, value: impl Into<String>) -> &mut Self {
            self.$field = Some(value.into());
            self
        }
    };
}

macro_rules! counted_setter {
    ($name:ident, $field:ident, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(&mut self, value: i32) -> &mut Self {
            self.$field = Some(value);
            self
        }
    };
}

macro_rules! flag_setter {
    ($name:ident, $field:ident, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(&mut self, value: bool) -> &mut Self {
            self.$field = Some(value);
            self
        }
    };
}

macro_rules! duration_setter {
    ($name:ident, $field:ident, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(&mut self, value_ns: i64) -> &mut Self {
            self.$field = Some(value_ns);
            self
        }
    };
}

impl ChannelUriStringBuilder {
    /// The `aeron-spy` (or other) prefix, written before `aeron:`.
    ///
    /// It is not a parameter: it goes in front of the scheme and carries its
    /// own colon (`ChannelUriStringBuilder.java:2455-2458`).
    pub fn prefix(&mut self, value: impl Into<String>) -> &mut Self {
        self.prefix = Some(value.into());
        self
    }

    /// `udp` or `ipc`.
    pub fn media(&mut self, value: impl Into<String>) -> &mut Self {
        self.media = Some(value.into());
        self
    }

    /// A session id, as a number.
    pub fn session_id(&mut self, value: i64) -> &mut Self {
        self.session_id = Some(value);
        self.is_session_id_tagged = false;
        self
    }

    textual_setter!(tags, tags, "`tags`.");
    textual_setter!(endpoint, endpoint, "`endpoint`.");
    textual_setter!(network_interface, network_interface, "`interface`.");
    textual_setter!(control_endpoint, control_endpoint, "`control`.");
    textual_setter!(control_mode, control_mode, "`control-mode`.");
    counted_setter!(mtu, mtu, "`mtu`, written as a size.");
    counted_setter!(
        term_length,
        term_length,
        "`term-length`, written as a size."
    );
    counted_setter!(initial_term_id, initial_term_id, "`init-term-id`.");
    counted_setter!(term_id, term_id, "`term-id`.");
    counted_setter!(term_offset, term_offset, "`term-offset`.");
    counted_setter!(ttl, ttl, "`ttl`.");
    flag_setter!(reliable, reliable, "`reliable`.");
    duration_setter!(linger_ns, linger_ns, "`linger`, written as a duration.");
    textual_setter!(alias, alias, "`alias`.");
    textual_setter!(congestion_control, congestion_control, "`cc`.");
    textual_setter!(flow_control, flow_control, "`fc`.");
    flag_setter!(sparse, sparse, "`sparse`.");
    flag_setter!(eos, eos, "`eos`.");
    flag_setter!(tether, tether, "`tether`.");
    flag_setter!(group, group, "`group`.");
    flag_setter!(rejoin, rejoin, "`rejoin`.");
    flag_setter!(
        spies_simulate_connection,
        spies_simulate_connection,
        "`ssc`."
    );
    counted_setter!(
        socket_sndbuf_length,
        socket_sndbuf_length,
        "`so-sndbuf`, written as a size."
    );
    counted_setter!(
        socket_rcvbuf_length,
        socket_rcvbuf_length,
        "`so-rcvbuf`, written as a size."
    );
    counted_setter!(
        receiver_window_length,
        receiver_window_length,
        "`rcv-wnd`, written as a size."
    );
    textual_setter!(
        media_receive_timestamp_offset,
        media_receive_timestamp_offset,
        "`media-rcv-ts-offset`."
    );
    textual_setter!(
        channel_receive_timestamp_offset,
        channel_receive_timestamp_offset,
        "`channel-rcv-ts-offset`."
    );
    textual_setter!(
        channel_send_timestamp_offset,
        channel_send_timestamp_offset,
        "`channel-snd-ts-offset`."
    );
    textual_setter!(response_endpoint, response_endpoint, "`response-endpoint`.");
    textual_setter!(
        response_correlation_id,
        response_correlation_id,
        "`response-correlation-id`."
    );
    duration_setter!(
        nak_delay_ns,
        nak_delay_ns,
        "`nak-delay`, written as a duration."
    );
    duration_setter!(
        untethered_window_limit_timeout_ns,
        untethered_window_limit_timeout_ns,
        "`untethered-window-limit-timeout`, written as a duration."
    );
    duration_setter!(
        untethered_linger_timeout_ns,
        untethered_linger_timeout_ns,
        "`untethered-linger-timeout`, written as a duration."
    );
    duration_setter!(
        untethered_resting_timeout_ns,
        untethered_resting_timeout_ns,
        "`untethered-resting-timeout`, written as a duration."
    );
    counted_setter!(max_resend, max_resend, "`max-resend`.");
    counted_setter!(stream_id, stream_id, "`stream-id`.");
    counted_setter!(
        publication_window_length,
        publication_window_length,
        "`pub-wnd`, written as a size."
    );

    /// The `gtag` some flow-control strategies take.
    ///
    /// Separate from the counted setters because the reference's field is a
    /// `Long` where the others are `Integer`, and a tagged group is a number
    /// this build should not be inventing a width for.
    pub fn group_tag(&mut self, value: i64) -> &mut Self {
        self.group_tag = Some(value);
        self
    }

    /// The URI, in the reference's parameter order (`:2451-2512`).
    ///
    /// The trailing separator is dropped, which is why a URI with no
    /// parameters is `aeron:ipc` and not `aeron:ipc?` (`:2507-2511`).
    pub fn build(&self) -> String {
        let mut out = String::with_capacity(128);

        if let Some(prefix) = &self.prefix {
            out.push_str(prefix);
            out.push(':');
        }

        out.push_str(SCHEME);
        if let Some(media) = &self.media {
            out.push_str(media);
        }
        out.push(PARAM_SEPARATOR);

        push_text(&mut out, "tags", self.tags.as_deref());
        push_text(&mut out, "endpoint", self.endpoint.as_deref());
        push_text(&mut out, "interface", self.network_interface.as_deref());
        push_text(&mut out, "control", self.control_endpoint.as_deref());
        push_text(&mut out, "control-mode", self.control_mode.as_deref());
        push_size(&mut out, "mtu", self.mtu);
        push_size(&mut out, "term-length", self.term_length);
        push_int(&mut out, "init-term-id", self.initial_term_id);
        push_int(&mut out, "term-id", self.term_id);
        push_int(&mut out, "term-offset", self.term_offset);

        if let Some(session_id) = self.session_id {
            let value = if self.is_session_id_tagged {
                format!("{TAG_PREFIX}{session_id}")
            } else {
                session_id.to_string()
            };
            push_text(&mut out, "session-id", Some(&value));
        }

        push_int(&mut out, "ttl", self.ttl);
        push_flag(&mut out, "reliable", self.reliable);
        push_duration(&mut out, "linger", self.linger_ns);
        push_text(&mut out, "alias", self.alias.as_deref());
        push_text(&mut out, "cc", self.congestion_control.as_deref());
        push_text(&mut out, "fc", self.flow_control.as_deref());
        push_long(&mut out, "gtag", self.group_tag);
        push_flag(&mut out, "sparse", self.sparse);
        push_flag(&mut out, "eos", self.eos);
        push_flag(&mut out, "tether", self.tether);
        push_flag(&mut out, "group", self.group);
        push_flag(&mut out, "rejoin", self.rejoin);
        push_flag(&mut out, "ssc", self.spies_simulate_connection);
        push_size(&mut out, "so-sndbuf", self.socket_sndbuf_length);
        push_size(&mut out, "so-rcvbuf", self.socket_rcvbuf_length);
        push_size(&mut out, "rcv-wnd", self.receiver_window_length);
        push_text(
            &mut out,
            "media-rcv-ts-offset",
            self.media_receive_timestamp_offset.as_deref(),
        );
        push_text(
            &mut out,
            "channel-rcv-ts-offset",
            self.channel_receive_timestamp_offset.as_deref(),
        );
        push_text(
            &mut out,
            "channel-snd-ts-offset",
            self.channel_send_timestamp_offset.as_deref(),
        );
        push_text(
            &mut out,
            "response-endpoint",
            self.response_endpoint.as_deref(),
        );
        push_text(
            &mut out,
            "response-correlation-id",
            self.response_correlation_id.as_deref(),
        );
        push_duration(&mut out, "nak-delay", self.nak_delay_ns);
        push_duration(
            &mut out,
            "untethered-window-limit-timeout",
            self.untethered_window_limit_timeout_ns,
        );
        push_duration(
            &mut out,
            "untethered-linger-timeout",
            self.untethered_linger_timeout_ns,
        );
        push_duration(
            &mut out,
            "untethered-resting-timeout",
            self.untethered_resting_timeout_ns,
        );
        push_int(&mut out, "max-resend", self.max_resend);
        push_int(&mut out, "stream-id", self.stream_id);
        push_size(&mut out, "pub-wnd", self.publication_window_length);

        // The builder appends a separator after every parameter, so the last
        // one is always dangling. A URI that ends here is one with no
        // parameters at all, and the `?` goes with it.
        if out.ends_with(PARAM_DELIMITER) || out.ends_with(PARAM_SEPARATOR) {
            out.pop();
        }

        out
    }
}

/// `name=value|`, or nothing at all when the field was never set.
fn push_text(out: &mut String, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        out.push_str(name);
        out.push('=');
        out.push_str(value);
        out.push(PARAM_DELIMITER);
    }
}

fn push_int(out: &mut String, name: &str, value: Option<i32>) {
    if let Some(value) = value {
        push_text(out, name, Some(&value.to_string()));
    }
}

fn push_long(out: &mut String, name: &str, value: Option<i64>) {
    if let Some(value) = value {
        push_text(out, name, Some(&value.to_string()));
    }
}

fn push_flag(out: &mut String, name: &str, value: Option<bool>) {
    if let Some(value) = value {
        push_text(out, name, Some(if value { "true" } else { "false" }));
    }
}

fn push_size(out: &mut String, name: &str, value: Option<i32>) {
    if let Some(value) = value {
        push_text(out, name, Some(&format_size(value)));
    }
}

fn push_duration(out: &mut String, name: &str, value_ns: Option<i64>) {
    if let Some(value_ns) = value_ns {
        push_text(out, name, Some(&format_duration_ns(value_ns)));
    }
}

/// A byte count in agrona's canonical spelling (`SystemUtil.formatSize`).
///
/// The suffix appears only when the value divides into that unit exactly, and
/// the largest such unit wins: `65536` is `64k`, `1048576` is `1m`, and `1536`
/// is neither `1.5k` nor `1536k` — it is `1536`. A value below the smallest
/// unit keeps its digits, which is why zero writes as `0` and not `0k`.
pub fn format_size(value: i32) -> String {
    let value = i64::from(value);

    for (unit, suffix) in [(1024 * 1024 * 1024, "g"), (1024 * 1024, "m"), (1024, "k")] {
        if value >= unit && value % unit == 0 {
            return format!("{}{suffix}", value / unit);
        }
    }

    value.to_string()
}

/// A byte count read back, as `SystemUtil.parseSize` and
/// `aeron_parse_size64` (`aeron_parse_util.c:42-105`) read one.
///
/// Digits, then **at most one** `k`/`m`/`g`, and nothing after it is looked
/// at: `1mb` is 1048576 bytes because the `b` is never reached
/// (`aeron_parse_util.c:60-70`). The pair with [`format_size`] round-trips for
/// everything `format_size` writes, which is what the archive's stripped
/// channel builder needs — a client's `so-sndbuf=1m` has to come back as `1m`
/// and not as nothing at all.
///
/// `None` for anything that does not begin with a digit, or that overflows an
/// `i32` — the width the URI's size parameters have.
pub fn parse_size(text: &str) -> Option<i32> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut value: i64 = 0;

    while let Some(digit) = bytes.get(index).filter(|byte| byte.is_ascii_digit()) {
        value = value
            .checked_mul(10)?
            .checked_add(i64::from(digit - b'0'))?;
        index += 1;
    }

    if 0 == index {
        return None;
    }

    let multiplier: i64 = match bytes.get(index) {
        Some(b'k' | b'K') => 1024,
        Some(b'm' | b'M') => 1024 * 1024,
        Some(b'g' | b'G') => 1024 * 1024 * 1024,
        _ => 1,
    };

    i32::try_from(value.checked_mul(multiplier)?).ok()
}

/// A duration in agrona's canonical spelling (`SystemUtil.formatDuration`).
///
/// The same divisibility rule as [`format_size`], over seconds, milliseconds
/// and microseconds: `1000000ns` is `1ms`, `1000ns` is `1us`, and `1001ns`
/// stays in nanoseconds. Nothing coarser than a second is ever written —
/// an hour is `3600s`.
pub fn format_duration_ns(value_ns: i64) -> String {
    for (unit, suffix) in [(1_000_000_000, "s"), (1_000_000, "ms"), (1_000, "us")] {
        if value_ns >= unit && value_ns % unit == 0 {
            return format!("{}{suffix}", value_ns / unit);
        }
    }

    format!("{value_ns}ns")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_udp_uri_reads_its_media_and_its_parameters() {
        let uri =
            ChannelUri::parse("aeron:udp?endpoint=localhost:40123|mtu=1408").expect("readable");

        assert_eq!("udp", uri.media());
        assert_eq!(None, uri.prefix());
        assert_eq!(Some("localhost:40123"), uri.get("endpoint"));
        assert_eq!(Some("1408"), uri.get("mtu"));
        assert_eq!(None, uri.get("term-length"));
    }

    /// `aeron:ipc` ends at the media: there is no `?`, so there is nothing to
    /// split off.
    #[test]
    fn a_uri_with_no_parameters_has_none() {
        let uri = ChannelUri::parse("aeron:ipc").expect("readable");

        assert_eq!("ipc", uri.media());
        assert!(uri.params().is_empty());
    }

    #[test]
    fn a_spy_prefix_is_kept_apart_from_the_scheme() {
        let uri = ChannelUri::parse("aeron-spy:aeron:udp?endpoint=localhost:1").expect("readable");

        assert_eq!(Some("aeron-spy"), uri.prefix());
        assert_eq!("udp", uri.media());
    }

    /// The scanner splits on the **first** `=`, so a value may contain more
    /// (`aeron_uri.c:57-62`) — which is what lets an endpoint be written the
    /// way an operator thinks of it.
    #[test]
    fn a_value_may_contain_an_equals_sign() {
        let uri = ChannelUri::parse("aeron:udp?endpoint=a=b").expect("readable");

        assert_eq!(Some("a=b"), uri.get("endpoint"));
    }

    #[test]
    fn a_parameter_with_no_value_is_refused() {
        assert_eq!(
            Err(UriError::MissingValue {
                text: "nonsense".to_owned()
            }),
            ChannelUri::parse("aeron:udp?nonsense")
        );
    }

    #[test]
    fn something_that_is_not_a_channel_is_refused() {
        assert_eq!(
            Err(UriError::InvalidScheme),
            ChannelUri::parse("udp?endpoint=localhost:1")
        );
    }

    /// The number that matters most: a control channel is an ordinary channel
    /// whose parameters survive a rebuild.
    #[test]
    fn building_writes_the_parameters_in_the_references_order() {
        let mut builder = ChannelUriStringBuilder::default();
        builder
            .media("udp")
            .control_endpoint("localhost:9090")
            .control_mode("response")
            .term_length(65536)
            .mtu(1408)
            .sparse(false)
            .response_correlation_id("42");

        assert_eq!(
            "aeron:udp?control=localhost:9090|control-mode=response|mtu=1408|term-length=64k\
             |sparse=false|response-correlation-id=42",
            builder.build()
        );
    }

    #[test]
    fn a_uri_with_nothing_set_is_just_its_media() {
        let mut builder = ChannelUriStringBuilder::default();
        builder.media("ipc");

        assert_eq!("aeron:ipc", builder.build());
    }

    #[test]
    fn a_prefix_goes_before_the_scheme() {
        let mut builder = ChannelUriStringBuilder::default();
        builder
            .prefix("aeron-spy")
            .media("udp")
            .endpoint("localhost:1");

        assert_eq!("aeron-spy:aeron:udp?endpoint=localhost:1", builder.build());
    }

    /// The rule both halves of which are load bearing: the suffix is written
    /// only when the value divides evenly, and a driver reads a suffixed value
    /// as a *different* number when it does not
    /// (`aeron_parse_util.c:60-70`).
    #[test]
    fn a_size_is_spelled_the_way_agrona_spells_it() {
        assert_eq!("64k", format_size(65536));
        assert_eq!("1m", format_size(1024 * 1024));
        assert_eq!("1g", format_size(1024 * 1024 * 1024));
        assert_eq!("1536", format_size(1536));
        assert_eq!("1023", format_size(1023));
        assert_eq!("0", format_size(0));
        assert_eq!("1", format_size(1));
        assert_eq!("1408", format_size(1408));
    }

    #[test]
    fn a_duration_is_spelled_the_way_agrona_spells_it() {
        assert_eq!("1ms", format_duration_ns(1_000_000));
        assert_eq!("1s", format_duration_ns(1_000_000_000));
        assert_eq!("1us", format_duration_ns(1_000));
        assert_eq!("1001ns", format_duration_ns(1001));
        assert_eq!("999999ns", format_duration_ns(999_999));
        assert_eq!("1500us", format_duration_ns(1_500_000));
        assert_eq!("0ns", format_duration_ns(0));
        assert_eq!("3600s", format_duration_ns(3_600_000_000_000));
    }

    /// A rebuilt URI is a URI: the archive hands the driver this string, and
    /// the driver parses it again.
    #[test]
    fn what_the_builder_writes_reads_back_the_same() {
        let mut builder = ChannelUriStringBuilder::default();
        builder
            .media("udp")
            .endpoint("localhost:8010")
            .control_endpoint("localhost:9090")
            .control_mode("response")
            .term_length(65536)
            .sparse(false)
            .mtu(65536)
            .response_correlation_id("7");

        let built = builder.build();
        let uri = ChannelUri::parse(&built).expect("what the builder wrote is a URI");

        assert_eq!("udp", uri.media());
        assert_eq!(Some("localhost:9090"), uri.get("control"));
        assert_eq!(Some("response"), uri.get("control-mode"));
        assert_eq!(Some("64k"), uri.get("term-length"));
        assert_eq!(Some("false"), uri.get("sparse"));
        assert_eq!(Some("7"), uri.get("response-correlation-id"));
    }
}
