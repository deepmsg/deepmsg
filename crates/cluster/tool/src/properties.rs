//! The four settings the tool reads (`ClusterTool.java:64-118`).
//!
//! They are the tool's own; a cluster component never reads them. The reference
//! takes them as JVM system properties, which is why they are not in the
//! `aeron.cluster.*` name table LC-6 counts (that table is the names a
//! **component** reads). Here they arrive the same way every other binary in
//! this workspace takes a property: `-Dname=value`.

use deepmsg_cluster_common::config::Properties;

/// `AERON_CLUSTER_TOOL_TIMEOUT_PROP_NAME` (`:70`): how long an operation waits,
/// in nanoseconds. Zero — the default — means "do not wait for anything".
pub const TOOL_TIMEOUT_PROP_NAME: &str = "aeron.cluster.tool.timeout";

/// `AERON_CLUSTER_TOOL_DELAY_PROP_NAME` (`:76`): the delay `backup-query`
/// applies, in nanoseconds.
pub const TOOL_DELAY_PROP_NAME: &str = "aeron.cluster.tool.delay";

/// `AERON_CLUSTER_TOOL_REPLAY_CHANNEL_PROP_NAME` (`:82`).
pub const TOOL_REPLAY_CHANNEL_PROP_NAME: &str = "aeron.cluster.tool.replay.channel";
/// `AERON_CLUSTER_TOOL_REPLAY_CHANNEL_DEFAULT` (`:88`).
pub const TOOL_REPLAY_CHANNEL_DEFAULT: &str = "aeron:ipc";

/// `AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_PROP_NAME` (`:100`).
pub const TOOL_REPLAY_STREAM_ID_PROP_NAME: &str = "aeron.cluster.tool.replay.stream.id";
/// `AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_DEFAULT` (`:106`).
pub const TOOL_REPLAY_STREAM_ID_DEFAULT: i32 = 103;

/// The tool's settings, resolved once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolProperties {
    /// `TIMEOUT_MS` (`:117-118`): the nanosecond setting converted to millis.
    pub timeout_ms: i64,
    /// The delay in milliseconds, from the same parser.
    pub delay_ms: i64,
    /// `AERON_CLUSTER_TOOL_REPLAY_CHANNEL` (`:93-94`).
    pub replay_channel: String,
    /// `AERON_CLUSTER_TOOL_REPLAY_STREAM_ID` (`:111-112`).
    pub replay_stream_id: i32,
}

impl Default for ToolProperties {
    fn default() -> Self {
        Self {
            timeout_ms: 0,
            delay_ms: 0,
            replay_channel: TOOL_REPLAY_CHANNEL_DEFAULT.to_owned(),
            replay_stream_id: TOOL_REPLAY_STREAM_ID_DEFAULT,
        }
    }
}

impl ToolProperties {
    /// Read the four settings, or the defaults.
    ///
    /// # Errors
    ///
    /// The message names the property and the value that was refused.
    pub fn from_properties(properties: &Properties) -> Result<Self, String> {
        let timeout_ns = properties
            .duration_ns(TOOL_TIMEOUT_PROP_NAME, 0)
            .map_err(|error| format!("{TOOL_TIMEOUT_PROP_NAME}: {error}"))?;
        let delay_ns = properties
            .duration_ns(TOOL_DELAY_PROP_NAME, 0)
            .map_err(|error| format!("{TOOL_DELAY_PROP_NAME}: {error}"))?;
        let replay_channel = properties
            .get(TOOL_REPLAY_CHANNEL_PROP_NAME)
            .unwrap_or(TOOL_REPLAY_CHANNEL_DEFAULT)
            .to_owned();
        let replay_stream_id = match properties.get(TOOL_REPLAY_STREAM_ID_PROP_NAME) {
            None => TOOL_REPLAY_STREAM_ID_DEFAULT,
            Some(text) => text.parse().map_err(|_| {
                format!("{TOOL_REPLAY_STREAM_ID_PROP_NAME}={text} is not an integer")
            })?,
        };

        Ok(Self {
            // `NANOSECONDS.toMillis` truncates toward zero.
            timeout_ms: timeout_ns / 1_000_000,
            delay_ms: delay_ns / 1_000_000,
            replay_channel,
            replay_stream_id,
        })
    }
}
