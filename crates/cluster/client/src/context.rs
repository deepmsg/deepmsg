//! The cluster client's context: what it connects with, and what it refuses
//! (`AeronCluster.Context`, `AeronCluster.java:1195-2109`).
//!
//! # Seven names, and where they come from
//!
//! The reference reads Java **system properties** — `-D` arguments — through
//! the static `Configuration` accessors, and a `Context` takes its field
//! initial values from them (`AC:1424-1442`). Here the same values come from
//! [`Properties`](deepmsg_cluster_common::config::Properties), which is this
//! build's reader for the same `-D` list, and
//! [`AeronClusterContext::from_properties`] is the constructor that reads them.
//! Nothing here looks at the process's own configuration: a test that sets one
//! context's values cannot disturb another's, which is the same reason the
//! archive client reads a list.
//!
//! # `conclude` refuses what a session cannot be opened without
//!
//! Four refusals, in the reference's order, with its words
//! (`AC:1471-1517`). They are checked once: a second `conclude` is refused too,
//! which is the reference's `ConcurrentConcludeException` said as a value.
//!
//! Two things a Rust caller should know that a Java one need not:
//!
//! * **The context is consumed by a connect.** The reference clones a context
//!   per `AsyncConnect` and keeps the caller's copy — the 8th connect test reads
//!   `context.newLeaderTimeoutNs()` after the connect. A Rust context holds a
//!   `Box<dyn CredentialsSupplier>` and is not `Clone`, so the connect takes
//!   ownership and the values a completed connect settled on are read from the
//!   [`AeronCluster`](crate::cluster::AeronCluster) instead.
//! * **There is no `ownsAeronClient`.** A `Client` is always the caller's here
//!   (the spec's shape), so there is no directory name to connect to and no
//!   client to close.

use core::fmt;

use deepmsg_cluster_common::config::{ConfigError, Properties};
use deepmsg_core::uri::ChannelUri;
use deepmsg_core::version::semantic_version_compose;

use crate::error::ClusterClientError;

/// `aeron.cluster.message.timeout` (`AC:1235`).
pub const MESSAGE_TIMEOUT_PROP_NAME: &str = "aeron.cluster.message.timeout";
/// `MESSAGE_TIMEOUT_DEFAULT_NS` (`AC:1240-1241`): five seconds.
pub const MESSAGE_TIMEOUT_DEFAULT_NS: i64 = 5 * 1_000 * 1_000 * 1_000;

/// `aeron.cluster.ingress.endpoints` (`AC:1253`). Default null.
pub const INGRESS_ENDPOINTS_PROP_NAME: &str = "aeron.cluster.ingress.endpoints";

/// `aeron.cluster.ingress.channel` (`AC:1267`). Default null.
pub const INGRESS_CHANNEL_PROP_NAME: &str = "aeron.cluster.ingress.channel";

/// `aeron.cluster.ingress.stream.id` (`AC:1279`).
pub const INGRESS_STREAM_ID_PROP_NAME: &str = "aeron.cluster.ingress.stream.id";
/// `INGRESS_STREAM_ID_DEFAULT` (`AC:1285`).
pub const INGRESS_STREAM_ID_DEFAULT: i32 = 101;

/// `aeron.cluster.egress.channel` (`AC:1302`). Default null.
pub const EGRESS_CHANNEL_PROP_NAME: &str = "aeron.cluster.egress.channel";

/// `aeron.cluster.egress.stream.id` (`AC:1314`).
pub const EGRESS_STREAM_ID_PROP_NAME: &str = "aeron.cluster.egress.stream.id";
/// `EGRESS_STREAM_ID_DEFAULT` (`AC:1320`).
pub const EGRESS_STREAM_ID_DEFAULT: i32 = 102;

/// `aeron.cluster.client.name` (`AC:1328`). Default the empty string.
pub const CLIENT_NAME_PROP_NAME: &str = "aeron.cluster.client.name";

/// `Aeron.Configuration.MAX_CLIENT_NAME_LENGTH` (`Aeron.java:1098`).
pub const MAX_CLIENT_NAME_LENGTH: usize = 100;

/// The client-to-consensus-module protocol's semantic version (`AC:1209-1229`):
/// 0.3.0.
pub const PROTOCOL_SEMANTIC_VERSION: i32 = semantic_version_compose(0, 3, 0);

/// The parameter a UDP egress channel gains in `conclude` (`AC:1498-1502`,
/// `CommonContext.REJOIN_PARAM_NAME`).
const REJOIN_PARAM_NAME: &str = "rejoin";
/// Where the IPC channel prefix starts (`CommonContext.IPC_CHANNEL`).
const IPC_CHANNEL: &str = "aeron:ipc";

/// What a cluster client sends when the cluster asks it to prove itself.
///
/// The reference's `CredentialsSupplier` is two methods
/// (`aeron-client/src/main/java/io/aeron/security/CredentialsSupplier.java:31`,
/// `:40`). The second is a `Result` here because the reference's suppliers
/// **throw** when they cannot answer — a replication's says so in its own
/// words — and a `Vec` cannot say that.
pub trait CredentialsSupplier {
    /// What to send with the connect request.
    fn encoded(&mut self) -> Vec<u8>;

    /// What to send in answer to `challenge`, or the reason it cannot be
    /// answered.
    ///
    /// # Errors
    ///
    /// The supplier's own words, which the connect carries on as
    /// [`ClusterClientError::Authentication`].
    fn on_challenge(&mut self, challenge: &[u8]) -> Result<Vec<u8>, String>;
}

/// The supplier a context has when nothing was configured: it offers nothing
/// and answers a challenge with nothing (`NullCredentialsSupplier.java:41-53`).
#[derive(Clone, Copy, Debug, Default)]
pub struct NullCredentialsSupplier;

impl CredentialsSupplier for NullCredentialsSupplier {
    fn encoded(&mut self) -> Vec<u8> {
        Vec::new()
    }

    fn on_challenge(&mut self, _challenge: &[u8]) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }
}

/// What a connect starts from.
pub struct AeronClusterContext {
    /// `aeron.cluster.message.timeout`, and the deadline of the whole connect.
    pub message_timeout_ns: i64,
    /// `newLeaderTimeoutNs`; `None` is `Aeron.NULL_VALUE`, which a completed
    /// connect turns into twice the leader's heartbeat timeout (`AC:2574-2578`).
    pub new_leader_timeout_ns: Option<i64>,
    /// `<memberId>=<endpoint>,…`, or `None` for a multicast/unicast channel
    /// that names its endpoint itself.
    pub ingress_endpoints: Option<String>,
    /// `aeron.cluster.ingress.channel`; `conclude` refuses a missing one.
    pub ingress_channel: Option<String>,
    /// `aeron.cluster.ingress.stream.id`.
    pub ingress_stream_id: i32,
    /// `aeron.cluster.egress.channel`; `conclude` refuses a missing one.
    pub egress_channel: Option<String>,
    /// `aeron.cluster.egress.stream.id`.
    pub egress_stream_id: i32,
    /// `aeron.cluster.client.name`.
    pub client_name: String,
    /// Whether ingress is from one thread (the reference's default, `AC:1436`).
    pub is_ingress_exclusive: bool,
    /// The challenge/response supplier.
    pub credentials: Box<dyn CredentialsSupplier>,
    /// `conclude` has run, once.
    is_concluded: bool,
}

impl Default for AeronClusterContext {
    fn default() -> Self {
        Self {
            message_timeout_ns: MESSAGE_TIMEOUT_DEFAULT_NS,
            new_leader_timeout_ns: None,
            ingress_endpoints: None,
            ingress_channel: None,
            ingress_stream_id: INGRESS_STREAM_ID_DEFAULT,
            egress_channel: None,
            egress_stream_id: EGRESS_STREAM_ID_DEFAULT,
            client_name: String::new(),
            is_ingress_exclusive: true,
            credentials: Box::new(NullCredentialsSupplier),
            is_concluded: false,
        }
    }
}

impl fmt::Debug for AeronClusterContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AeronClusterContext")
            .field("message_timeout_ns", &self.message_timeout_ns)
            .field("new_leader_timeout_ns", &self.new_leader_timeout_ns)
            .field("ingress_endpoints", &self.ingress_endpoints)
            .field("ingress_channel", &self.ingress_channel)
            .field("ingress_stream_id", &self.ingress_stream_id)
            .field("egress_channel", &self.egress_channel)
            .field("egress_stream_id", &self.egress_stream_id)
            .field("client_name", &self.client_name)
            .field("is_ingress_exclusive", &self.is_ingress_exclusive)
            .field("is_concluded", &self.is_concluded)
            .finish()
    }
}

impl AeronClusterContext {
    /// An empty context with the reference's defaults and no channels.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the seven names. The stream ids parse the way Java's
    /// `Integer.getInteger` does — a value that will not parse keeps the
    /// default rather than refusing the client.
    ///
    /// # Errors
    ///
    /// [`ClusterClientError::Configuration`] when the message timeout is not a
    /// duration Agrona would accept (that one *is* an error: a deadline that
    /// cannot be read is not a deadline that has a default).
    pub fn from_properties(properties: &Properties) -> Result<Self, ClusterClientError> {
        let message_timeout_ns = properties
            .duration_ns(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS)
            .map_err(|error| ClusterClientError::Configuration(error.to_string()))?;

        let int = |name: &str, default: i32| {
            properties
                .get(name)
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };

        Ok(Self {
            message_timeout_ns,
            ingress_endpoints: properties
                .get(INGRESS_ENDPOINTS_PROP_NAME)
                .map(str::to_owned),
            ingress_channel: properties.get(INGRESS_CHANNEL_PROP_NAME).map(str::to_owned),
            ingress_stream_id: int(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT),
            egress_channel: properties.get(EGRESS_CHANNEL_PROP_NAME).map(str::to_owned),
            egress_stream_id: int(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT),
            client_name: properties
                .get(CLIENT_NAME_PROP_NAME)
                .unwrap_or_default()
                .to_owned(),
            ..Self::default()
        })
    }

    /// `aeron.cluster.message.timeout`.
    #[must_use]
    pub const fn message_timeout_ns(mut self, value: i64) -> Self {
        self.message_timeout_ns = value;
        self
    }

    /// `newLeaderTimeoutNs`; `None` leaves it for a completed connect to
    /// compute (`AC:2574-2578`).
    #[must_use]
    pub const fn new_leader_timeout_ns(mut self, value: Option<i64>) -> Self {
        self.new_leader_timeout_ns = value;
        self
    }

    /// `<memberId>=<endpoint>,…`.
    #[must_use]
    pub fn ingress_endpoints(mut self, value: Option<&str>) -> Self {
        self.ingress_endpoints = value.map(str::to_owned);
        self
    }

    /// `aeron.cluster.ingress.channel`.
    #[must_use]
    pub fn ingress_channel(mut self, value: Option<&str>) -> Self {
        self.ingress_channel = value.map(str::to_owned);
        self
    }

    /// `aeron.cluster.ingress.stream.id`.
    #[must_use]
    pub const fn ingress_stream_id(mut self, value: i32) -> Self {
        self.ingress_stream_id = value;
        self
    }

    /// `aeron.cluster.egress.channel`.
    #[must_use]
    pub fn egress_channel(mut self, value: Option<&str>) -> Self {
        self.egress_channel = value.map(str::to_owned);
        self
    }

    /// `aeron.cluster.egress.stream.id`.
    #[must_use]
    pub const fn egress_stream_id(mut self, value: i32) -> Self {
        self.egress_stream_id = value;
        self
    }

    /// `aeron.cluster.client.name`.
    #[must_use]
    pub fn client_name(mut self, value: &str) -> Self {
        self.client_name = value.to_owned();
        self
    }

    /// Whether ingress is from one thread; the reference's default is `true`.
    #[must_use]
    pub const fn is_ingress_exclusive(mut self, value: bool) -> Self {
        self.is_ingress_exclusive = value;
        self
    }

    /// Replace the credentials supplier.
    #[must_use]
    pub fn credentials(mut self, supplier: Box<dyn CredentialsSupplier>) -> Self {
        self.credentials = supplier;
        self
    }

    /// Whether [`Self::conclude`] has run.
    #[must_use]
    pub const fn is_concluded(&self) -> bool {
        self.is_concluded
    }

    /// The reference's five rules (`AC:1471-1517`), with its words.
    ///
    /// # Errors
    ///
    /// [`ClusterClientError::Configuration`] naming the first rule that fails.
    pub fn conclude(&mut self) -> Result<(), ClusterClientError> {
        if self.is_concluded {
            // The reference's `ConcurrentConcludeException`; a Rust caller has
            // no second thread to race with, so the same rule is a value.
            return Err(ClusterClientError::Configuration(
                "AeronCluster.Context is already concluded".to_owned(),
            ));
        }
        self.is_concluded = true;

        let ingress = self.ingress_channel.as_deref().unwrap_or_default();
        if ingress.is_empty() {
            return Err(ClusterClientError::Configuration(
                "ingressChannel must be specified".to_owned(),
            ));
        }

        if ingress.starts_with(IPC_CHANNEL) && self.ingress_endpoints.is_some() {
            return Err(ClusterClientError::Configuration(
                "AeronCluster.Context ingressEndpoints must be null when using IPC ingress"
                    .to_owned(),
            ));
        }

        let egress = self.egress_channel.as_deref().unwrap_or_default();
        if egress.is_empty() {
            return Err(ClusterClientError::Configuration(
                "egressChannel must be specified".to_owned(),
            ));
        }

        let mut uri = ChannelUri::parse(egress).map_err(|error| {
            ClusterClientError::Configuration(format!("egressChannel is not a channel: {error}"))
        })?;

        // `AC:1498-1502`: a UDP egress channel is told not to rejoin, because
        // the client decides where its answers come from — the new leader's
        // egress publication is created for this session, not joined.
        if "udp" == uri.media() {
            uri.put(REJOIN_PARAM_NAME, "false");
            self.egress_channel = Some(uri.build());
        }

        // Java's `String.length()` counts UTF-16 code units, not bytes.
        let name_length = self.client_name.encode_utf16().count();
        if name_length > MAX_CLIENT_NAME_LENGTH {
            return Err(ClusterClientError::Configuration(format!(
                "AeronCluster.Context.clientName length must be <= {MAX_CLIENT_NAME_LENGTH}"
            )));
        }

        Ok(())
    }
}

/// A `Properties` read that failed, kept for callers that build one themselves.
impl From<ConfigError> for ClusterClientError {
    fn from(error: ConfigError) -> Self {
        Self::Configuration(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The context every accepting test starts from: a channel pair that
    /// concludes.
    fn context() -> AeronClusterContext {
        AeronClusterContext::default()
            .ingress_channel(Some("aeron:udp"))
            .egress_channel(Some("aeron:udp?endpoint=localhost:0"))
    }

    /// `concludeThrowsConfigurationExceptionIfIngressChannelIsNotSet`: null
    /// and empty, which the reference's `NullAndEmptySource` runs as two.
    #[test]
    fn conclude_refuses_a_missing_ingress_channel() {
        for missing in [None, Some("")] {
            let mut context = AeronClusterContext::default()
                .ingress_channel(missing)
                .egress_channel(Some("aeron:udp?endpoint=localhost:0"));

            let error = context.conclude().expect_err("no ingress channel");

            assert_eq!(
                "ERROR - ingressChannel must be specified",
                error.to_string()
            );
        }
    }

    /// `concludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified`.
    #[test]
    fn conclude_refuses_ipc_ingress_with_endpoints() {
        let mut context = AeronClusterContext::default()
            .ingress_channel(Some("aeron:ipc"))
            .ingress_endpoints(Some("0,localhost:1234"))
            .egress_channel(Some("aeron:udp?endpoint=localhost:0"));

        let error = context.conclude().expect_err("ipc with endpoints");

        assert_eq!(
            "ERROR - AeronCluster.Context ingressEndpoints must be null when using IPC ingress",
            error.to_string()
        );
    }

    /// `concludeThrowsConfigurationExceptionIfEgressChannelIsNotSet`.
    #[test]
    fn conclude_refuses_a_missing_egress_channel() {
        for missing in [None, Some("")] {
            let mut context = AeronClusterContext::default()
                .ingress_channel(Some("aeron:udp"))
                .egress_channel(missing);

            let error = context.conclude().expect_err("no egress channel");

            assert_eq!("ERROR - egressChannel must be specified", error.to_string());
        }
    }

    /// `clientNameShouldHandleEmptyValue`.
    #[test]
    fn client_name_handles_an_empty_value() {
        for missing in [None, Some("")] {
            let mut properties = Properties::new();
            if let Some(value) = missing {
                properties
                    .apply_args([format!("-D{CLIENT_NAME_PROP_NAME}={value}")])
                    .expect("a property");
            }

            let context =
                AeronClusterContext::from_properties(&properties).expect("no name is no problem");

            assert_eq!("", context.client_name);
        }
    }

    /// `clientNameShouldReturnAssignedValue`.
    #[test]
    fn client_name_is_the_assigned_value() {
        for name in ["test", "Some other name"] {
            let mut context = context().client_name(name);
            context.conclude().expect("a legal name");
            assert_eq!(name, context.client_name);
        }
    }

    /// `clientNameCanBeSetViaSystemProperty`, read through this build's
    /// `Properties` instead of the process's own configuration.
    #[test]
    fn the_client_name_is_read_from_the_properties() {
        for name in ["some", "42"] {
            let mut properties = Properties::new();
            properties
                .apply_args([format!("-D{CLIENT_NAME_PROP_NAME}={name}")])
                .expect("a property");

            let context =
                AeronClusterContext::from_properties(&properties).expect("a readable context");

            assert_eq!(name, context.client_name);
        }
    }

    /// `clientNameMustNotExceedMaxLength`.
    ///
    /// The reference's generated name is `"test"` plus 100 `"x"`s — 104
    /// characters — so anything over 100 must be refused, and exactly 100 must
    /// not be.
    #[test]
    fn a_client_name_longer_than_the_driver_allows_is_refused() {
        let mut at_the_limit = context().client_name(&"x".repeat(MAX_CLIENT_NAME_LENGTH));
        at_the_limit
            .conclude()
            .expect("100 is the limit, not one short of it");

        let mut over_the_limit = context().client_name(&"x".repeat(MAX_CLIENT_NAME_LENGTH + 1));
        let error = over_the_limit.conclude().expect_err("101 characters");

        assert_eq!(
            "ERROR - AeronCluster.Context.clientName length must be <= 100",
            error.to_string()
        );
    }

    /// `AC:1498-1502`: a UDP egress channel gains `rejoin=false`, so the
    /// cluster creates a publication for this session rather than reusing one.
    #[test]
    fn a_udp_egress_channel_gets_rejoin_false() {
        let mut context = context();
        context.conclude().expect("a context that concludes");

        assert_eq!(
            Some("aeron:udp?endpoint=localhost:0|rejoin=false"),
            context.egress_channel.as_deref()
        );
    }

    /// An IPC egress channel is not a UDP one and is left exactly as it was:
    /// the parameter means nothing there, and the reference only adds it inside
    /// its `isUdp` branch.
    #[test]
    fn an_ipc_egress_channel_is_left_alone() {
        let mut context = context().egress_channel(Some("aeron:ipc"));
        context.conclude().expect("a context that concludes");

        assert_eq!(Some("aeron:ipc"), context.egress_channel.as_deref());
    }

    /// The reference's `ConcurrentConcludeException`: once, and once only.
    #[test]
    fn conclude_is_a_one_time_act() {
        let mut context = context();
        context.conclude().expect("the first");
        assert!(context.conclude().is_err(), "the second is refused");
    }

    /// The seven names, read from one `Properties` — the values the interop
    /// test and a deployment start from.
    #[test]
    fn from_properties_reads_all_seven() {
        let mut properties = Properties::new();
        properties
            .apply_args([
                "-Daeron.cluster.message.timeout=3s",
                "-Daeron.cluster.ingress.endpoints=0=localhost:20000",
                "-Daeron.cluster.ingress.channel=aeron:udp",
                "-Daeron.cluster.ingress.stream.id=301",
                "-Daeron.cluster.egress.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.egress.stream.id=302",
                "-Daeron.cluster.client.name=named",
            ])
            .expect("properties");

        let context = AeronClusterContext::from_properties(&properties).expect("readable");

        assert_eq!(3_000_000_000, context.message_timeout_ns);
        assert_eq!(
            Some("0=localhost:20000"),
            context.ingress_endpoints.as_deref()
        );
        assert_eq!(Some("aeron:udp"), context.ingress_channel.as_deref());
        assert_eq!(301, context.ingress_stream_id);
        assert_eq!(
            Some("aeron:udp?endpoint=localhost:0"),
            context.egress_channel.as_deref()
        );
        assert_eq!(302, context.egress_stream_id);
        assert_eq!("named", context.client_name);
    }
}
