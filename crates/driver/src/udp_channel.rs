//! A `aeron:udp` channel, with its parameters read into addresses.
//!
//! Mirrors `aeron-driver/src/main/c/media/aeron_udp_channel.c`: the parse that
//! turns a URI into the four addresses an endpoint works with
//! (`aeron_udp_channel_finish_parse`, `:278-523`), the canonical form that
//! decides when two channels are the same endpoint (`:148-208`), and the
//! address lookups underneath both (`aeron-client/src/main/c/util/
//! aeron_netutil.c:479-787`).
//!
//! # The four addresses
//!
//! A channel names one address — `endpoint=` — and what it *means* depends on
//! which side is reading it, because the two kinds of endpoint bind different
//! ones:
//!
//! * a **send** endpoint binds `local_control` and connects to `remote_data`
//!   (`aeron-driver/src/main/c/media/aeron_send_channel_endpoint.c:112-140`);
//! * a **receive** endpoint binds `remote_data` — the endpoint parameter is
//!   where a *subscriber* listens
//!   (`aeron-driver/src/main/c/media/aeron_receive_destination.c:47-76`).
//!
//! So for the common `aeron:udp?endpoint=host:port`, `local_data` and
//! `local_control` are the wildcard (the kernel picks the sending port) and
//! `remote_data` is `host:port`. TCP-era intuition says the publisher owns the
//! port; here it is the subscriber that binds it, and the publisher that sends
//! from a port it never named.
//!
//! # Which parameters this driver serves
//!
//! P1-4 is unicast only. `mtu`/`term-length`/`fc`/`cc`/`session-id`/… are
//! publication and subscription *params* rather than channel ones and are read
//! by [`crate::publication_params`]; what lives here is the transport: the
//! addresses, the socket buffer sizes, the receiver window, the tag and the
//! control mode. The timestamp-offset parameters are **refused** rather than
//! ignored — a driver that dropped them silently would serve a channel that
//! behaves like a different one — and so are `group`/`gtag`, whose flow-control
//! semantics are a later slice. Both refusals are recorded in `docs/compat.md`.
//!
//! # A multicast endpoint is a different shape
//!
//! When `endpoint=` names a group the four addresses stop meaning what they
//! mean for unicast (`aeron_udp_channel.c:421-441`):
//!
//! * `remote_data` is the group, and `remote_control` is the group with its
//!   **last byte incremented** (`aeron_multicast_control_address`, `:76-136`) —
//!   which is why the reference refuses a group whose last byte is even;
//! * `local_data` and `local_control` are both the *interface*, not a wildcard:
//!   a multicast socket has to name the interface it sends on and joins on, so
//!   the wildcard is a lookup here rather than an answer
//!   (`aeron_find_multicast_interface`, `:138-144`);
//! * the canonical form is built with the group's own address and **no** unique
//!   suffix, whatever the URI's tag says, so two subscribers to one group share
//!   one endpoint.
//!
//! `control-mode=response` is a channel like the others here: the reference
//! resolves its addresses down the same path as every other mode
//! (`aeron_udp_channel.c:346-381`), the only difference being that a response
//! channel with nothing else to distinguish it is allowed through
//! (`:336-344`) and that it is *not* a multi-destination mode
//! ([`ControlMode::is_multi_destination`]). What is special about a response
//! channel lives above this layer, in the conductor.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::channel_uri::{ChannelUri, Transport, UriError};
use crate::sys::{self, AddressFamily};

/// The tag of a channel that did not name one (`AERON_URI_INVALID_TAG`,
/// `aeron-client/src/main/c/uri/aeron_uri.h:37`).
pub const INVALID_TAG: i64 = -1;

/// `control-mode=` (`aeron_udp_channel.h:29-35`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlMode {
    /// Nothing named: the receiver answers to the source address of the data
    /// it receives (`aeron_publication_image.c:584-587`).
    None,
    /// A control address resolved on its own and re-resolved on a timer.
    Dynamic,
    /// The control address is the `control=` parameter, verbatim.
    Manual,
    /// A response channel's control address.
    Response,
}

impl ControlMode {
    /// Whether a channel in this mode is a **multi-destination** channel
    /// (`aeron_udp_channel_is_multi_destination`,
    /// `media/aeron_udp_channel.h:147-151`).
    ///
    /// That is not "has had a destination added": it is a property of the
    /// control mode, and it is what makes a channel a member of the
    /// multi-destination *category* — the thing that decides whether the send
    /// endpoint keeps a destination tracker, whether the channel has group
    /// semantics, and which flow-control supplier it gets.
    pub const fn is_multi_destination(self) -> bool {
        matches!(self, Self::Manual | Self::Dynamic)
    }
}

/// Which address the channel's `interface=` parameter asked for
/// (`aeron_interface_split`, `aeron-client/src/main/c/util/aeron_parse_util.c:455-560`).
#[derive(Clone, Debug, PartialEq, Eq)]
enum InterfaceSpec {
    /// Nothing named: the wildcard, and the kernel picks the address.
    Wildcard,
    /// A named interface, `{name:port}` (`aeron_netutil.c:571-632`).
    Named {
        /// The interface name, as written.
        name: String,
        /// The port to bind on it; zero when the spec named none.
        port: u16,
    },
    /// An address, with the prefix length its netmask is matched on.
    Address {
        /// The address as written.
        address: IpAddr,
        /// The prefix length — a bare address is a full-length host match.
        prefix_length: u8,
        /// The port to bind, from the spec's `:port` part.
        port: u16,
    },
}

/// A UDP channel, as the driver's endpoints see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UdpChannel {
    /// The URI the client wrote, which counters and log files quote back.
    pub original_uri: Vec<u8>,
    /// The form two channels have to agree on to be the same endpoint
    /// (`aeron_uri_udp_canonicalise`, `aeron_udp_channel.c:148-208`).
    pub canonical_form: String,
    /// The endpoint parameter: where data is sent, and — for a subscription —
    /// where the socket listens.
    pub remote_data: SocketAddr,
    /// The interface the local socket is bound to.
    pub local_data: SocketAddr,
    /// Where control frames are sent: the endpoint, unless the channel named
    /// an explicit `control=`.
    pub remote_control: SocketAddr,
    /// Where control frames are sent from: `local_data`, unless the channel
    /// named an explicit `control=`, in which case that address is bound.
    pub local_control: SocketAddr,
    /// The `tags=` channel tag, or [`INVALID_TAG`].
    pub tag_id: i64,
    /// The kernel's index for the interface, which the multicast options use
    /// and a unicast socket does not.
    pub interface_index: u32,
    /// The multicast hop limit, from `ttl=`; zero for a unicast channel and
    /// for a multicast one that named none — which the endpoint then reads as
    /// "use the driver's own default", because zero and "not set" are one
    /// value here (`aeron_send_channel_endpoint.c:129`). The reference clamps
    /// what it reads to 255 rather than refusing it.
    pub multicast_ttl: u8,
    /// Whether the URI named an endpoint at all — the difference between a
    /// channel that listens where it was told and one the kernel places.
    pub has_explicit_endpoint: bool,
    /// Whether the URI named a `control=`.
    pub has_explicit_control: bool,
    /// What `control-mode=` said.
    pub control_mode: ControlMode,
    /// Whether the endpoint is a multicast group
    /// (`aeron_is_addr_multicast(&endpoint_addr)`, `aeron_udp_channel.c:421`).
    ///
    /// It is read off the *endpoint*, not off the local side, and it is what
    /// decides the shape of the addresses above: the group for `remote_data`,
    /// the group with its last byte incremented for `remote_control`, and the
    /// interface for both local sides.
    pub is_multicast: bool,
    /// `so-sndbuf=`, in bytes; zero means the driver's configured default.
    pub socket_sndbuf_length: usize,
    /// `so-rcvbuf=`, in bytes; zero likewise.
    pub socket_rcvbuf_length: usize,
    /// `rcv-wnd=`, in bytes; zero means the subscription default.
    pub receiver_window_length: usize,
}

/// Why a URI could not become a channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UdpChannelError {
    /// The URI itself — scheme, parameters, their values as text.
    Uri(UriError),
    /// A channel the reference refuses with an invalid-channel error
    /// (`aeron_udp_channel.c:303-344`, `:383-393`): its parameters contradict
    /// each other or leave it nothing to connect to.
    InvalidChannel(String),
    /// A host, a service or an interface that did not resolve. The reference's
    /// answer is `EINVAL`, which reaches a client as a generic error.
    Resolution(String),
    /// A channel this build does not serve. A divergence from the reference,
    /// which would serve it — recorded in `docs/compat.md`.
    Unsupported(String),
}

impl UdpChannelError {
    /// The `ON_ERROR` code the reference answers with, by
    /// `aeron_driver_conductor_on_error`'s rule
    /// (`aeron-driver/src/main/c/aeron_driver_conductor.c:2326-2358`): an error
    /// the reference *named* arrives negated and is sent as its absolute value;
    /// an errno arrives positive and reaches the client as a generic error.
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Uri(
                UriError::InvalidScheme
                | UriError::TooLong { .. }
                | UriError::NotUtf8
                | UriError::MissingKey { .. }
                | UriError::MissingValue { .. },
            )
            | Self::InvalidChannel(_) => deepmsg_cnc::command::ERROR_CODE_INVALID_CHANNEL,
            Self::Unsupported(_) => deepmsg_cnc::command::ERROR_CODE_NOT_SUPPORTED,
            Self::Uri(_) | Self::Resolution(_) => deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
        }
    }
}

impl std::fmt::Display for UdpChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uri(error) => write!(f, "{error}"),
            Self::InvalidChannel(message)
            | Self::Resolution(message)
            | Self::Unsupported(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for UdpChannelError {}

impl From<UriError> for UdpChannelError {
    fn from(error: UriError) -> Self {
        Self::Uri(error)
    }
}

impl UdpChannel {
    /// Whether this is a multi-destination channel
    /// (`aeron_udp_channel_is_multi_destination`, `media/aeron_udp_channel.h:147-151`).
    pub const fn is_multi_destination(&self) -> bool {
        self.control_mode.is_multi_destination()
    }

    /// Whether this channel has group semantics
    /// (`aeron_udp_channel_has_group_semantics`, `media/aeron_udp_channel.h:153-156`):
    /// multicast, or multi-destination.
    ///
    /// The two are one predicate in the reference because both name a channel
    /// that may have several receivers at once, which is what the setup frame's
    /// `GROUP` flag and the log buffer's `group` byte are about.
    pub const fn has_group_semantics(&self) -> bool {
        self.is_multicast || self.control_mode.is_multi_destination()
    }

    /// Where a control frame about data that arrived from `source` goes
    /// (`aeron_receive_destination.c:120-129`, and the same three arms in
    /// `aeron_data_packet_dispatcher.c:636-637`).
    ///
    /// The multicast arm is the one that is easy to miss and impossible to
    /// work without: a group's control frames go to the group's **control
    /// twin**, not back to the source address they came from. A subscriber
    /// that answered the source would be answering a port the publisher never
    /// listens on, and the publisher would wait for a status message that can
    /// never arrive.
    pub fn control_address(&self, source: SocketAddr) -> SocketAddr {
        if self.is_multicast {
            self.remote_control
        } else if self.has_explicit_control {
            self.local_control
        } else {
            source
        }
    }

    /// Read a `aeron:udp` URI into the addresses an endpoint works with
    /// (`aeron_udp_channel_finish_parse`, `aeron_udp_channel.c:278-523`).
    ///
    /// # Errors
    ///
    /// [`UdpChannelError`] for a URI this build cannot serve, a channel the
    /// reference refuses, or an address that does not resolve.
    pub fn resolve(original_uri: &[u8], uri: &ChannelUri<'_>) -> Result<Self, UdpChannelError> {
        if uri.transport() != Transport::Udp {
            return Err(UdpChannelError::InvalidChannel(
                "UDP channels must use UDP URIs".to_owned(),
            ));
        }

        let endpoint = uri.value("endpoint");
        let control = uri.value("control");
        let control_mode = read_control_mode(uri)?;
        let channel_tag = channel_tag(uri);

        refuse_unsupported(uri)?;

        // `:331-344`: a UDP channel has to name something that distinguishes it
        // from every other channel — an endpoint, a control address, a tag, or
        // a control mode that supplies one.
        let has_no_distinguishing_characteristic =
            endpoint.is_none() && control.is_none() && channel_tag.is_none();

        if has_no_distinguishing_characteristic
            && !matches!(control_mode, ControlMode::Manual | ControlMode::Response)
        {
            return Err(UdpChannelError::InvalidChannel(
                "URIs for UDP must specify endpoint, control, tags, or \
                 control-mode=manual/response"
                    .to_owned(),
            ));
        }

        // `:346-374`: the control address resolves first, because it decides
        // the family of the endpoint when no endpoint was named.
        let explicit_control_addr = match control {
            Some(text) => Some(resolve_host_and_port(text)?),
            None => None,
        };

        let endpoint_addr = match endpoint {
            Some(text) => resolve_host_and_port(text)?,
            None => wildcard_socket(
                explicit_control_addr
                    .map_or(AddressFamily::Inet, |addr| AddressFamily::of(addr.ip())),
            ),
        };

        let tag_id = match channel_tag {
            Some(text) => parse_tag(text).ok_or_else(|| {
                UdpChannelError::InvalidChannel(format!(
                    "could not parse channel tag string: {text}"
                ))
            })?,
            None => INVALID_TAG,
        };

        let interface = read_interface(uri.value("interface"))?;

        // `:421-441`: a multicast endpoint is a group, so the two remote
        // addresses are the group and its control twin, and both local sides
        // are the *interface* — a socket that has to join and to send on a
        // chosen interface cannot leave either to the kernel. Note what the
        // branch does not do: an explicit `control=` is resolved above (and
        // still fails if it will not resolve) and then **ignored**, and the
        // tag stops mattering, because the canonical form below is built with
        // neither a tag nor a unique suffix.
        if is_multicast(endpoint_addr.ip()) {
            let (local, interface_index) = interface.resolve_multicast(endpoint_addr.ip())?;

            return Ok(Self {
                original_uri: original_uri.to_vec(),
                canonical_form: canonicalise(None, local, None, endpoint_addr, false, INVALID_TAG),
                remote_data: endpoint_addr,
                local_data: local,
                remote_control: multicast_control_address(endpoint_addr)?,
                local_control: local,
                tag_id,
                interface_index,
                multicast_ttl: read_multicast_ttl(uri),
                has_explicit_endpoint: endpoint.is_some(),
                has_explicit_control: false,
                control_mode,
                is_multicast: true,
                socket_sndbuf_length: read_size(uri, "so-sndbuf")?,
                socket_rcvbuf_length: read_size(uri, "so-rcvbuf")?,
                receiver_window_length: read_size(uri, "rcv-wnd")?,
            });
        }

        // `:376-381`: a channel nothing *identifies* gets a unique suffix in
        // its canonical form, so it is an endpoint of its own rather than one
        // shared with every other unaddressed channel.
        let requires_additional_suffix = (endpoint.is_none() && control.is_none())
            || (endpoint.is_some() && endpoint_addr.port() == 0)
            || explicit_control_addr.is_some_and(|addr| addr.port() == 0);

        let (local, interface_index) = interface.resolve(endpoint_addr.ip())?;

        let (local_data, local_control, has_explicit_control, canonical_form) =
            match (control, explicit_control_addr) {
                // `:442-464`: with an explicit control address the local socket
                // binds *there* — both the data and control sockets are the same
                // one — while the remote side stays the endpoint.
                (Some(text), Some(explicit)) => (
                    explicit,
                    explicit,
                    true,
                    canonicalise(
                        Some(text),
                        explicit,
                        endpoint,
                        endpoint_addr,
                        requires_additional_suffix,
                        tag_id,
                    ),
                ),
                // `:465-487`: otherwise the local side is the interface, and the
                // control addresses travel with the data ones.
                _ => (
                    local,
                    local,
                    false,
                    canonicalise(
                        None,
                        local,
                        endpoint,
                        endpoint_addr,
                        requires_additional_suffix,
                        tag_id,
                    ),
                ),
            };

        Ok(Self {
            original_uri: original_uri.to_vec(),
            canonical_form,
            remote_data: endpoint_addr,
            local_data,
            remote_control: endpoint_addr,
            local_control,
            tag_id,
            interface_index,
            multicast_ttl: 0,
            has_explicit_endpoint: endpoint.is_some(),
            has_explicit_control,
            control_mode,
            is_multicast: false,
            socket_sndbuf_length: read_size(uri, "so-sndbuf")?,
            socket_rcvbuf_length: read_size(uri, "so-rcvbuf")?,
            receiver_window_length: read_size(uri, "rcv-wnd")?,
        })
    }
}

/// `control-mode=`, with the reference's silence about unknown values
/// (`aeron_udp_channel.c:309-323`): a string that is none of the three is not
/// an error, it is simply no mode at all.
///
/// # Errors
///
/// [`UdpChannelError::InvalidChannel`] when `dynamic` is asked for without a
/// control address to resolve (`:325-329`).
fn read_control_mode(uri: &ChannelUri<'_>) -> Result<ControlMode, UdpChannelError> {
    let mode = match uri.value("control-mode") {
        Some("manual") => ControlMode::Manual,
        Some("dynamic") => ControlMode::Dynamic,
        Some("response") => ControlMode::Response,
        _ => ControlMode::None,
    };

    if mode == ControlMode::Dynamic && uri.value("control").is_none() {
        return Err(UdpChannelError::InvalidChannel(
            "explicit control expected with dynamic control mode".to_owned(),
        ));
    }

    Ok(mode)
}

/// The prefix a spy channel carries (`AERON_SPY_PREFIX`,
/// `aeron-client/src/main/c/uri/aeron_uri.h:36`).
pub const SPY_PREFIX: &str = "aeron-spy:";

/// The prefix an IPC channel carries (`AERON_IPC_CHANNEL`,
/// `aeron-client/src/main/c/uri/aeron_uri.h:35`), which the destination triage
/// matches on its length alone (`aeron_driver_conductor.c:3051`).
pub const IPC_PREFIX: &str = "aeron:ipc";

/// The keys a destination URI may not carry
/// (`AERON_DRIVER_CONDUCTOR_INVALID_DESTINATION_KEYS`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:52-60`).
///
/// Each of them describes the *channel a publication or subscription owns* —
/// its MTU, its window, its socket buffers, its response correlation — and a
/// destination is a place inside such a channel, not a channel of its own. A
/// destination that named one would be asking for a setting nothing would read.
pub const INVALID_DESTINATION_KEYS: [&str; 5] = [
    "mtu",
    "rcv-wnd",
    "so-rcvbuf",
    "so-sndbuf",
    "response-correlation-id",
];

/// Refuse a spy channel as a **send** destination
/// (`aeron_driver_conductor_validate_destination_uri_prefix`, `:392-407`).
///
/// This is the send half only. A **receive** destination that names a spy is
/// served — it is the third way a spy link is made, beside the two a
/// subscription makes (`aeron_driver_conductor_execute_add_receive_spy_destination`,
/// `:5704-5806`) — and the triage between the two is the caller's
/// ([`crate::conductor`]).
///
/// # Errors
///
/// [`UdpChannelError::InvalidChannel`], in the reference's words.
pub fn validate_destination_prefix(channel: &[u8], direction: &str) -> Result<(), UdpChannelError> {
    if is_spy_channel(channel) {
        return Err(UdpChannelError::InvalidChannel(format!(
            "Aeron spies are invalid as {direction} destinations: {}",
            String::from_utf8_lossy(channel)
        )));
    }

    Ok(())
}

/// Whether a client's channel is a spy channel (`AERON_SPY_PREFIX`,
/// `aeron-client/src/main/c/uri/aeron_uri.h:36`).
pub fn is_spy_channel(channel: &[u8]) -> bool {
    channel.starts_with(SPY_PREFIX.as_bytes())
}

/// Resolve the channel a spy names, with the `aeron-spy:` prefix taken off
/// (`aeron_driver_conductor_on_add_spy_subscription`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:4939-4942`, and its
/// destination twin at `:5821-5824`).
///
/// A spy is not a channel of its own: what follows the prefix is the UDP
/// channel the publication it reads sends on, parsed exactly as that
/// publication parsed it. Everything the match rule compares — the canonical
/// form and the channel tag — is read off this parse (`:92-108`); a spy whose
/// URI carries parameters the publisher's never mentioned is still reading the
/// same channel, because the canonical form quotes the two sides and nothing
/// else.
///
/// The address is resolved here, where the reference hands the text to its
/// native resource agent and resolves it off the conductor's thread. Both
/// arrive at the same channel; the difference is when, not what
/// (`docs/compat.md` carries the same deviation for every other channel).
///
/// # Errors
///
/// [`UdpChannelError::InvalidChannel`] when the prefix is not there — a caller
/// that has already triaged on [`is_spy_channel`] cannot see this one — and
/// whatever [`UdpChannel::resolve`] raises for the channel that follows it.
pub fn resolve_spy_channel(channel: &[u8]) -> Result<UdpChannel, UdpChannelError> {
    let inner = channel.strip_prefix(SPY_PREFIX.as_bytes()).ok_or_else(|| {
        UdpChannelError::InvalidChannel(format!(
            "not a spy channel: {}",
            String::from_utf8_lossy(channel)
        ))
    })?;

    let uri = ChannelUri::parse(inner)?;

    UdpChannel::resolve(inner, &uri)
}

/// A destination URI a send endpoint can be given, resolved to where it points
/// (`aeron_driver_conductor_validate_send_destination_uri`, `:5369-5410`, and
/// `aeron_driver_conductor_validate_destination_uri_params`, `:411-460`).
///
/// The resolution is **synchronous**, which is where this build's
/// `compat.md:281` deviation lives: the reference hands the name to a native
/// resource agent and answers the client before it is resolved. The behaviour a
/// caller sees is the same either way — a name that does not resolve becomes a
/// destination with no address — so the difference is when the work happens,
/// not what it produces.
///
/// # Errors
///
/// [`UdpChannelError::InvalidChannel`] for a URI that is not UDP, names no
/// endpoint, names port zero, carries one of [`INVALID_DESTINATION_KEYS`], or
/// asks for `control-mode=response`; and [`UdpChannelError::Resolve`] for a host
/// that does not resolve.
pub fn validate_send_destination_uri(channel: &[u8]) -> Result<SocketAddr, UdpChannelError> {
    let uri = ChannelUri::parse(channel)?;
    let text = String::from_utf8_lossy(channel);

    if uri.transport() != Transport::Udp || uri.value("endpoint").is_none() {
        return Err(UdpChannelError::InvalidChannel(format!(
            "incorrect URI format for destination: {text}"
        )));
    }

    for key in INVALID_DESTINATION_KEYS {
        if uri.value(key).is_some() {
            return Err(UdpChannelError::InvalidChannel(format!(
                "destinations must not contain the key: {key} channel={text}"
            )));
        }
    }

    if uri.value("control-mode") == Some("response") {
        return Err(UdpChannelError::InvalidChannel(format!(
            "destinations may not specify control-mode=response channel={text}"
        )));
    }

    let endpoint = uri.value("endpoint").expect("checked above");

    // Port zero is refused rather than resolved: a destination that names no
    // port is one the driver cannot send to, and the reference refuses it by
    // name before it resolves anything (`:5394-5404`).
    if let Some((_, port)) = endpoint.rsplit_once(':') {
        if "0" == port {
            return Err(UdpChannelError::InvalidChannel(format!(
                "endpoint has port=0 for send destination: channel={text}"
            )));
        }
    }

    resolve_host_and_port(endpoint)
}

/// The channel tag: `tags=` up to the first comma
/// (`aeron_udp_uri_params_func`, `aeron-client/src/main/c/uri/aeron_uri.c:166-177`).
///
/// The reference splits the value in place at the comma — the first field is
/// the channel tag, the rest the entity tag — so a value that starts with a
/// comma has no channel tag at all.
fn channel_tag<'a>(uri: &ChannelUri<'a>) -> Option<&'a str> {
    let value = uri.value("tags")?;
    let (tag, _) = value.split_once(',').unwrap_or((value, ""));

    (!tag.is_empty()).then_some(tag)
}

/// `strtoul(tag, &end, 10)` with the digits requirement but **no** trailing
/// check, which is what makes `tags=12x` tag twelve
/// (`aeron_uri_parse_tag`, `aeron-client/src/main/c/uri/aeron_uri.c:517-529`).
fn parse_tag(text: &str) -> Option<i64> {
    let digits = text.trim_start().trim_start_matches('+');
    let end = first_non_digit(digits);

    if end == 0 {
        return None;
    }

    digits[..end].parse().ok()
}

/// Where `text` stops being decimal digits.
fn first_non_digit(text: &str) -> usize {
    text.find(|character: char| !character.is_ascii_digit())
        .unwrap_or(text.len())
}

/// A `so-sndbuf`/`so-rcvbuf`/`rcv-wnd` parameter, read with the reference's
/// size reader.
fn read_size(uri: &ChannelUri<'_>, key: &str) -> Result<usize, UdpChannelError> {
    match uri.size(key)? {
        Some(value) => usize::try_from(value).map_err(|_| {
            UdpChannelError::Uri(UriError::OutOfRange {
                key: key.to_owned(),
                value: value.to_string(),
            })
        }),
        None => Ok(0),
    }
}

/// Refuse the parameters of the features this slice does not carry, rather
/// than ignoring them.
///
/// The reference would serve every one of these. A driver that dropped them
/// silently would be a driver whose channel behaved like a different one — a
/// multicast group that receives nothing, a timestamped image without
/// timestamps — so they are refused and `docs/compat.md` carries the
/// divergence.
///
/// # Errors
///
/// [`UdpChannelError::Unsupported`] for the first such parameter present.
fn refuse_unsupported(uri: &ChannelUri<'_>) -> Result<(), UdpChannelError> {
    for key in [
        "gtag",
        "media-rcv-ts-offset",
        "channel-rcv-ts-offset",
        "channel-snd-ts-offset",
        "ats",
    ] {
        if uri.value(key).is_some() {
            return Err(UdpChannelError::Unsupported(format!(
                "`{key}` is not served by this driver"
            )));
        }
    }

    Ok(())
}

/// Whether an address is one the kernel would treat as a multicast group
/// (`aeron_is_addr_multicast`,
/// `aeron-client/src/main/c/util/aeron_netutil.c:788-810`).
fn is_multicast(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => address.is_multicast(),
        IpAddr::V6(address) => address.is_multicast(),
    }
}

/// The control address of a multicast channel: the group with its last byte
/// incremented, same family and same port
/// (`aeron_multicast_control_address`, `aeron_udp_channel.c:76-136`).
///
/// The even-last-byte refusal is the reference's, and it is a real constraint
/// rather than a typo: control and data are two groups one apart, so a group
/// whose last byte is even would put its control group where some other
/// channel's data group is.
///
/// # Errors
///
/// [`UdpChannelError::Resolution`] when the last byte is even. The reference
/// answers `EINVAL` here and not an `AERON_ERROR_CODE_*`, so what a client
/// sees is a generic error (`aeron_driver_conductor.c:2326-2358`).
fn multicast_control_address(address: SocketAddr) -> Result<SocketAddr, UdpChannelError> {
    let even =
        || UdpChannelError::Resolution(format!("Multicast data address must be odd: {address}"));

    let incremented = match address.ip() {
        IpAddr::V4(group) => {
            let mut octets = group.octets();
            let last = octets[3];

            if last & 1 == 0 {
                return Err(even());
            }

            octets[3] = last + 1;
            IpAddr::V4(Ipv4Addr::from(octets))
        }

        IpAddr::V6(group) => {
            let mut octets = group.octets();
            let last = octets[15];

            if last & 1 == 0 {
                return Err(even());
            }

            octets[15] = last + 1;
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    };

    Ok(SocketAddr::new(incremented, address.port()))
}

/// `ttl=` as the multicast hop limit
/// (`aeron_uri_multicast_ttl`, `aeron-client/src/main/c/uri/aeron_uri.c:323-334`).
///
/// Three things the reference's `strtoull(value, NULL, 0)` brings with it, all
/// of them kept: the radix is the value's own (so `0x10` and `010` are hop
/// limits 16 and 8), text with no digits at all is **zero** rather than an
/// error, and a value above 255 is clamped rather than refused. A negative
/// value is the fourth: `strtoull` wraps it, so `ttl=-1` clamps to 255.
fn read_multicast_ttl(uri: &ChannelUri<'_>) -> u8 {
    let Some(text) = uri.value("ttl") else {
        return 0;
    };

    let value = if text.trim_start().starts_with('-') {
        u32::MAX
    } else {
        parse_base_zero(text).unwrap_or(0)
    };

    u8::try_from(value.min(255)).unwrap_or(255)
}

/// Read the `interface=` parameter into what it names
/// (`aeron_interface_split`, `aeron-client/src/main/c/util/aeron_parse_util.c:455-560`).
///
/// # Errors
///
/// [`UdpChannelError::Resolution`] for text that is none of the three shapes,
/// or a prefix or port that is not one.
fn read_interface(text: Option<&str>) -> Result<InterfaceSpec, UdpChannelError> {
    let bad = || {
        UdpChannelError::Resolution(format!(
            "could not parse interface='{}'",
            text.unwrap_or_default()
        ))
    };

    let Some(text) = text else {
        return Ok(InterfaceSpec::Wildcard);
    };

    if let Some(rest) = text.strip_prefix('{') {
        let Some((name, tail)) = rest.split_once('}') else {
            return Err(bad());
        };

        if name.is_empty() {
            return Err(bad());
        }

        let port = match tail {
            "" => 0,
            tail => {
                let Some(digits) = tail.strip_prefix(':') else {
                    return Err(UdpChannelError::Resolution(format!(
                        "unexpected character after name closing brace: {text}"
                    )));
                };

                parse_port(digits)
                    .map_err(|error| UdpChannelError::Resolution(format!("{error}: {text}")))?
            }
        };

        return Ok(InterfaceSpec::Named {
            name: name.to_owned(),
            port,
        });
    }

    let (host, prefix) = match text.split_once('/') {
        Some((host, prefix)) => (host, Some(prefix)),
        None => (text, None),
    };

    // An interface spec carries a port the same way an address does, and
    // unlike an address it may leave it out (`aeron_udp_port_resolver(.., true)`,
    // `aeron-client/src/main/c/util/aeron_netutil.c:154-186`).
    let (host, port) = match host.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => {
            let port = parse_port(port)
                .map_err(|error| UdpChannelError::Resolution(format!("{error}: {text}")))?;
            (host, port)
        }
        _ => (host, 0),
    };

    // A literal first, and only then a name — which is the reference's order
    // (`aeron_ipv4_addr_resolver`: `inet_pton`, then `getaddrinfo`).
    //
    // The family is **IPv4** for any spec that is not bracketed
    // (`aeron_parse_util.c:505-517`, where a missing `[...]` sets the hint to
    // 4), so an interface named `localhost` resolves the way every other
    // unbracketed host does. Since 1.53.2 resolves a host at all here, a name
    // was a *refusal* before — `interface=localhost` is written 25 times across
    // the reference's own system tests.
    let address = match host.parse::<IpAddr>() {
        Ok(address) => address,
        Err(_) => lookup(host, 0, AddressFamily::Inet)
            .map_err(|_| bad())?
            .map(|resolved| resolved.ip())
            .ok_or_else(bad)?,
    };

    let full_length = if address.is_ipv4() { 32 } else { 128 };

    let prefix_length = match prefix {
        Some(prefix) => parse_prefix_length(prefix, full_length)?,
        None => full_length,
    };

    Ok(InterfaceSpec::Address {
        address,
        prefix_length,
        port,
    })
}

/// `aeron_prefixlen_resolver` (`aeron-client/src/main/c/util/aeron_parse_util.c:565-605`):
/// a decimal number no larger than the family's address length. A value too
/// large is clamped rather than refused, which is what the reference does.
fn parse_prefix_length(text: &str, maximum: u8) -> Result<u8, UdpChannelError> {
    let value: u16 = text
        .parse()
        .map_err(|_| UdpChannelError::Resolution(format!("could not parse prefix: {text}")))?;

    Ok(u8::try_from(value.min(u16::from(maximum))).unwrap_or(maximum))
}

impl InterfaceSpec {
    /// Where the local socket binds, and the kernel's index for the interface
    /// (`aeron_find_unicast_interface`,
    /// `aeron-client/src/main/c/util/aeron_netutil.c:750-786`).
    ///
    /// # Errors
    ///
    /// [`UdpChannelError::Resolution`] when a named interface has no address,
    /// or an address is not one this host holds.
    fn resolve(&self, remote: IpAddr) -> Result<(SocketAddr, u32), UdpChannelError> {
        let family = AddressFamily::of(remote);

        match self {
            // No interface named: the wildcard, and the kernel picks.
            Self::Wildcard => Ok((wildcard_socket(family), 0)),

            Self::Named { name, port } => Self::resolve_by_name(family, name, *port),

            Self::Address {
                address,
                prefix_length,
                port,
            } => {
                // `:756-764`: a wildcard address written out is taken as
                // itself, without asking the kernel whether this host has it.
                if address.is_unspecified() {
                    return Ok((SocketAddr::new(*address, *port), 0));
                }

                Self::resolve_by_address(family, *address, *prefix_length, *port)
            }
        }
    }

    /// The same question for a **multicast** channel
    /// (`aeron_find_multicast_interface`, `aeron_udp_channel.c:138-144`, which
    /// is `aeron_find_interface` with the wildcard written out as the string
    /// `0.0.0.0/0` — `[0::]/0` for the other family).
    ///
    /// The difference from [`Self::resolve`] is exactly the two shortcuts that
    /// path takes and this one cannot: naming no interface, and naming the
    /// wildcard outright, both answer the wildcard *without asking the kernel*.
    /// A multicast socket needs an interface — it is what `IP_MULTICAST_IF`
    /// sends on and what a join is against — so here even the wildcard is a
    /// lookup, and the kernel's answer is the one `aeron_ip_lookup_func` picks:
    /// loopback if it is up, otherwise the multicast-capable interface with the
    /// longest netmask (`aeron_netutil.c:479-517`).
    ///
    /// # Errors
    ///
    /// As [`Self::resolve`], plus the empty host case: this host has no
    /// interface to join on.
    fn resolve_multicast(&self, remote: IpAddr) -> Result<(SocketAddr, u32), UdpChannelError> {
        let family = AddressFamily::of(remote);

        match self {
            Self::Named { name, port } => Self::resolve_by_name(family, name, *port),

            Self::Address {
                address,
                prefix_length,
                port,
            } => Self::resolve_by_address(family, *address, *prefix_length, *port),

            Self::Wildcard => {
                let wildcard = wildcard_socket(family);
                Self::resolve_by_address(family, wildcard.ip(), 0, wildcard.port())
            }
        }
    }

    /// A named interface, `{name:port}` (`aeron_find_interface_by_name_and_family`,
    /// `aeron_netutil.c:634-700`).
    ///
    /// # Errors
    ///
    /// [`UdpChannelError::Resolution`] when the interface does not exist, or
    /// holds no address in the family.
    fn resolve_by_name(
        family: AddressFamily,
        name: &str,
        port: u16,
    ) -> Result<(SocketAddr, u32), UdpChannelError> {
        let found = sys::interface_by_name(family, name)
            .map_err(|error| UdpChannelError::Resolution(format!("interface {name}: {error}")))?;

        // `:658-690`: an interface that exists but holds no address in the
        // family is refused, as is one that does not exist.
        let interface = found
            .ok_or_else(|| UdpChannelError::Resolution(format!("unknown interface {name}")))?;

        Ok((SocketAddr::new(interface.address, port), interface.index))
    }

    /// An address, matched under its netmask
    /// (`aeron_find_interface_by_address`, `aeron_netutil.c:702-732`).
    ///
    /// # Errors
    ///
    /// [`UdpChannelError::Resolution`] when no interface on this host matches.
    fn resolve_by_address(
        family: AddressFamily,
        address: IpAddr,
        prefix_length: u8,
        port: u16,
    ) -> Result<(SocketAddr, u32), UdpChannelError> {
        let found =
            sys::interface_for_address(family, address, prefix_length).map_err(|error| {
                UdpChannelError::Resolution(format!("interface {address}: {error}"))
            })?;

        let interface = found.ok_or_else(|| {
            UdpChannelError::Resolution(format!("could not find matching interface='{address}'"))
        })?;

        Ok((SocketAddr::new(interface.address, port), interface.index))
    }
}

/// The wildcard address for a family: what `aeron_set_ipv4_wildcard_host_and_port`
/// and its IPv6 twin write
/// (`aeron-client/src/main/c/util/aeron_netutil.c:360-375`).
fn wildcard_socket(family: AddressFamily) -> SocketAddr {
    match family {
        AddressFamily::Inet => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        AddressFamily::Inet6 => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
}

/// `host:port` into an address, the default resolver's synchronous path
/// (`aeron_name_resolver_resolve_host_and_port`,
/// `aeron-driver/src/main/c/aeron_name_resolver.c:129-215`, with the default
/// resolver's lookup doing nothing on the way, `:103-115`).
///
/// # Errors
///
/// [`UdpChannelError::Resolution`] when the text is not `host:port`, when the
/// port is not a port, or when the host does not resolve.
pub fn resolve_host_and_port(text: &str) -> Result<SocketAddr, UdpChannelError> {
    let (host, port_text, family) = split_address(text)?;
    let port = parse_port(port_text)
        .map_err(|error| UdpChannelError::Resolution(format!("{error}: {text}")))?;

    // `:158-178`: a literal address is taken as itself — no lookup, and no
    // chance of a name server changing the channel under the driver.
    if let Ok(address) = host.parse::<IpAddr>() {
        if AddressFamily::of(address) == family {
            return Ok(SocketAddr::new(address, port));
        }
    }

    lookup(host, port, family)
        .map_err(|error| UdpChannelError::Resolution(format!("{text}: {error}")))?
        .ok_or_else(|| UdpChannelError::Resolution(format!("could not resolve host: {text}")))
}

/// Ask the system to resolve `host` and keep the address of the family the
/// brackets asked for, which is what the reference's `getaddrinfo` hint does
/// (`aeron_ip_addr_resolver`,
/// `aeron-client/src/main/c/util/aeron_netutil.c:180-202`).
fn lookup(host: &str, port: u16, family: AddressFamily) -> std::io::Result<Option<SocketAddr>> {
    use std::net::ToSocketAddrs;

    let wanted = |address: &SocketAddr| {
        matches!(
            (family, address),
            (AddressFamily::Inet, SocketAddr::V4(_)) | (AddressFamily::Inet6, SocketAddr::V6(_))
        )
    };

    Ok((host, port).to_socket_addrs()?.find(wanted))
}

/// `aeron_address_split`
/// (`aeron-client/src/main/c/util/aeron_parse_util.c:350-450`): the host, the
/// port, and whether the brackets said IPv6.
///
/// The reference scans for the **last** `:` and the brackets rather than
/// parsing, which is why a bare IPv6 address is not an address here: it has no
/// brackets, so its last colon reads as the port separator and its host becomes
/// everything before it.
///
/// # Errors
///
/// [`UdpChannelError::Resolution`] for an empty address, unbalanced brackets,
/// or a colon with nothing after it.
fn split_address(text: &str) -> Result<(&str, &str, AddressFamily), UdpChannelError> {
    if text.is_empty() {
        return Err(UdpChannelError::Resolution("no address value".to_owned()));
    }

    let left_brace = text.find('[');
    let right_brace = text.rfind(']');
    let last_colon = text.rfind(':');

    if left_brace.is_none() && right_brace.is_none() {
        let Some(colon) = last_colon else {
            return Ok((text, "", AddressFamily::Inet));
        };

        if colon == text.len() - 1 {
            return Err(UdpChannelError::Resolution(format!("port invalid: {text}")));
        }

        return Ok((&text[..colon], &text[colon + 1..], AddressFamily::Inet));
    }

    let (Some(left), Some(right)) = (left_brace, right_brace) else {
        return Err(UdpChannelError::Resolution(format!(
            "host address invalid: {text}"
        )));
    };

    if right < left {
        return Err(UdpChannelError::Resolution(format!(
            "host address invalid: {text}"
        )));
    }

    // A port is only a colon *after* the closing bracket; everything inside is
    // the address.
    let port = match last_colon {
        Some(colon) if colon > right => &text[colon + 1..],
        _ => "",
    };

    // A scope (`fe80::1%eth0`) is not part of the address `inet_pton` reads.
    let host = &text[left + 1..right];
    let host = host.split('%').next().unwrap_or(host);

    Ok((host, port, AddressFamily::Inet6))
}

/// `aeron_udp_port_resolver`
/// (`aeron-client/src/main/c/util/aeron_netutil.c:154-186`): a `strtoul` in
/// base zero — hexadecimal and octal included — with the digits requirement but
/// **no** trailing check, so `40123x` is port 40123.
fn parse_port(text: &str) -> Result<u16, String> {
    let text = text.strip_prefix(':').unwrap_or(text);

    if text.is_empty() {
        return Err("port invalid: ''".to_owned());
    }

    let Some(value) = parse_base_zero(text) else {
        return Err(format!("port invalid: '{text}'"));
    };

    if value > u32::from(u16::MAX) {
        return Err(format!("port out of range: '{text}'"));
    }

    #[allow(clippy::cast_possible_truncation)] // bounded above
    Ok(value as u16)
}

/// `strtoul(value, &end, 0)`, accepting a value with trailing characters
/// because the reference never checks `*end`.
fn parse_base_zero(text: &str) -> Option<u32> {
    let digits = text.trim_start().trim_start_matches('+');

    if let Some(hex) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        return u32::from_str_radix(&hex[..take_digits(hex, 16)], 16).ok();
    }

    if digits.len() > 1 && digits.starts_with('0') {
        let octal = &digits[1..];
        return u32::from_str_radix(&octal[..take_digits(octal, 8)], 8).ok();
    }

    digits[..take_digits(digits, 10)].parse().ok()
}

/// How many leading characters of `text` are digits in `radix`.
fn take_digits(text: &str, radix: u32) -> usize {
    text.find(|character: char| character.to_digit(radix).is_none())
        .unwrap_or(text.len())
}

/// `aeron_format_source_identity`
/// (`aeron-client/src/main/c/util/aeron_netutil.c:848-882`): `ip:port`, with
/// IPv6 bracketed.
///
/// # Errors
///
/// [`UdpChannelError::Resolution`] when the identity does not fit the
/// reference's buffer, which is the only way that function fails.
pub fn format_source_identity(address: SocketAddr) -> Result<String, UdpChannelError> {
    let text = match address {
        SocketAddr::V4(address) => format!("{}:{}", address.ip(), address.port()),
        SocketAddr::V6(address) => format!("[{}]:{}", address.ip(), address.port()),
    };

    if text.len() >= AERON_NETUTIL_FORMATTED_MAX_LENGTH {
        return Err(UdpChannelError::Resolution(
            "source identity too long".to_owned(),
        ));
    }

    Ok(text)
}

/// `AERON_NETUTIL_FORMATTED_MAX_LENGTH`
/// (`aeron-client/src/main/c/util/aeron_netutil.h:56`).
const AERON_NETUTIL_FORMATTED_MAX_LENGTH: usize = 128;

/// The counter a canonical form that has to be unique counts from
/// (`unique_canonical_form_value`, `aeron_udp_channel.c:146-147`).
static UNIQUE_CANONICAL_FORM_VALUE: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(0);

/// `aeron_uri_udp_canonicalise`
/// (`aeron-driver/src/main/c/media/aeron_udp_channel.c:148-208`):
/// `UDP-<local>-<remote>`, where each side is the parameter the URI wrote or,
/// when it wrote none, the formatted address — plus a suffix when the channel
/// has to be unique.
///
/// Which is why two clients that name the same endpoint in different ways can
/// still share one endpoint, and why a channel whose port the kernel picks
/// never shares one.
fn canonicalise(
    local_param_value: Option<&str>,
    local_data: SocketAddr,
    remote_param_value: Option<&str>,
    remote_data: SocketAddr,
    make_unique: bool,
    tag: i64,
) -> String {
    let local = match local_param_value {
        Some(text) => text.to_owned(),
        None => format_source_identity(local_data).unwrap_or_default(),
    };

    let remote = match remote_param_value {
        Some(text) => text.to_owned(),
        None => format_source_identity(remote_data).unwrap_or_default(),
    };

    let suffix = if make_unique {
        if tag != INVALID_TAG {
            format!("#{tag}")
        } else {
            // The reference's get-and-add: the value used is the one *before*
            // the increment, so the first unique channel ends in `-0`.
            let value =
                UNIQUE_CANONICAL_FORM_VALUE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("-{value}")
        }
    } else {
        String::new()
    };

    format!("UDP-{local}-{remote}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(uri: &str) -> UdpChannel {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        UdpChannel::resolve(uri.as_bytes(), &parsed)
            .unwrap_or_else(|error| panic!("{uri}: {error}"))
    }

    fn refuse(uri: &str) -> UdpChannelError {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        UdpChannel::resolve(uri.as_bytes(), &parsed).expect_err("refused")
    }

    fn ipv4(text: &str) -> SocketAddr {
        text.parse().expect("an address")
    }

    #[test]
    fn an_endpoint_is_where_a_subscription_listens_and_a_publication_sends() {
        let channel = resolve("aeron:udp?endpoint=127.0.0.1:40123");

        assert_eq!(ipv4("127.0.0.1:40123"), channel.remote_data);
        assert_eq!(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            channel.local_data,
            "the local side is the wildcard: the kernel picks the sending port"
        );
        assert!(channel.has_explicit_endpoint);
        assert!(!channel.has_explicit_control);
        assert_eq!(ControlMode::None, channel.control_mode);
        assert_eq!(INVALID_TAG, channel.tag_id);
        assert!(!channel.is_multicast);
        assert_eq!(channel.remote_data, channel.remote_control);
        assert_eq!(channel.local_data, channel.local_control);
    }

    #[test]
    fn the_canonical_form_names_both_sides_and_ignores_what_does_not_move_them() {
        let one = resolve("aeron:udp?endpoint=127.0.0.1:40123");
        let two = resolve("aeron:udp?endpoint=127.0.0.1:40123|mtu=1408");

        assert_eq!(one.canonical_form, two.canonical_form);
        assert_eq!("UDP-0.0.0.0:0-127.0.0.1:40123", one.canonical_form);
    }

    #[test]
    fn a_channel_the_kernel_places_gets_a_unique_canonical_form() {
        // Nothing distinguishes this channel from the next one like it, so the
        // reference appends a counting suffix (`aeron_udp_channel.c:376-381`).
        let one = resolve("aeron:udp?control-mode=manual");
        let two = resolve("aeron:udp?control-mode=manual");

        assert_ne!(one.canonical_form, two.canonical_form);
        assert!(
            one.canonical_form.starts_with("UDP-0.0.0.0:0-0.0.0.0:0-"),
            "{}",
            one.canonical_form
        );
    }

    #[test]
    fn a_tag_takes_the_place_of_the_counting_suffix() {
        let channel = resolve("aeron:udp?control-mode=manual|tags=17");

        assert_eq!(17, channel.tag_id);
        assert!(
            channel.canonical_form.ends_with("#17"),
            "{}",
            channel.canonical_form
        );
    }

    #[test]
    fn a_channels_tag_is_the_first_of_the_comma_separated_pair() {
        assert_eq!(7, resolve("aeron:udp?endpoint=127.0.0.1:1|tags=7,8").tag_id);
        assert_eq!(7, resolve("aeron:udp?endpoint=127.0.0.1:1|tags=7").tag_id);
        // Nothing before the comma is no channel tag at all, which is not
        // fatal for a channel that named an endpoint.
        assert_eq!(
            INVALID_TAG,
            resolve("aeron:udp?endpoint=127.0.0.1:1|tags=,8").tag_id
        );
        // And a tag string with no digits is refused, as `aeron_uri_parse_tag`
        // refuses it.
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1:1|tags=x"),
            UdpChannelError::InvalidChannel(_)
        ));
    }

    #[test]
    fn a_channel_with_nothing_to_identify_it_is_refused() {
        // `aeron_udp_channel.c:331-344`.
        assert_eq!(
            UdpChannelError::InvalidChannel(
                "URIs for UDP must specify endpoint, control, tags, or \
                 control-mode=manual/response"
                    .to_owned()
            ),
            refuse("aeron:udp?mtu=1408")
        );

        // Unless a control mode says where it is — the two modes the reference
        // exempts (`:336-344`).
        assert_eq!(
            ControlMode::Manual,
            resolve("aeron:udp?control-mode=manual").control_mode
        );
        assert_eq!(
            ControlMode::Response,
            resolve("aeron:udp?control-mode=response").control_mode
        );
    }

    #[test]
    fn dynamic_control_mode_needs_a_control_address_to_resolve() {
        // `aeron_udp_channel.c:325-329`.
        assert_eq!(
            UdpChannelError::InvalidChannel(
                "explicit control expected with dynamic control mode".to_owned()
            ),
            refuse("aeron:udp?endpoint=127.0.0.1:40123|control-mode=dynamic")
        );

        let channel = resolve(
            "aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124|control-mode=dynamic",
        );
        assert_eq!(ControlMode::Dynamic, channel.control_mode);
        assert!(channel.has_explicit_control);
    }

    /// A spy is not a destination: it names a publication's own log buffer, not
    /// a place to send datagrams (`:392-407`).
    #[test]
    fn a_spy_is_refused_as_a_destination() {
        assert!(validate_destination_prefix(b"aeron:udp?endpoint=127.0.0.1:40123", "send").is_ok());
        assert!(matches!(
            validate_destination_prefix(b"aeron-spy:aeron:udp?endpoint=127.0.0.1:40123", "send"),
            Err(UdpChannelError::InvalidChannel(message))
                if message.starts_with("Aeron spies are invalid as send destinations:")
        ));
    }

    /// A spy names the channel it reads, with the prefix taken off — and the
    /// two channels it can then be compared with are the publisher's
    /// (`:4939-4942`, `:92-108`).
    #[test]
    fn a_spy_names_the_channel_after_its_prefix() {
        let spy = resolve_spy_channel(b"aeron-spy:aeron:udp?endpoint=127.0.0.1:40123")
            .expect("a spy channel");
        let plain = resolve("aeron:udp?endpoint=127.0.0.1:40123");

        assert_eq!(plain.canonical_form, spy.canonical_form);
        assert_eq!(plain.remote_data, spy.remote_data);
        assert_eq!(INVALID_TAG, spy.tag_id);

        // The tag is read off the inner channel too, and it is one of the two
        // things the match rule compares.
        let tagged = resolve_spy_channel(b"aeron-spy:aeron:udp?endpoint=127.0.0.1:1|tags=17,3")
            .expect("a spy channel");
        assert_eq!(17, tagged.tag_id);
        assert_eq!(resolve("aeron:udp?endpoint=127.0.0.1:1|tags=17,3"), tagged);

        // And a channel without the prefix is not a spy, which is a different
        // answer from a channel whose inner URI is malformed.
        assert!(is_spy_channel(b"aeron-spy:aeron:udp?endpoint=127.0.0.1:1"));
        assert!(!is_spy_channel(b"aeron:udp?endpoint=127.0.0.1:1"));
        assert!(matches!(
            resolve_spy_channel(b"aeron:udp?endpoint=127.0.0.1:1"),
            Err(UdpChannelError::InvalidChannel(message)) if message.starts_with("not a spy channel:")
        ));
    }

    /// What a destination URI has to be, and the four ways it can fail
    /// (`:5369-5410`, `:411-460`).
    #[test]
    fn a_send_destination_is_a_udp_endpoint_with_a_port() {
        assert_eq!(
            "127.0.0.1:40456".parse::<SocketAddr>().expect("an address"),
            validate_send_destination_uri(b"aeron:udp?endpoint=127.0.0.1:40456")
                .expect("a destination")
        );

        let refusal = |channel: &[u8]| {
            validate_send_destination_uri(channel)
                .expect_err("this destination is refused")
                .to_string()
        };

        // Not UDP at all.
        assert!(
            refusal(b"aeron:ipc?endpoint=127.0.0.1:40456")
                .contains("incorrect URI format for destination")
        );

        // UDP, but naming nowhere to send. Which of the two refusals it is
        // depends on where the URI parser gives up, and either is a refusal.
        assert!(
            validate_send_destination_uri(b"aeron:udp").is_err(),
            "a destination with no endpoint is refused"
        );

        // Naming nowhere to send *from* the far end's point of view.
        assert!(
            refusal(b"aeron:udp?endpoint=127.0.0.1:0")
                .contains("endpoint has port=0 for send destination")
        );

        // A key that belongs to the channel, not to a place inside it.
        for key in INVALID_DESTINATION_KEYS {
            let channel = format!("aeron:udp?endpoint=127.0.0.1:40456|{key}=1");
            assert!(
                refusal(channel.as_bytes())
                    .contains(&format!("destinations must not contain the key: {key}")),
                "{key}"
            );
        }

        // A response channel is a channel, not a destination.
        assert!(
            refusal(b"aeron:udp?endpoint=127.0.0.1:40456|control-mode=response")
                .contains("destinations may not specify control-mode=response")
        );
    }

    /// A multi-destination channel is one whose **control mode** says so, not
    /// one that has had a destination added
    /// (`aeron_udp_channel_is_multi_destination`, `media/aeron_udp_channel.h:147-151`).
    ///
    /// Three things hang off it: whether a send endpoint keeps a destination
    /// tracker, whether the channel has group semantics, and which flow-control
    /// supplier it is given.
    #[test]
    fn the_control_mode_is_what_makes_a_channel_multi_destination() {
        assert!(
            resolve("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual")
                .is_multi_destination()
        );
        assert!(
            resolve(
                "aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124|control-mode=dynamic"
            )
            .is_multi_destination()
        );

        assert!(!resolve("aeron:udp?endpoint=127.0.0.1:40123").is_multi_destination());
        assert!(
            !resolve("aeron:udp?endpoint=127.0.0.1:40123|control-mode=nonsense")
                .is_multi_destination(),
            "an unknown control mode is no mode at all (`:309-323`)"
        );

        // `response` is a control mode that is not a multi-destination one
        // (`media/aeron_udp_channel.h:147-151`), even though its channel now
        // resolves like any other.
        assert!(
            !resolve("aeron:udp?endpoint=127.0.0.1:40123|control-mode=response")
                .is_multi_destination()
        );
    }

    #[test]
    fn group_semantics_follow_the_multi_destination_category() {
        assert!(
            resolve("aeron:udp?endpoint=127.0.0.1:40123|control-mode=manual").has_group_semantics()
        );
        assert!(
            resolve(
                "aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124|control-mode=dynamic"
            )
            .has_group_semantics()
        );
        assert!(!resolve("aeron:udp?endpoint=127.0.0.1:40123").has_group_semantics());
    }

    #[test]
    fn an_unknown_control_mode_is_no_mode_at_all() {
        // `:309-323` matches the three known strings and leaves the rest
        // behind, which is what makes `control-mode=` plus an endpoint a
        // channel like any other.
        assert_eq!(
            ControlMode::None,
            resolve("aeron:udp?endpoint=127.0.0.1:40123|control-mode=nonsense").control_mode
        );
    }

    #[test]
    fn the_error_codes_are_the_ones_the_reference_answers_with() {
        // `-AERON_ERROR_CODE_INVALID_CHANNEL` is sent as its absolute value; a
        // resolution failure is an errno, which reaches the client as a
        // generic error (`aeron_driver_conductor.c:2326-2358`).
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_INVALID_CHANNEL,
            refuse("aeron:udp?mtu=1408").error_code()
        );
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
            refuse("aeron:udp?endpoint=nosuchhost.invalid:40123").error_code()
        );
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_NOT_SUPPORTED,
            refuse("aeron:udp?endpoint=224.0.1.1:40123|gtag=1").error_code()
        );
    }

    #[test]
    fn a_group_tag_is_still_refused_where_a_group_parameter_is_not() {
        assert!(matches!(
            refuse("aeron:udp?endpoint=224.0.1.1:40123|gtag=1"),
            UdpChannelError::Unsupported(_)
        ));

        // `group=` is a *subscription* parameter and never reached this layer
        // in the first place: it is read where a subscription is created, into
        // the parameters an image is built from
        // (`aeron_driver_uri.c:494-495`), so a channel that carries it resolves
        // like any other.
        let named = resolve("aeron:udp?endpoint=127.0.0.1:40123|group=true");
        assert!(!named.is_multicast);
        assert_eq!(ipv4("127.0.0.1:40123"), named.remote_data);
    }

    /// The interface this host would join a group on, asked of the kernel the
    /// same way the channel asks: a wildcard lookup, not the wildcard itself.
    fn interface_of_this_host(family: AddressFamily) -> sys::LocalInterface {
        sys::interface_for_address(family, wildcard_socket(family).ip(), 0)
            .expect("the interface list")
            .expect("this host has an interface to join on")
    }

    #[test]
    fn a_multicast_endpoint_is_the_group_and_its_control_twin() {
        let channel = resolve("aeron:udp?endpoint=224.0.1.1:40123");

        assert!(channel.is_multicast);
        assert!(channel.has_group_semantics());
        assert_eq!(ipv4("224.0.1.1:40123"), channel.remote_data);
        assert_eq!(
            ipv4("224.0.1.2:40123"),
            channel.remote_control,
            "the control group is the data group with its last byte incremented"
        );
        assert_eq!(
            channel.remote_control.port(),
            channel.remote_data.port(),
            "and the same port"
        );
    }

    #[test]
    fn a_multicast_channel_binds_the_interface_not_the_wildcard() {
        let channel = resolve("aeron:udp?endpoint=224.0.1.1:40123");
        let interface = interface_of_this_host(AddressFamily::Inet);

        assert_eq!(channel.local_data, channel.local_control);
        assert_eq!(interface.address, channel.local_data.ip());
        assert_eq!(interface.index, channel.interface_index);
        assert!(!channel.local_data.ip().is_unspecified());
        assert!(!channel.has_explicit_control);
        assert_eq!(
            format!("UDP-{}:0-224.0.1.1:40123", interface.address),
            channel.canonical_form
        );
    }

    #[test]
    fn an_interface_named_by_host_resolves_like_any_other_host() {
        // `interface=localhost` is written 25 times across 17 of the
        // reference's own system-test files, and until this it was refused:
        // the host went through `parse::<IpAddr>` and a name is not a literal.
        // The reference tries the literal first and then resolves
        // (`aeron_netutil.c:115-126`), and every unbracketed spec is resolved
        // as IPv4 (`aeron_parse_util.c:505-517`).
        let named = resolve("aeron:udp?endpoint=127.0.0.1:40123|interface=localhost");
        let literal = resolve("aeron:udp?endpoint=127.0.0.1:40123|interface=127.0.0.1");

        assert_eq!(
            literal.local_data, named.local_data,
            "a name that resolves to the literal is the same interface"
        );
        assert_eq!(literal.interface_index, named.interface_index);

        // And it is a real name that is resolved, not a special case for this
        // one: a name that does not resolve is still refused.
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1:40123|interface=nosuchhost.invalid"),
            UdpChannelError::Resolution(_)
        ));
    }

    #[test]
    fn a_multicast_interface_is_looked_up_where_a_unicast_one_is_taken_as_written() {
        // The same parameter, two answers, and the difference is the point.
        // `aeron_find_unicast_interface` hands a wildcard address back as
        // itself (`aeron_netutil.c:750-786`); `aeron_find_multicast_interface`
        // goes straight to `aeron_find_interface` (`aeron_udp_channel.c:138-144`),
        // and a multicast socket cannot send on or join through `0.0.0.0`, so
        // there is no wildcard to hand back — the lookup has to answer.
        assert!(
            resolve("aeron:udp?endpoint=127.0.0.1:40123|interface=0.0.0.0")
                .local_data
                .ip()
                .is_unspecified()
        );

        // Written without a prefix it is a **host** match — the reference
        // reads no prefix as the family's full length (`aeron_netutil.c:187-192`)
        // — so the lookup finds nothing and the channel is refused rather than
        // silently bound.
        assert!(matches!(
            refuse("aeron:udp?endpoint=224.0.1.1:40123|interface=0.0.0.0"),
            UdpChannelError::Resolution(_)
        ));

        // Written as the wildcard *net* it is what naming no interface also
        // resolves to (`aeron_udp_channel.c:140`), and the kernel picks.
        assert_eq!(
            interface_of_this_host(AddressFamily::Inet).address,
            resolve("aeron:udp?endpoint=224.0.1.1:40123|interface=0.0.0.0/0")
                .local_data
                .ip()
        );
    }

    #[test]
    fn a_multicast_six_group_is_incremented_at_the_end_of_its_sixteen_bytes() {
        let channel = resolve("aeron:udp?endpoint=[ff02::1]:40123");

        assert_eq!(
            "[ff02::2]:40123".parse::<SocketAddr>().expect("an address"),
            channel.remote_control
        );
        assert!(channel.is_multicast);
    }

    #[test]
    fn a_group_whose_last_byte_is_even_is_refused() {
        assert!(matches!(
            refuse("aeron:udp?endpoint=224.0.1.2:40123"),
            UdpChannelError::Resolution(_)
        ));
        assert!(matches!(
            refuse("aeron:udp?endpoint=[ff02::2]:40123"),
            UdpChannelError::Resolution(_)
        ));

        // The reference answers `EINVAL` rather than an `AERON_ERROR_CODE_*`,
        // so what a client sees is a generic error.
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
            refuse("aeron:udp?endpoint=224.0.1.2:40123").error_code()
        );
    }

    #[test]
    fn a_group_is_the_same_endpoint_whatever_the_tag_says() {
        let tagged = resolve("aeron:udp?endpoint=224.0.1.1:40123|tags=7");
        let plain = resolve("aeron:udp?endpoint=224.0.1.1:40123");

        assert_eq!(7, tagged.tag_id, "the tag is still read");
        assert_eq!(
            plain.canonical_form, tagged.canonical_form,
            "but the canonical form of a group takes no suffix (`:421-441`)"
        );
    }

    #[test]
    fn a_groups_control_frames_go_to_its_control_twin() {
        let source = ipv4("127.0.0.1:5555");

        assert_eq!(
            ipv4("224.0.1.2:40123"),
            resolve("aeron:udp?endpoint=224.0.1.1:40123").control_address(source),
            "a group answers on its control twin, not to whoever wrote"
        );
        assert_eq!(
            ipv4("127.0.0.1:40124"),
            resolve("aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124")
                .control_address(source)
        );
        assert_eq!(
            source,
            resolve("aeron:udp?endpoint=127.0.0.1:40123").control_address(source)
        );
    }

    #[test]
    fn a_multicast_endpoint_with_a_control_address_ignores_it() {
        let channel = resolve("aeron:udp?endpoint=224.0.1.1:40123|control=127.0.0.1:40124");

        assert_eq!(ipv4("224.0.1.2:40123"), channel.remote_control);
        assert!(!channel.has_explicit_control);
        assert_eq!(channel.local_data, channel.local_control);
    }

    #[test]
    fn the_hop_limit_is_read_the_way_strtoull_reads_it() {
        let hop_limit = |uri: &str| resolve(uri).multicast_ttl;

        assert_eq!(0, hop_limit("aeron:udp?endpoint=224.0.1.1:40123"));
        assert_eq!(16, hop_limit("aeron:udp?endpoint=224.0.1.1:40123|ttl=0x10"));
        assert_eq!(8, hop_limit("aeron:udp?endpoint=224.0.1.1:40123|ttl=010"));
        assert_eq!(
            255,
            hop_limit("aeron:udp?endpoint=224.0.1.1:40123|ttl=300"),
            "clamped rather than refused"
        );
        assert_eq!(
            0,
            hop_limit("aeron:udp?endpoint=224.0.1.1:40123|ttl=slow"),
            "text with no digits at all is zero, not an error"
        );
        assert_eq!(
            255,
            hop_limit("aeron:udp?endpoint=224.0.1.1:40123|ttl=-1"),
            "a sign wraps in `strtoull` before it clamps"
        );

        assert_eq!(
            0,
            resolve("aeron:udp?endpoint=127.0.0.1:40123|ttl=3").multicast_ttl,
            "a unicast channel has no hop limit at all (`:389`, `:446`, `:472`)"
        );
    }

    #[test]
    fn a_port_is_read_the_way_strtoul_reads_it() {
        assert_eq!(
            ipv4("127.0.0.1:40123"),
            resolve("aeron:udp?endpoint=127.0.0.1:40123x").remote_data,
            "the reference never looks past the digits"
        );
        assert_eq!(
            ipv4("127.0.0.1:31"),
            resolve("aeron:udp?endpoint=127.0.0.1:0x1f").remote_data,
            "base zero, so hexadecimal counts"
        );
        assert_eq!(
            ipv4("127.0.0.1:9"),
            resolve("aeron:udp?endpoint=127.0.0.1:011").remote_data,
            "and so does octal"
        );
        assert_eq!(
            ipv4("127.0.0.1:0"),
            resolve("aeron:udp?endpoint=127.0.0.1:0").remote_data,
            "a zero port is a port: the wildcard one"
        );
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1"),
            UdpChannelError::Resolution(_)
        ));
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1:99999"),
            UdpChannelError::Resolution(_)
        ));
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1:"),
            UdpChannelError::Resolution(_)
        ));
    }

    #[test]
    fn a_host_is_resolved_by_the_system_when_it_is_not_a_literal() {
        assert_eq!(
            ipv4("127.0.0.1:40123"),
            resolve("aeron:udp?endpoint=localhost:40123").remote_data
        );
    }

    #[test]
    fn an_explicit_control_address_binds_the_local_side_to_it() {
        // `aeron_udp_channel.c:442-464`.
        let channel = resolve("aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124");

        assert_eq!(ipv4("127.0.0.1:40124"), channel.local_data);
        assert_eq!(ipv4("127.0.0.1:40124"), channel.local_control);
        assert_eq!(
            ipv4("127.0.0.1:40123"),
            channel.remote_data,
            "the data still goes to the endpoint"
        );
        assert!(channel.has_explicit_control);
        assert!(
            channel
                .canonical_form
                .starts_with("UDP-127.0.0.1:40124-127.0.0.1:40123")
        );
    }

    #[test]
    fn a_six_channel_is_bracketed_in_its_canonical_form() {
        let channel = resolve("aeron:udp?endpoint=[::1]:40123");

        assert_eq!(
            "[::1]:40123".parse::<SocketAddr>().ok(),
            Some(channel.remote_data)
        );
        assert_eq!(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
            channel.local_data,
            "the wildcard follows the endpoint's family"
        );
        assert_eq!("UDP-[::]:0-[::1]:40123", channel.canonical_form);
    }

    #[test]
    fn a_bare_six_address_is_not_one_the_reference_accepts_either() {
        // No brackets means the last colon reads as the port separator, so the
        // host is everything before it — which is not an address.
        assert!(matches!(
            refuse("aeron:udp?endpoint=::1:40123"),
            UdpChannelError::Resolution(_)
        ));
        // A bracketed address with no port is likewise refused: an endpoint
        // needs one.
        assert!(matches!(
            refuse("aeron:udp?endpoint=[::1]"),
            UdpChannelError::Resolution(_)
        ));
        // Unbalanced brackets are not an address at all.
        assert!(matches!(
            refuse("aeron:udp?endpoint=[::1:40123"),
            UdpChannelError::Resolution(_)
        ));
    }

    #[test]
    fn the_socket_buffer_and_window_parameters_are_read_as_sizes() {
        let channel =
            resolve("aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=1m|so-rcvbuf=128k|rcv-wnd=64k");

        assert_eq!(1024 * 1024, channel.socket_sndbuf_length);
        assert_eq!(128 * 1024, channel.socket_rcvbuf_length);
        assert_eq!(64 * 1024, channel.receiver_window_length);

        assert_eq!(
            0,
            resolve("aeron:udp?endpoint=127.0.0.1:1").socket_rcvbuf_length
        );
    }

    #[test]
    fn an_interface_by_address_is_looked_up_on_this_host() {
        // The loopback address is on every host this runs on, and the lookup
        // is the kernel's — which is what the address-bearing arm exists for.
        let channel = resolve("aeron:udp?endpoint=127.0.0.1:40123|interface=127.0.0.1");

        assert_eq!(IpAddr::V4(Ipv4Addr::LOCALHOST), channel.local_data.ip());
        assert_eq!(0, channel.local_data.port(), "no port was named");
    }

    #[test]
    fn the_wildcard_interface_is_taken_as_written() {
        let channel = resolve("aeron:udp?endpoint=127.0.0.1:40123|interface=0.0.0.0");

        assert_eq!(IpAddr::V4(Ipv4Addr::UNSPECIFIED), channel.local_data.ip());
        assert_eq!(0, channel.interface_index);
    }

    #[test]
    fn an_interface_this_host_does_not_have_is_refused() {
        let error = refuse("aeron:udp?endpoint=127.0.0.1:40123|interface=10.255.255.1");

        assert!(matches!(error, UdpChannelError::Resolution(_)), "{error}");
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1:40123|interface={nosuchiface}"),
            UdpChannelError::Resolution(_)
        ));
        assert!(matches!(
            refuse("aeron:udp?endpoint=127.0.0.1:40123|interface=not-an-address"),
            UdpChannelError::Resolution(_)
        ));
    }

    #[test]
    fn the_unsupported_parameters_are_named_rather_than_dropped() {
        for uri in [
            "aeron:udp?endpoint=127.0.0.1:40123|media-rcv-ts-offset=0",
            "aeron:udp?endpoint=127.0.0.1:40123|channel-rcv-ts-offset=0",
            "aeron:udp?endpoint=127.0.0.1:40123|ats=1",
        ] {
            let error = refuse(uri);
            assert!(
                matches!(error, UdpChannelError::Unsupported(_)),
                "{uri}: {error}"
            );
        }
    }

    #[test]
    fn a_response_channel_resolves_its_addresses_like_any_other() {
        // The reference sends every control mode down the same resolution
        // (`aeron_udp_channel.c:346-381`), and a response channel names its
        // control address the way a manual one does.
        let response = resolve(
            "aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124|control-mode=response",
        );
        let manual = resolve(
            "aeron:udp?endpoint=127.0.0.1:40123|control=127.0.0.1:40124|control-mode=manual",
        );

        assert_eq!(ControlMode::Response, response.control_mode);
        assert_eq!(manual.remote_data, response.remote_data);
        assert_eq!(manual.local_data, response.local_data);
        assert_eq!(manual.local_control, response.local_control);
        assert!(response.has_explicit_control);

        // A response channel is *not* multi-destination, so it keeps the
        // single-endpoint shape rather than the tracker's.
        assert!(!response.is_multi_destination());
        assert!(manual.is_multi_destination());

        // And it is allowed to name nothing else at all (`:336-344`).
        assert_eq!(
            ControlMode::Response,
            resolve("aeron:udp?control-mode=response").control_mode
        );
    }

    #[test]
    fn a_channel_that_is_not_udp_is_not_a_udp_channel() {
        let uri = ChannelUri::parse(b"aeron:ipc").expect("a URI");

        assert_eq!(
            UdpChannelError::InvalidChannel("UDP channels must use UDP URIs".to_owned()),
            UdpChannel::resolve(b"aeron:ipc", &uri).expect_err("refused")
        );
    }
}
