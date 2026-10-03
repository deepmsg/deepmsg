//! The subscriptions a driver owns, and the images they are given.
//!
//! Mirrors the IPC half of the subscription path in
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`: `on_add_ipc_subscription`
//! (`:4741-4824`), the match rule (`:110-117`), and `link_subscribable`
//! (`:3547-3617`), which is where a subscription becomes a *reader* — a
//! position counter, an entry in the publication's subscribable set, and an
//! `ON_AVAILABLE_IMAGE` telling the client where the log buffer is. (M20)
//!
//! # Two halves of one thing
//!
//! A subscription is stored here and a publication is stored in
//! [`crate::ipc_publications`], but neither is usable without the other: the
//! subscription's state is the *set* of (publication, counter) pairs it reads,
//! and the publication's limit is computed from the positions in it. The
//! reference keeps two arrays on the conductor and links across them in one
//! function; this keeps them in two modules and links across them in
//! [`link_subscribable`], which takes both.
//!
//! # One set of links, two kinds of publication
//!
//! A subscription is a *reader*, and what it reads is either an IPC
//! publication's log buffer — which this driver made and its producer writes —
//! or a **network image** built from datagrams (`crate::publication_images`).
//! The two are the same kind of thing to a link: a counter for the client's
//! position, and an entry in a set the far side computes its limit from. That
//! is why [`SubscriptionLinkEntry`] holds a [`SubscriptionTarget`] rather than a
//! publication id, and why this module is still called `IpcSubscriptions` for
//! historical reasons only — the links are the driver's, not IPC's.
//!
//! # Matching is a question about the stream, not the channel
//!
//! `link.stream_id == publication.stream_id`, and then the session: a
//! subscription that named one only matches that session, and one that named
//! none matches **every** session on the stream — which is how a subscriber to
//! `aeron:ipc` sees a publisher that has not started yet, and how it sees the
//! next one after that publisher dies. The channel string itself is never
//! compared: two clients can name the same stream with different URIs, and one
//! of them may have parameters the other never heard of.

use deepmsg_cnc::command::{
    AddSubscriptionCommand, CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED, DestinationCommandReceived,
    ERROR_CODE_EINVAL, ERROR_CODE_GENERIC_ERROR, ImageBuffersReady,
};
use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::channel_uri::{ChannelUri, Transport, UriError};
use crate::clients::{ClientEvents, Clients};
use crate::config::{DriverConfig, InferableBoolean};
use crate::ipc_publications::{AddError, IpcPublications};
use crate::network_publications::{NetworkPublicationRecord, NetworkPublications};
use crate::publication_images::PublicationImages;
use crate::publication_params::{PublicationParamsError, SubscriptionParams};
use crate::receive_endpoints::{ReceiveChannelEndpoints, ReceiveEndpointErrorKind};
use crate::receiver::ReceiverProxy;
use crate::sender::SenderProxy;
use crate::subscribable::TetherState;
use crate::subscribable::TetherablePosition;
use crate::udp_channel::{ControlMode, INVALID_TAG, UdpChannel};
use crate::{ipc_publication::IpcPublication, position as counter_position};

/// The channel an IPC image reports as its source
/// (`AERON_IPC_CHANNEL`, `aeron-client/src/main/c/uri/aeron_uri.h:39`).
///
/// The *constant*, not the channel the client subscribed with: two clients can
/// each name `aeron:ipc` differently — one with parameters, one without — and
/// an image's source identity is about where the bytes come from rather than
/// about what the reader asked for.
pub const IPC_CHANNEL: &[u8] = b"aeron:ipc";

/// What a reader reads (`aeron_subscribable_list_entry_t`'s `subscribable`
/// pointer, `aeron-driver/src/main/c/aeron_driver_conductor.h:113-118`).
///
/// The reference holds a pointer to whichever subscribable — a publication's or
/// an image's. Here the two collections are separate, so the *kind* is what
/// survives; the id is a registration id in both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionTarget {
    /// An IPC publication's log buffer.
    IpcPublication(i64),
    /// A network image's log buffer, built from datagrams.
    Image(i64),
    /// A **network publication's** log buffer, read locally without a socket:
    /// what a spy reads (`aeron_driver_conductor_link_subscribable` called
    /// with a network publication's subscribable,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:4897-4921`).
    ///
    /// It is the same kind of thing to a link as the other two — a log buffer
    /// in this process and a counter the client advances — and it is *not* an
    /// image: an image is built from datagrams and belongs to the receiver,
    /// while this is the buffer the producer is writing into right now.
    NetworkPublication(i64),
}

impl SubscriptionTarget {
    /// The registration id of whatever this points at.
    pub const fn registration_id(self) -> i64 {
        match self {
            Self::IpcPublication(registration_id)
            | Self::Image(registration_id)
            | Self::NetworkPublication(registration_id) => registration_id,
        }
    }

    /// Whether this is an image.
    pub const fn is_image(self) -> bool {
        matches!(self, Self::Image(_))
    }
}

/// One (subscription, publication) pair: a reader position, and the counter it
/// is written through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionLinkEntry {
    /// Which publication or image this reader reads.
    pub target: SubscriptionTarget,
    /// The `sub-pos` counter the client writes its position into.
    pub counter_id: i32,
}

/// How far a network subscription's setup has got
/// (`aeron_subscription_link_setup_status_t`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.h:94-100`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupStatus {
    /// Nothing has answered yet. Every subscription starts here; for an IPC one
    /// and for every network mode but response, it is also where it ends,
    /// because nothing consults it.
    Pending,
    /// A RSP_SETUP named this subscription's session.
    Complete,
    /// A RSP_SETUP arrived with a session the subscription's own `session-id=`
    /// contradicts. It will never be read, and later response setups for the
    /// same correlation id are ignored (`aeron_driver_conductor.c:7069-7070`).
    Error,
}

/// One subscription (`aeron_subscription_link_t`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.h:102-131`).
///
/// The reference's struct is wider than this and most of the difference is
/// network: a spy channel, a setup status and the group consideration are all
/// about a transport that has to be *built* before it can be read from. An IPC
/// subscription has nothing to set up — the log buffer it reads already exists
/// — which is why this is the flags, the identity, the readers and, for a
/// network subscription, [`SubscriptionLink::endpoint_id`].
///
/// The endpoint was left out until something needed it. `ADD_RCV_DESTINATION`
/// is what needs it: a client adds a source to **a subscription**, and what a
/// destination is added to is that subscription's receive endpoint
/// (`aeron_driver_conductor.c:5879-5910`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionLink {
    /// The client's correlation id for the `ADD_SUBSCRIPTION`.
    pub registration_id: i64,
    /// The client that owns it.
    pub client_id: i64,
    /// The stream it reads.
    pub stream_id: i32,
    /// The session it named, if it named one.
    pub session_id: Option<i32>,
    /// The channel as the client wrote it, which the `sub-pos` counters quote
    /// back in their keys and labels.
    pub channel: Vec<u8>,
    /// Whether it asked to keep its position whatever it costs.
    pub is_tether: bool,
    /// Whether it is re-joining a stream it left.
    pub is_rejoin: bool,
    /// Whether it is a response channel.
    pub is_response: bool,
    /// What its `group=` said, or the driver's consideration when it said
    /// nothing (`aeron_subscription_link_t`'s `group`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.h:113`).
    ///
    /// It is kept on the **link** and not on the channel because an image is
    /// created from a `SETUP`, which arrives long after the subscription did —
    /// and because the channel a `SETUP` names is the endpoint's, which for a
    /// multi-destination subscription is not the one that carried the
    /// parameter.
    pub group: InferableBoolean,
    /// How far its setup has got
    /// (`AERON_SUBSCRIPTION_LINK_SETUP_STATUS_*`,
    /// `aeron_driver_conductor.h:94-100`).
    ///
    /// Only a response subscription ever leaves [`SetupStatus::Pending`]: the
    /// reference sets it for every network subscription but reads it in one
    /// place, [`IpcSubscriptions::on_response_setup`], where `Complete` means
    /// the setup that just arrived is a second one.
    pub setup_status: SetupStatus,
    /// Whether the channel is reliable. No effect on IPC, recorded because the
    /// link is where the reference records it.
    pub is_reliable: bool,
    /// Whether its log buffers are sparse. No effect on IPC, likewise.
    pub is_sparse: bool,
    /// The receive endpoint this subscription reads through, or [`None`] for an
    /// IPC one — which has no socket, and so no destinations to add to it.
    pub endpoint_id: Option<u64>,
    /// The channel a **spy** subscription reads, or [`None`] for every other
    /// kind (`aeron_subscription_link_t`'s `spy_channel`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.h:102-131`).
    ///
    /// It is the field that separates the two endpoints-less kinds: an IPC
    /// subscription has no channel of its own beyond the string the client
    /// wrote, while a spy has one it must *match against* — the channel the
    /// publication it wants sends on. [`SubscriptionLink::channel`] stays what
    /// the client wrote, prefix and all, because that is what the reference
    /// puts in the reader's counter label
    /// (`aeron_driver_init_subscription_channel`, `:780-788`, which copies the
    /// command's own bytes).
    pub spy_channel: Option<UdpChannel>,
    /// What it reads: one entry per publication it was matched with.
    pub subscribables: Vec<SubscriptionLinkEntry>,
}

impl SubscriptionLink {
    /// Whether this subscription is already reading that publication
    /// (`aeron_driver_conductor_is_subscribable_linked`, `:3519-3523`).
    pub fn reads(&self, publication_registration_id: i64) -> bool {
        self.subscribables
            .iter()
            .any(|entry| entry.target.registration_id() == publication_registration_id)
    }

    /// Whether this subscription reads that publication *as an image*.
    pub fn reads_image(&self, image_registration_id: i64) -> bool {
        self.reads(image_registration_id)
    }

    /// Whether this subscription reads an image on `endpoint_id` for
    /// `(stream_id, session_id)`
    /// (`aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:81-90`, whose two
    /// clauses are the endpoint and
    /// [`is_wildcard_or_session_id_match`](Self::matches_image)).
    ///
    /// The endpoint clause is the one a reader does not expect: an image is a
    /// session read through **one** receive endpoint, so a subscription on
    /// another channel shares neither its socket nor its session however much
    /// the stream and session numbers agree. A subscription with no endpoint
    /// at all is an IPC one, and no image is ever its.
    ///
    /// The session clause is the reference's `:75-79`, and the `is_response`
    /// in it is load-bearing: a response subscription that named no session is
    /// **not** a wildcard looking for whatever appears. It is waiting for a
    /// RSP_SETUP to name its session, and until one does it reads nothing.
    pub fn matches_image(&self, endpoint_id: u64, stream_id: i32, session_id: i32) -> bool {
        self.endpoint_id == Some(endpoint_id)
            && self.stream_id == stream_id
            && ((self.session_id.is_none() && !self.is_response)
                || self.session_id == Some(session_id))
    }

    /// Whether this subscription reads that publication
    /// (`aeron_driver_conductor_subscription_link_matches_ipc_publication`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:110-117`).
    ///
    /// The session rule is the whole of it, and it is asymmetric: a
    /// subscription that named a session is looking for that one stream, while
    /// one that named none is looking for whatever appears — including a
    /// publication that is created after the subscription, which is the case
    /// the reference calls "wildcard".
    pub fn matches(&self, publication: &IpcPublication) -> bool {
        self.stream_id == publication.stream_id
            && ((self.session_id.is_none() && !self.is_response)
                || self.session_id == Some(publication.session_id))
    }

    /// Whether this **spy** subscription reads that network publication
    /// (`aeron_driver_conductor_spy_subscription_link_matches`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:92-108`).
    ///
    /// Two clauses, and the first is the same stream rule every other match in
    /// this module uses. What is different is what stands in for it when the
    /// stream agrees:
    ///
    /// * the two channels name the same **tag** — `tags=`, and only when both
    ///   are a real tag, because [`INVALID_TAG`] is not a value two channels
    ///   can share — **or**
    /// * the session matches and the two channels have the same canonical form,
    ///   byte for byte.
    ///
    /// The canonical form is compared rather than the channel string for the
    /// reason it exists at all: it is the two sides of the channel and nothing
    /// else — the local and remote addresses, each quoted as the parameter the
    /// URI wrote or as the address it resolved to — so two clients that named
    /// the same endpoint with different *other* parameters named the same
    /// channel (`aeron_uri_udp_canonicalise`,
    /// `media/aeron_udp_channel.c:148-208`). What it does **not** do is treat
    /// two spellings of one address as one: a channel that wrote a host name
    /// and one that wrote the address it resolves to are two canonical forms,
    /// which is why the tag branch exists at all.
    ///
    /// The tag branch is what reaches across that: a tagged channel is
    /// addressed by its tag, and two channels carrying the same one are the
    /// same channel however differently they are spelled.
    pub fn spy_matches(
        &self,
        publication_channel: &UdpChannel,
        stream_id: i32,
        session_id: i32,
    ) -> bool {
        let Some(spy_channel) = self.spy_channel.as_ref() else {
            return false;
        };

        let is_same_channel_tag =
            INVALID_TAG != spy_channel.tag_id && spy_channel.tag_id == publication_channel.tag_id;

        // The session clause is the reference's
        // `is_wildcard_or_session_id_match` (`:74-79`), the same one the image
        // rule uses — a spy link is never a response one
        // (`:4886` sets `is_response` false), so the two shapes agree.
        let session_matches =
            (self.session_id.is_none() && !self.is_response) || self.session_id == Some(session_id);

        self.stream_id == stream_id
            && (is_same_channel_tag
                || (session_matches
                    && publication_channel.canonical_form == spy_channel.canonical_form))
    }
}

/// Why an `ADD_SUBSCRIPTION` could not be served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddSubscriptionError {
    /// The channel URI or one of its parameters was refused.
    Params(PublicationParamsError),
    /// A channel this build does not serve — the same answer
    /// `ADD_PUBLICATION` gives a UDP channel, and for the same reason.
    UnsupportedTransport,
    /// The client could not be registered.
    NoClientRecord,
    /// A publication matched, but its reader position could not be created:
    /// no counter id left, or the log buffer refused it.
    Link,
    /// A UDP channel this build refused
    /// ([`crate::udp_channel::UdpChannelError`]).
    Channel(Box<crate::udp_channel::UdpChannelError>),
    /// The subscription's endpoint could not be made — a socket that will not
    /// bind, most often because it is already bound.
    Endpoint {
        /// The reference's words, which name the channel.
        message: String,
    },
    /// The receiver thread has stopped.
    Receiver,
    /// An `ADD_RCV_DESTINATION` named a subscription no network link carries
    /// (`:6053-6062`).
    UnknownSubscription,
    /// An `ADD_RCV_DESTINATION` named a subscription whose channel does not
    /// allow manual control, and so may not have sources added to it (`:5608-5611`).
    NotManualControl,
    /// A subscription named an option that disagrees with one already on the
    /// same endpoint and stream, so the two cannot share the image that would
    /// serve them (`aeron_driver_conductor_has_clashing_subscription`,
    /// `aeron_driver_conductor.c:286-366`).
    Clashing {
        /// The reference's words, which name the option, its value and both
        /// channels.
        message: String,
    },
}

impl AddSubscriptionError {
    /// The lines the reference's URI parse left behind, when this failure is
    /// one of its (`channel_uri::UriError::parse_failure`).
    pub fn uri_parse_failure(&self, channel: &[u8]) -> Option<String> {
        match self {
            Self::Params(PublicationParamsError::Uri(error)) => error.parse_failure(channel),
            Self::Channel(error) => match error.as_ref() {
                crate::udp_channel::UdpChannelError::Uri(error) => error.parse_failure(channel),
                _ => None,
            },
            _ => None,
        }
    }

    /// The `ON_ERROR` code this failure is reported under, by the same rule as
    /// [`AddError::error_code`]: a URI the driver cannot *read* is an invalid
    /// channel, a parameter *value* the reference's readers would reject is
    /// generic.
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Params(PublicationParamsError::Uri(
                UriError::InvalidScheme
                | UriError::TooLong { .. }
                | UriError::NotUtf8
                | UriError::MissingKey { .. }
                | UriError::MissingValue { .. },
            )) => deepmsg_cnc::command::ERROR_CODE_INVALID_CHANNEL,
            Self::UnsupportedTransport => deepmsg_cnc::command::ERROR_CODE_NOT_SUPPORTED,
            Self::UnknownSubscription => deepmsg_cnc::command::ERROR_CODE_UNKNOWN_SUBSCRIPTION,
            Self::NotManualControl => deepmsg_cnc::command::ERROR_CODE_INVALID_CHANNEL,
            // Not one of Aeron's codes: the reference hands `AERON_SET_ERR` the
            // platform's `EINVAL` here (`:323`), and `aeron_errcode()` returns
            // whatever it was given (`util/aeron_error.c:355`), so 22 is what
            // the client reads back. It is the one refusal in this family whose
            // code is an errno rather than an `AERON_ERROR_CODE_*`.
            Self::Clashing { .. } => ERROR_CODE_EINVAL,
            Self::Channel(error) => error.error_code(),
            Self::Params(_)
            | Self::NoClientRecord
            | Self::Link
            | Self::Endpoint { .. }
            | Self::Receiver => ERROR_CODE_GENERIC_ERROR,
        }
    }
}

impl std::fmt::Display for AddSubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Params(error) => write!(f, "{error}"),
            Self::UnsupportedTransport => {
                f.write_str("only `aeron:ipc` channels are served by this driver")
            }
            Self::NoClientRecord => f.write_str("failed to add client"),
            Self::Link => f.write_str("failed to allocate the subscriber position"),
            Self::Channel(error) => write!(f, "{error}"),
            Self::Endpoint { message } => f.write_str(message),
            Self::Receiver => f.write_str("the receiver thread has stopped"),
            Self::UnknownSubscription => f.write_str("unknown subscription"),
            Self::NotManualControl => f.write_str("channel does not allow manual control"),
            Self::Clashing { message } => f.write_str(message),
        }
    }
}

impl std::error::Error for AddSubscriptionError {}

impl From<PublicationParamsError> for AddSubscriptionError {
    fn from(error: PublicationParamsError) -> Self {
        Self::Params(error)
    }
}

impl From<crate::channel_uri::UriError> for AddSubscriptionError {
    fn from(error: crate::channel_uri::UriError) -> Self {
        Self::Params(error.into())
    }
}

impl From<AddError> for AddSubscriptionError {
    fn from(error: AddError) -> Self {
        match error {
            AddError::Params(error) => Self::Params(error),
            AddError::UnsupportedTransport => Self::UnsupportedTransport,
            AddError::NoClientRecord => Self::NoClientRecord,
            _ => Self::Link,
        }
    }
}

/// The subscriptions a driver owns.
#[derive(Debug, Default)]
pub struct IpcSubscriptions {
    links: Vec<SubscriptionLink>,
}

impl IpcSubscriptions {
    /// No subscriptions.
    pub const fn new() -> Self {
        Self { links: Vec::new() }
    }

    /// The subscriptions, in the order they were added.
    pub fn links(&self) -> &[SubscriptionLink] {
        &self.links
    }

    /// The subscription a client holds by the id it was answered with, which is
    /// what `ADD_RCV_DESTINATION` names
    /// (`aeron_driver_conductor_find_mds_subscription`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:5581-5615`).
    ///
    /// A destination is added to **a subscription**, not to a channel or an
    /// endpoint: the client holds the subscription's registration id, and the
    /// endpoint is what that subscription happens to read through
    /// (`:5879-5910`).
    ///
    /// The endpoint clause is the reference's — it walks
    /// `network_subscriptions` and nothing else — and it earns its place
    /// because a registration id can name more than one link: a spy added as a
    /// receive destination is stored under its subscription's id and has no
    /// endpoint at all. A destination is a socket or a local read *inside* a
    /// subscription that listens, so the link a destination names is always
    /// one with an endpoint.
    pub fn find_mds(&self, registration_id: i64) -> Option<&SubscriptionLink> {
        self.links
            .iter()
            .find(|link| link.registration_id == registration_id && link.endpoint_id.is_some())
    }

    /// How many publications this subscription reads.
    pub fn images(&self) -> usize {
        self.links.iter().map(|link| link.subscribables.len()).sum()
    }

    /// Serve an `ADD_SUBSCRIPTION` (`aeron_driver_conductor.c:4741-4824`).
    ///
    /// The reply goes out **before** the matching: a client is told its
    /// subscription exists first, and the images it already matches follow as
    /// separate messages. A client that got them in the other order would have
    /// images for a subscription it has not been told about yet.
    ///
    /// # Errors
    ///
    /// [`AddSubscriptionError`] for a channel the reference refuses, and for a
    /// match whose reader position could not be created — in which case the
    /// client has already been told the subscription exists, which is the
    /// reference's behaviour too.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn add_subscription(
        &mut self,
        request: &AddSubscriptionCommand<'_>,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        publications: &mut IpcPublications,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddSubscriptionError> {
        let uri = ChannelUri::parse(request.channel)?;
        if uri.transport() != Transport::Ipc {
            return Err(AddSubscriptionError::UnsupportedTransport);
        }
        let params = SubscriptionParams::resolve(&uri, config)?;

        let Some(_client) = clients.get_or_add(
            request.client_id,
            now.ms,
            now.client_liveness_timeout_ns,
            counters,
            regions,
            events,
        ) else {
            return Err(AddSubscriptionError::NoClientRecord);
        };

        let link = SubscriptionLink {
            registration_id: request.correlation_id,
            client_id: request.client_id,
            stream_id: request.stream_id,
            session_id: params.session_id,
            channel: request.channel.to_vec(),
            is_tether: params.is_tether,
            is_rejoin: params.is_rejoin,
            is_response: params.is_response,
            group: params.group,
            setup_status: SetupStatus::Pending,
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
            endpoint_id: None,
            spy_channel: None,
            subscribables: Vec::new(),
        };

        // The reply first (`:4788-4789`).
        events.subscription_ready(
            request.correlation_id,
            CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
        );

        self.links.push(link);

        // Then every publication it already matches, and — because a
        // publication created later runs the same match from its own side
        // (`link_ipc_subscriptions`) — every publication that appears after it.
        let link = self.links.last_mut().expect("just pushed");

        for publication in publications.publications_mut() {
            if !link.matches(publication)
                || !publication.is_accepting_subscriptions(counters, regions)
            {
                continue;
            }

            link_subscribable(link, publication, counters, regions, now, events)
                .map_err(|()| AddSubscriptionError::Link)?;
        }

        Ok(())
    }

    /// Serve an `ADD_SUBSCRIPTION` for a spy channel
    /// (`aeron_driver_conductor_on_add_spy_subscription`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:4926-4966`, and the
    /// executor the parse ends in, `:4827-4924`).
    ///
    /// This is the one kind of subscription that is **not a socket**. What it
    /// reads is a publication's log buffer, in this process, which the
    /// publisher is already writing into — so there is no endpoint to make, no
    /// session to elicit and no channel status to report: the reply carries
    /// [`CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`], which is the reference
    /// saying exactly that (`:4892-4894`).
    ///
    /// # Errors
    ///
    /// [`AddSubscriptionError::Channel`] for a channel that does not follow the
    /// prefix, [`AddSubscriptionError::Params`] for its parameters, and
    /// [`AddSubscriptionError::NoClientRecord`] for a client that cannot be
    /// registered.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn add_spy_subscription(
        &mut self,
        request: &AddSubscriptionCommand<'_>,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        clients: &mut Clients,
        publications: &NetworkPublications,
        sender: &SenderProxy,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddSubscriptionError> {
        // The prefix comes off first and the rest is an ordinary UDP channel
        // (`:4939-4942`): the reference parses the stripped bytes and keeps the
        // result as the link's `spy_channel`, which is what the match rule
        // compares against a publication's endpoint channel.
        let spy_channel = crate::udp_channel::resolve_spy_channel(request.channel)
            .map_err(Box::new)
            .map_err(AddSubscriptionError::Channel)?;

        let inner = &request.channel[crate::udp_channel::SPY_PREFIX.len()..];
        let uri = ChannelUri::parse(inner)?;
        let params = SubscriptionParams::resolve(&uri, config)?;

        let Some(_client) = clients.get_or_add(
            request.client_id,
            now.ms,
            now.client_liveness_timeout_ns,
            counters,
            regions,
            events,
        ) else {
            return Err(AddSubscriptionError::NoClientRecord);
        };

        let link = SubscriptionLink {
            registration_id: request.correlation_id,
            client_id: request.client_id,
            stream_id: request.stream_id,
            session_id: params.session_id,
            channel: request.channel.to_vec(),
            is_tether: params.is_tether,
            is_rejoin: params.is_rejoin,
            is_response: false,
            group: params.group,
            setup_status: SetupStatus::Pending,
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
            endpoint_id: None,
            spy_channel: Some(spy_channel),
            subscribables: Vec::new(),
        };

        // The reply first, without a channel status — a spy has no socket to
        // report one for, and `NOT_ALLOCATED` is how the client is told.
        events.subscription_ready(
            request.correlation_id,
            CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
        );

        self.links.push(link);

        // Then every publication it already matches (`:4900-4921`), and — the
        // other half of the same question — every publication that appears
        // after it ([`Self::link_spy_subscriptions`]).
        //
        // The reference asks `is_accepting_subscriptions` of each candidate
        // before linking it (`aeron_network_publication.h:290-297`), which is
        // about its three-state life: an ACTIVE publication takes readers, and
        // a DRAINING one only while it still has some and its producer is
        // ahead of its sender. A network publication here has no draining
        // state — a removal takes it out of the collection in one step — so a
        // publication that is there is one that is taking readers, and the
        // question has no third answer to give.
        let link = self.links.last_mut().expect("just pushed");

        for publication in publications.publications() {
            if !link.spy_matches(
                &publication.endpoint_channel,
                publication.stream_id,
                publication.session_id,
            ) {
                continue;
            }

            link_spy_publication(link, publication, counters, regions, sender, now, events)
                .map_err(|()| AddSubscriptionError::Link)?;
        }

        Ok(())
    }

    /// Give a network publication to every spy subscription that was waiting
    /// for it (`aeron_driver_conductor.c:4201-4226`, run from the create).
    ///
    /// The reverse of the scan [`Self::add_spy_subscription`] makes, and it has
    /// to exist for the same reason the IPC one does: a client is allowed to
    /// spy on a stream nobody publishes yet, so neither order may be the one
    /// that works.
    ///
    /// Nothing here fails the pass. The reference returns an error from its
    /// create when a link does, but the publication exists and its client has
    /// been answered either way; a spy that could not be linked is a reader
    /// that will not be told about the buffer, which is what the caller's
    /// error log is for.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn link_spy_subscriptions(
        &mut self,
        publication: &NetworkPublicationRecord,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        sender: &SenderProxy,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> usize {
        let mut failures = 0;

        for link in &mut self.links {
            if link.spy_channel.is_none()
                || !link.spy_matches(
                    &publication.endpoint_channel,
                    publication.stream_id,
                    publication.session_id,
                )
                || link.reads(publication.registration_id)
            {
                continue;
            }

            if link_spy_publication(link, publication, counters, regions, sender, now, events)
                .is_err()
            {
                failures += 1;
            }
        }

        failures
    }

    /// Serve an `ADD_RCV_DESTINATION` whose channel is a spy
    /// (`aeron_driver_conductor_execute_add_receive_spy_destination`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:5704-5806`).
    ///
    /// This is the third way a spy link is made, and it is a *destination*
    /// rather than a subscription: a client takes a channel it already
    /// subscribes to with `control-mode=manual` — a multi-destination
    /// subscription, which is the only kind that may have sources added to it —
    /// and names a spy as one of its sources. What comes back is an
    /// acknowledgement of the destination, and what the subscription then gets
    /// is an image for every publication the spy names, exactly as if it had
    /// been spied from the start.
    ///
    /// The link is stored under the **subscription's** registration id, not the
    /// destination's (`:5763`), which is what makes the pair one thing: a
    /// `REMOVE_SUBSCRIPTION` for that id takes the spied images with it, and
    /// the destination is removed by naming the channel it was added with.
    ///
    /// # Errors
    ///
    /// [`AddSubscriptionError::UnknownSubscription`] for a registration id no
    /// network subscription carries,
    /// [`AddSubscriptionError::NotManualControl`] for one whose channel may not
    /// have sources added to it, and the channel's own errors for the URI.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn add_spy_destination(
        &mut self,
        request: &DestinationCommandReceived<'_>,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        endpoints: &ReceiveChannelEndpoints,
        publications: &NetworkPublications,
        sender: &SenderProxy,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddSubscriptionError> {
        let spy_channel = crate::udp_channel::resolve_spy_channel(request.channel)
            .map_err(Box::new)
            .map_err(AddSubscriptionError::Channel)?;

        let inner = &request.channel[crate::udp_channel::SPY_PREFIX.len()..];
        let uri = ChannelUri::parse(inner)?;
        let params = SubscriptionParams::resolve(&uri, config)?;

        // The subscription the destination is added to, and the one thing it
        // has to be: a network subscription on a channel that allows manual
        // control (`aeron_driver_conductor_find_mds_subscription`, `:5990-6021`).
        let Some(mds) = self.find_mds(request.registration_id) else {
            return Err(AddSubscriptionError::UnknownSubscription);
        };

        let Some(endpoint_id) = mds.endpoint_id else {
            return Err(AddSubscriptionError::UnknownSubscription);
        };

        let is_manual = endpoints
            .get(endpoint_id)
            .is_some_and(|entry| entry.channel.control_mode == ControlMode::Manual);

        if !is_manual {
            return Err(AddSubscriptionError::NotManualControl);
        }

        // The subscription's stream, the destination's session — the reference
        // reads each off the thing that owns it (`:5759-5763`), and the channel
        // it records is the **destination's**, which is what the reader's
        // counter is labelled with and what a removal names.
        let link = SubscriptionLink {
            registration_id: mds.registration_id,
            client_id: request.client_id,
            stream_id: mds.stream_id,
            session_id: params.session_id,
            channel: request.channel.to_vec(),
            is_tether: params.is_tether,
            is_rejoin: params.is_rejoin,
            is_response: false,
            group: params.group,
            setup_status: SetupStatus::Pending,
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
            endpoint_id: None,
            spy_channel: Some(spy_channel),
            subscribables: Vec::new(),
        };

        // An acknowledgement, not a subscription ready: the client asked for a
        // destination and that is what it is told about (`:5774`).
        events.operation_succeeded(request.correlation_id);

        self.links.push(link);

        let link = self.links.last_mut().expect("just pushed");

        for publication in publications.publications() {
            if !link.spy_matches(
                &publication.endpoint_channel,
                publication.stream_id,
                publication.session_id,
            ) {
                continue;
            }

            link_spy_publication(link, publication, counters, regions, sender, now, events)
                .map_err(|()| AddSubscriptionError::Link)?;
        }

        Ok(())
    }

    /// Serve an `ADD_RCV_DESTINATION` whose channel is `aeron:ipc`
    /// (`aeron_driver_conductor_on_add_receive_ipc_destination`, `:5617-5700`).
    ///
    /// An IPC destination is a **source** rather than a socket, like a spy's,
    /// and the difference between the two is where the bytes come from: a spy
    /// reads a network publication's log buffer, and this reads an IPC one. It
    /// is what lets a multi-destination subscription — whose own channel is a
    /// network one — be fed by a publisher in this process, and it is the
    /// reference's one way to mix IPC and UDP publishers on one stream.
    ///
    /// The link is built the same way the spy link is and the reference builds
    /// both in one shape (`:5646-5665` against `:5808-5870`): the channel is
    /// the **destination's**, the stream is the **subscription's**, the session
    /// and the options are the destination URI's, and the registration id is
    /// the subscription's — because a destination is not a subscription and
    /// must not be one to a client's `removeSubscription`. Three fields are the
    /// IPC ones: no endpoint and no spy channel, so the link is local, and
    /// `is_reliable` is written **true** rather than taken from the URI: there
    /// is no datagram to lose.
    ///
    /// # Errors
    ///
    /// [`AddSubscriptionError::UnknownSubscription`] for a registration id no
    /// network subscription carries, [`AddSubscriptionError::NotManualControl`]
    /// for one whose channel may not have sources added to it, and the
    /// channel's own errors for the URI.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn add_ipc_destination(
        &mut self,
        request: &DestinationCommandReceived<'_>,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        endpoints: &ReceiveChannelEndpoints,
        publications: &mut IpcPublications,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddSubscriptionError> {
        let uri = ChannelUri::parse(request.channel)?;
        let params = SubscriptionParams::resolve(&uri, config)?;

        // The subscription the destination is added to, and the one thing it
        // has to be: a network subscription whose channel allows manual control
        // (`aeron_driver_conductor_find_mds_subscription`, `:5581-5599`, which
        // is the same lookup the spy path makes).
        let Some(mds) = self.find_mds(request.registration_id) else {
            return Err(AddSubscriptionError::UnknownSubscription);
        };

        let Some(endpoint_id) = mds.endpoint_id else {
            return Err(AddSubscriptionError::UnknownSubscription);
        };

        if !endpoints
            .get(endpoint_id)
            .is_some_and(|entry| entry.channel.control_mode == ControlMode::Manual)
        {
            return Err(AddSubscriptionError::NotManualControl);
        }

        let link = SubscriptionLink {
            registration_id: mds.registration_id,
            client_id: request.client_id,
            stream_id: mds.stream_id,
            session_id: params.session_id,
            channel: request.channel.to_vec(),
            is_tether: params.is_tether,
            is_rejoin: params.is_rejoin,
            is_response: false,
            // `AERON_INFER`, written where the spy path copies the URI's: a
            // local read has no group to infer from.
            group: InferableBoolean::Infer,
            setup_status: SetupStatus::Pending,
            is_reliable: true,
            is_sparse: params.is_sparse,
            endpoint_id: None,
            spy_channel: None,
            subscribables: Vec::new(),
        };

        // An acknowledgement, not a subscription ready (`:5668`), and it goes
        // out **before** the publications are read: what follows is the images
        // the client is told about, as separate messages.
        events.operation_succeeded(request.correlation_id);

        self.links.push(link);

        let link = self.links.last_mut().expect("just pushed");

        for publication in publications.publications_mut() {
            if !link.matches(publication)
                || !publication.is_accepting_subscriptions(counters, regions)
            {
                continue;
            }

            // The reference stops at the first failure rather than carrying on
            // (`:5682-5686` goes to its cleanup), and the link it has already
            // added stays where it is.
            link_subscribable(link, publication, counters, regions, now, events)
                .map_err(|()| AddSubscriptionError::Link)?;
        }

        Ok(())
    }

    /// Serve a `REMOVE_RCV_DESTINATION` whose channel is `aeron:ipc`
    /// (`aeron_driver_conductor_on_remove_receive_ipc_destination`,
    /// `:5984-6018`).
    ///
    /// The link is found by its registration id **and** the channel it was
    /// added with — an id alone would name either of two destinations on one
    /// subscription. Its readers are told which images are going
    /// (`subscription_link_notify_unavailable_images`), then let go, in that
    /// order: a client that hears about an image after the position it reads
    /// has been freed is reading a counter id that may already be someone
    /// else's.
    ///
    /// # Returns
    ///
    /// `false` when no such link is there, which the reference answers with an
    /// error naming the subscription.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn remove_ipc_destination(
        &mut self,
        registration_id: i64,
        channel: &[u8],
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut IpcPublications,
        now_ms: i64,
        events: &mut impl ClientEvents,
    ) -> bool {
        let Some(index) = self.links.iter().position(|link| {
            link.registration_id == registration_id
                && link.endpoint_id.is_none()
                && link.spy_channel.is_none()
                && link.channel == channel
        }) else {
            return false;
        };

        let link = self.links.swap_remove(index);

        for entry in &link.subscribables {
            events.unavailable_image(
                entry.target.registration_id(),
                link.registration_id,
                link.stream_id,
                &link.channel,
            );
        }

        // No receiver and no sender: an IPC link's readers are the
        // publication's own, and `unlink_all` finds it by the registration id
        // in the target (`aeron_driver_conductor_unlink_all_subscribable`,
        // `:3620-3691`).
        unlink_all(link, counters, regions, publications, None, None, now_ms);

        true
    }

    /// Serve a `REMOVE_RCV_DESTINATION` whose channel is a spy
    /// (`aeron_driver_conductor_on_remove_receive_spy_destination`, `:6024-6065`).
    ///
    /// The link is found by its registration id **and** the channel it was
    /// added with, which is what tells two spy destinations on one subscription
    /// apart — an id alone would name either.
    ///
    /// Every image it was holding is announced as unavailable before the link
    /// goes, which is the one place a spy removal differs from a subscription
    /// removal: a subscription removal is silent in C and in Java alike, while
    /// this path is the client taking one source out of several and being told
    /// which images that source was.
    ///
    /// # Returns
    ///
    /// `false` when no such link is there, which the reference answers with an
    /// error naming the subscription.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn remove_spy_destination(
        &mut self,
        registration_id: i64,
        channel: &[u8],
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        sender: &SenderProxy,
        now_ms: i64,
        events: &mut impl ClientEvents,
    ) -> bool {
        let Some(index) = self.links.iter().position(|link| {
            link.registration_id == registration_id
                && link.spy_channel.is_some()
                && link.channel == channel
        }) else {
            return false;
        };

        let link = self.links.swap_remove(index);

        for entry in &link.subscribables {
            // The publication is told before the counter goes back, for the
            // same reason the message is sent before it: a set holding a freed
            // counter id reads whatever takes its place.
            if let SubscriptionTarget::NetworkPublication(publication) = entry.target {
                let _ = sender.remove_subscriber(publication, entry.counter_id);
            }

            events.unavailable_image(
                entry.target.registration_id(),
                link.registration_id,
                link.stream_id,
                &link.channel,
            );
            counters.free(regions, entry.counter_id, now_ms);
        }

        true
    }

    /// Give a publication to every subscription that was waiting for it
    /// (`aeron_driver_conductor_link_ipc_subscriptions`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:3693-3727`).
    ///
    /// This is the other half of the match: a subscription that arrives before
    /// its publisher is linked from [`Self::add_subscription`], and a
    /// publication that arrives after its subscribers is linked from here. Both
    /// sides ask the same question, which is what makes the order they happen
    /// in not matter — and it must not, because a client is allowed to
    /// subscribe to a stream nobody publishes yet.
    pub fn link_publication(
        &mut self,
        publication: &mut IpcPublication,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) {
        for link in &mut self.links {
            if !link.matches(publication)
                || link.reads(publication.registration_id)
                || !publication.is_accepting_subscriptions(counters, regions)
            {
                continue;
            }

            // A failure here is a counter or a log buffer that would not take
            // the reader; the publication exists and its client has been told,
            // so the pass carries on — which is what the reference does with
            // the error it gets back from its own link (`:3763-3766` appends it
            // and keeps the publication).
            let _ = link_subscribable(link, publication, counters, regions, now, events);
        }
    }

    /// Point a response **subscription** at the session of the response
    /// publication that has just been created for it
    /// (`aeron_driver_conductor_find_and_update_ipc_response_subscription`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:1711-1760`, reached
    /// from `execute_add_ipc_publication_create_publication`, `:3895`).
    ///
    /// The chain is three links long and each is a registration id, which is
    /// why none of it crosses the wire: the response publication's
    /// `response-correlation-id` names the **request** publication, and that
    /// publication's own `response-correlation-id` names the **response
    /// subscription**. Anything the chain does not reach is not an error — a
    /// publication whose correlation id names nothing is an ordinary one, and
    /// so is a response publication whose subscription has since gone.
    ///
    /// Why it matters: a response subscription that named no session is
    /// deliberately not a wildcard ([`SubscriptionLink::matches`] — it is
    /// waiting to be told which stream it is), so without this it reads
    /// nothing, ever. And one that *named* a session has already chosen: a
    /// publication that would answer it under another is refused, with the
    /// reference's words, rather than being told apart later on a channel the
    /// subscriber never opened.
    ///
    /// # Errors
    ///
    /// The reference's message for the mismatch, which a client reads as a
    /// `RegistrationException`.
    pub fn attach_response_publication(
        &mut self,
        publications: &[IpcPublication],
        response_correlation_id: i64,
        response_registration_id: i64,
        response_channel: &[u8],
        response_session_id: i32,
    ) -> Result<(), String> {
        let Some(request) = publications
            .iter()
            .find(|publication| publication.registration_id == response_correlation_id)
        else {
            return Ok(());
        };

        let Some(link) = self
            .links
            .iter_mut()
            .find(|link| link.registration_id == request.response_correlation_id)
        else {
            return Ok(());
        };

        if let Some(named) = link.session_id {
            if named != response_session_id {
                return Err(format!(
                    "failed to create response publication (registrationId={response_registration_id}, \
                     channel={}), because response subscription (registrationId={}, channel={}) \
                     uses `session-id` parameter that does not match `session-id={response_session_id}` \
                     of the response publication",
                    String::from_utf8_lossy(response_channel),
                    link.registration_id,
                    String::from_utf8_lossy(&link.channel),
                ));
            }
        }

        link.session_id = Some(response_session_id);

        Ok(())
    }

    /// The subscriptions reading a publication, for the caller that has to tell
    /// them it is going away (`aeron_driver_conductor_unlink_ipc_subscriptions`,
    /// `:6453-6475`, which sends one message per reader).
    pub fn readers_of(&self, publication_registration_id: i64) -> Vec<&SubscriptionLink> {
        self.links
            .iter()
            .filter(|link| link.reads(publication_registration_id))
            .collect()
    }

    /// A network publication is going away: tell every spy that reads it, and
    /// give up their readers
    /// (`aeron_driver_conductor_cleanup_spies`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:1502-1519`).
    ///
    /// The reference splits this in three. It sends the message here, drops the
    /// entries from each link in `aeron_network_publication_entry_delete`
    /// (`:1481-1489`), and frees the counters in the publication's own close —
    /// which walks the subscribable set it is about to destroy
    /// (`aeron_network_publication.c:340-343`).
    ///
    /// This build's spy link keeps no such set: a spy's counter is the whole of
    /// what the conductor linked, so all three happen here. What a client sees
    /// is the same either way — one `ON_UNAVAILABLE_IMAGE` per spy, naming the
    /// channel it spied with, before anything is freed.
    ///
    /// # Returns
    ///
    /// How many spies were told.
    pub fn unlink_spies_of(
        &mut self,
        publication_registration_id: i64,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
        events: &mut impl ClientEvents,
    ) -> usize {
        let mut told = 0;

        for link in &mut self.links {
            let mut reading = false;

            link.subscribables.retain(|entry| {
                let is_this_publication = matches!(
                    entry.target,
                    SubscriptionTarget::NetworkPublication(registration_id)
                        if registration_id == publication_registration_id
                );

                if is_this_publication {
                    reading = true;
                    counters.free(regions, entry.counter_id, now_ms);
                }

                !is_this_publication
            });

            if reading {
                events.unavailable_image(
                    publication_registration_id,
                    link.registration_id,
                    link.stream_id,
                    &link.channel,
                );
                told += 1;
            }
        }

        told
    }

    /// Forget a publication that no longer exists: drop the entries that point
    /// at it, without freeing anything — its own close owns the counters
    /// (`aeron_driver_conductor_unlink_subscribable`, `:3662-3677`).
    pub fn forget_publication(&mut self, publication_registration_id: i64) {
        for link in &mut self.links {
            link.subscribables
                .retain(|entry| entry.target.registration_id() != publication_registration_id);
        }
    }

    /// A publication is being rejected: tell every subscription reading it
    /// that the image is gone, and drop those readers from the subscriptions
    /// (`aeron_driver_conductor_unlink_ipc_subscriptions`,
    /// `aeron_driver_conductor.c:6453-6473`).
    ///
    /// Nothing is freed here. The counter a reader was given belongs to the
    /// publication's own set, and that set is what gives it back a step later
    /// (`aeron_ipc_publication.c:256-271`: `unlink_subscribable` drops the
    /// link's entries, then the subscribable's array is freed) — freeing it
    /// here as well would give the same counter back twice.
    ///
    /// The message names what the **link** holds — its own stream id and its
    /// own channel — where the revoke path names the publication's stream and
    /// the constant `aeron:ipc`. That asymmetry is the reference's, and the two
    /// are thirty lines apart in the conductor (`:6462-6468` against
    /// `:503-517`).
    ///
    /// # Returns
    ///
    /// How many subscriptions were told.
    pub fn unlink_publication(
        &mut self,
        publication_registration_id: i64,
        events: &mut impl ClientEvents,
    ) -> usize {
        let mut told = 0;

        for link in &mut self.links {
            let before = link.subscribables.len();

            link.subscribables.retain(|entry| {
                !matches!(
                    entry.target,
                    SubscriptionTarget::IpcPublication(registration_id)
                        if registration_id == publication_registration_id
                )
            });

            if link.subscribables.len() != before {
                events.unavailable_image(
                    publication_registration_id,
                    link.registration_id,
                    link.stream_id,
                    &link.channel,
                );
                told += 1;
            }
        }

        told
    }

    /// Whether a **network** subscription with this registration id exists
    /// (`aeron_driver_conductor.c:641-648`, which walks
    /// `network_subscriptions`).
    ///
    /// The distinction is the reference's and it matters here: a publication
    /// naming a `response-correlation-id` is naming a subscription a responder
    /// will send to over UDP, and an IPC subscription has no endpoint to send
    /// anything to. A link with no endpoint is an IPC one.
    pub fn has_network(&self, registration_id: i64) -> bool {
        self.links
            .iter()
            .any(|link| link.endpoint_id.is_some() && link.registration_id == registration_id)
    }

    /// Whether a subscription with this registration id exists, which is what
    /// decides between an acknowledgement and an error before the removal
    /// itself happens (`aeron_driver_conductor.c:5203-5266`).
    pub fn has(&self, registration_id: i64) -> bool {
        self.links
            .iter()
            .any(|link| link.registration_id == registration_id)
    }

    /// Remove a subscription (`aeron_driver_conductor_on_remove_subscription`,
    /// `:5199-5267`).
    ///
    /// Every reader position is detached from its publication and its counter
    /// given back, and the link is dropped — with **no image message**: the
    /// reference's removal path only unlinks, in C and in Java alike, so the
    /// images a subscription held go unannounced and the acknowledgement is
    /// the whole of what the client hears. Returns whether one was found —
    /// the reference's `is_any_subscription_found`, which decides between an
    /// acknowledgement and an error.
    ///
    /// The match is on the registration id **alone**: the reference does not
    /// check the client id here, so a client that knows a subscription's
    /// registration id can remove it even though it does not own it. That is
    /// reproduced rather than tightened — a driver that refused would be one
    /// where a legitimate removal failed.
    ///
    /// **Every** link carrying that id goes, not the first one found. The
    /// reference walks all three of its arrays and removes each match
    /// (`:5204-5251`), and a registration id can name more than one link: a spy
    /// added as a receive destination is stored under the subscription's id
    /// ([`Self::add_spy_destination`]), and removing the subscription has to
    /// take it too — otherwise the subscription is gone and something is still
    /// reading a publication on its behalf.
    #[allow(clippy::too_many_arguments)] // the collaborators a removal needs
    pub fn remove(
        &mut self,
        registration_id: i64,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut IpcPublications,
        endpoints: &mut crate::receive_endpoints::ReceiveChannelEndpoints,
        receiver: Option<&ReceiverProxy>,
        sender: &SenderProxy,
        now_ms: i64,
    ) -> bool {
        let mut found = false;
        let mut index = self.links.len();

        while index > 0 {
            index -= 1;

            if self.links[index].registration_id != registration_id {
                continue;
            }

            let link = self.links.swap_remove(index);
            unlink_from_endpoint(&link, counters, regions, endpoints, receiver);
            unlink_all(
                link,
                counters,
                regions,
                publications,
                receiver,
                Some(sender),
                now_ms,
            );
            found = true;
        }

        found
    }

    /// Give up every subscription a client owned, without telling it anything:
    /// it is gone, and a message to a client that is not there is a message
    /// nobody reads (`aeron_client_delete`, `:1234-1250`).
    #[allow(clippy::too_many_arguments)] // the collaborators a removal needs
    pub fn remove_for_client(
        &mut self,
        client_id: i64,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut IpcPublications,
        endpoints: &mut crate::receive_endpoints::ReceiveChannelEndpoints,
        receiver: Option<&ReceiverProxy>,
        sender: &SenderProxy,
        now_ms: i64,
    ) -> usize {
        let mut removed = 0;
        let mut index = self.links.len();

        while index > 0 {
            index -= 1;

            if self.links[index].client_id != client_id {
                continue;
            }

            let link = self.links.swap_remove(index);
            // `aeron_client_delete` takes the same two steps in the same order
            // for a client's network subscriptions (`:1246-1258`).
            unlink_from_endpoint(&link, counters, regions, endpoints, receiver);
            unlink_all(
                link,
                counters,
                regions,
                publications,
                receiver,
                Some(sender),
                now_ms,
            );
            removed += 1;
        }

        removed
    }

    /// Give up every subscription.
    ///
    /// No counters are freed here: a reader's `sub-pos` belongs to the
    /// *publication's* set, and the publication's close is what frees it
    /// (`aeron_ipc_publication_close`, `aeron_ipc_publication.c:196-214`).
    /// Freeing them in both places would hand the same counter back twice, and
    /// the ordering here is the reference's — publications first.
    pub fn close(&mut self) {
        self.links.clear();
    }
}

impl IpcSubscriptions {
    /// A responder answered a publication that asked for a response channel
    /// (`aeron_driver_conductor_on_response_setup`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:7060-7115`).
    ///
    /// This is where a response subscription stops being one. Until a
    /// RSP_SETUP arrives it reads nothing — it is not registered with the
    /// receiver at all — and what the frame carries is the **session** it
    /// should read, which is the one thing it could not know. So the
    /// subscription is given that session, told it is no longer a response
    /// channel, and registered like any other reader; from here on the
    /// difference is gone.
    ///
    /// # Returns
    ///
    /// The error to record, when the subscription named a session that the
    /// response publication contradicts. Nothing else here is an error: a
    /// correlation id no subscription carries is a frame for another driver's
    /// client, and the reference walks past it.
    pub fn on_response_setup(
        &mut self,
        response_correlation_id: i64,
        response_session_id: i32,
        receiver: &ReceiverProxy,
    ) -> Option<(i32, String)> {
        for index in 0..self.links.len() {
            let link = &mut self.links[index];

            if link.registration_id != response_correlation_id
                || link.setup_status == SetupStatus::Error
            {
                continue;
            }

            if link.setup_status == SetupStatus::Complete {
                // A second response setup for a subscription that is already
                // reading. The answer is to ask the far end to describe itself
                // again (`aeron_driver_receiver.c:412-427`), which is what
                // re-opens a publication that has met a receiver before
                // (`aeron_network_publication.c:586-589`).
                if let (Some(endpoint_id), Some(session_id)) = (link.endpoint_id, link.session_id) {
                    let _ = receiver.request_setup(endpoint_id, link.stream_id, session_id);
                }

                continue;
            }

            if let Some(named) = link.session_id {
                if named != response_session_id {
                    // The subscription said which session it would read and
                    // the response publication says otherwise. The reference
                    // drops the named session as well as poisoning the link,
                    // and **returns** rather than continuing — so a later
                    // link with the same correlation id is not examined
                    // (`:7094-7098`).
                    link.session_id = None;
                    link.setup_status = SetupStatus::Error;

                    return Some((
                        deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
                        format!(
                            "failed to setup response subscription (registrationId={}, channel={}), \
                             because it contains `session-id` parameter that does not match \
                             `session-id={}` of the response publication",
                            link.registration_id,
                            String::from_utf8_lossy(&link.channel),
                            response_session_id
                        ),
                    ));
                }
            }

            link.session_id = Some(response_session_id);
            link.is_response = false;
            link.setup_status = SetupStatus::Complete;

            if let Some(endpoint_id) = link.endpoint_id {
                // The same call the subscription's own creation makes, and the
                // one it deliberately did not make then. The reference pairs it
                // with `decref_to_response_stream`; this build keeps no
                // response refcount to give back (see `add_network_subscription`).
                let _ = receiver.add_subscription(
                    endpoint_id,
                    link.stream_id,
                    Some(response_session_id),
                );
            }
        }

        None
    }

    /// Refuse a subscription whose `reliable`, `rejoin` or `is_response`
    /// disagrees with one already reading the same endpoint and stream
    /// (`aeron_driver_conductor_has_clashing_subscription`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:286-366`; the three
    /// arms are `:320-332`, `:334-346` and `:348-360`).
    ///
    /// Two subscriptions that one image would serve have to agree about the
    /// options that decide how it behaves. Without this the second is served by
    /// the first one's image and reads a stream whose rules it did not choose —
    /// a client that asked for `reliable=false` silently getting retransmissions,
    /// or one that asked for a response channel reading an ordinary one — with
    /// nothing raised and no counter moved.
    ///
    /// The three are checked **in the reference's order and only the first
    /// disagreement is reported**: it `return true`s out of the loop, so a
    /// subscription that differs in two ways hears about `reliable=` and not
    /// about the other. `isResponse` is last, and the test that needed it is
    /// `io.aeron.ResponseChannelsTest::shouldRejectSubscriptionIfResponseConfigurationDoesNotMatch`.
    ///
    /// The match is the reference's own: same endpoint, same stream, and the
    /// same session *including* both being wildcards — a subscription that named
    /// a session and one that did not are two subscriptions the reference does
    /// not call clashing, whatever image they later end up sharing
    /// (`aeron_driver_conductor_network_subscription_link_matches`, `:62-70`).
    ///
    /// One thing the reference checks before this loop is **not** here:
    /// `aeron_driver_conductor_receive_endpoint_has_clashing_timestamp_offsets`
    /// (`:299`), which is about the timestamp offsets this build refuses at the
    /// channel instead (`docs/compat.md`, "ATS and the timestamp-offset
    /// parameters are refused").
    fn refuse_a_clashing_options(
        &self,
        endpoint_id: u64,
        stream_id: i32,
        session_id: Option<i32>,
        params: &crate::publication_params::SubscriptionParams,
        channel: &[u8],
    ) -> Result<(), AddSubscriptionError> {
        let Some(existing) = self.links.iter().find(|link| {
            link.endpoint_id == Some(endpoint_id)
                && link.stream_id == stream_id
                && link.session_id == session_id
        }) else {
            return Ok(());
        };

        for (name, requested, held) in [
            ("reliable", params.is_reliable, existing.is_reliable),
            ("rejoin", params.is_rejoin, existing.is_rejoin),
            ("isResponse", params.is_response, existing.is_response),
        ] {
            if requested != held {
                return Err(AddSubscriptionError::Clashing {
                    message: format!(
                        "option conflicts with existing subscription: {name}={requested} \
                         existingChannel={} channel={}",
                        String::from_utf8_lossy(&existing.channel),
                        String::from_utf8_lossy(channel),
                    ),
                });
            }
        }

        Ok(())
    }

    /// Serve an `ADD_SUBSCRIPTION` for a UDP channel
    /// (`aeron_driver_conductor_on_add_network_subscription`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:5156-5280`).
    ///
    /// The reply goes out before the matching, as it does for IPC and for the
    /// same reason; what is different is that the subscription's endpoint has
    /// to *exist* first, because a subscription that reads the network is a
    /// socket bound to the endpoint parameter, and a client told its
    /// subscription is ready should be able to receive.
    ///
    /// # Errors
    ///
    /// [`AddSubscriptionError`] for a channel the reference refuses or a socket
    /// that cannot be bound.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn add_network_subscription(
        &mut self,
        request: &AddSubscriptionCommand<'_>,
        config: &DriverConfig,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        channel: UdpChannel,
        clients: &mut Clients,
        endpoints: &mut ReceiveChannelEndpoints,
        images: &mut PublicationImages,
        receiver: &ReceiverProxy,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), AddSubscriptionError> {
        let uri = ChannelUri::parse(request.channel)?;
        if uri.transport() != Transport::Udp {
            return Err(AddSubscriptionError::UnsupportedTransport);
        }

        let params = SubscriptionParams::resolve(&uri, config)?;

        validate_for_subscription(&channel)?;

        // Before the endpoint is made, as the reference checks it before it
        // makes one (`aeron_driver_conductor.c:5035-5041`): a subscription whose
        // options disagree with one already reading that endpoint and stream
        // cannot be served, because the image they would share can only behave
        // one way.
        if let Some(existing) = endpoints.find(&channel) {
            self.refuse_a_clashing_options(
                existing,
                request.stream_id,
                params.session_id,
                &params,
                request.channel,
            )?;
        }

        let Some(_client) = clients.get_or_add(
            request.client_id,
            now.ms,
            now.client_liveness_timeout_ns,
            counters,
            regions,
            events,
        ) else {
            return Err(AddSubscriptionError::NoClientRecord);
        };

        // The endpoint first: a subscription is a socket, and a channel whose
        // socket cannot be bound is a subscription that cannot be served —
        // which is a different answer from one that has nothing to read yet.
        let endpoint_params = ReceiveChannelEndpoints::transport_params(config, &channel);
        // Read before the channel moves into the endpoint: the mode decides
        // what the receiver is told below, and the endpoint takes ownership.
        let is_response_channel = channel.control_mode == ControlMode::Response;

        // `params->initial_window_length`, read before the channel moves into
        // the endpoint: the endpoint's create compares the arriving channel's
        // receive buffer against it (`aeron_driver_conductor.c:2116-2130`).
        let initial_window_length =
            ReceiveChannelEndpoints::initial_window_length(config, &channel);

        let (endpoint_id, channel_status_counter_id, new_endpoint) = endpoints
            .get_or_add(
                channel,
                &endpoint_params,
                config,
                initial_window_length,
                counters,
                regions,
                request.correlation_id,
                now.ms,
                now.ns,
            )
            .map_err(|error| AddSubscriptionError::Endpoint {
                message: match error {
                    ReceiveEndpointErrorKind::Bind(mut report) => {
                        // `:5043`: the last `AERON_APPEND_ERR` on the way up,
                        // and the one with an **empty message** — the site
                        // line alone says "this is where the subscription was
                        // being added", and the reference writes it that way.
                        report.append(
                            "aeron_driver_conductor_execute_add_network_subscription",
                            "aeron_driver_conductor.c",
                            5043,
                            "",
                        );
                        report.into_text()
                    }
                    other => other.to_string(),
                },
            })?;

        if let Some(endpoint) = new_endpoint {
            receiver
                .add_endpoint(endpoint_id, endpoint)
                .map_err(|_| AddSubscriptionError::Receiver)?;
        }

        // `aeron_driver_conductor.c:5072-5092`: a response subscription is
        // **not** registered with the receiver — the reference calls
        // `incref_to_response_stream` where every other mode calls
        // `add_network_subscription_to_receiver`. There is no session to read
        // until a RSP_SETUP names one, and registering one would have the
        // receiver eliciting a setup for a session the far end has not
        // published yet.
        //
        // The reference's response refcount (`response_stream_id_to_refcnt_map`,
        // `aeron_receive_channel_endpoint.c:746-779`) is what holds the
        // endpoint open in the meantime; this build keeps no such count because
        // it releases no endpoint when its last subscription leaves
        // (`receive_endpoints.rs::detach_subscription` has no caller), and a
        // count nothing consults is not a fact about the wire. It arrives with
        // the endpoint lifecycle.
        if !is_response_channel {
            receiver
                .add_subscription(endpoint_id, request.stream_id, params.session_id)
                .map_err(|_| AddSubscriptionError::Receiver)?;
        }

        endpoints.attach_subscription(endpoint_id);

        let link = SubscriptionLink {
            registration_id: request.correlation_id,
            client_id: request.client_id,
            stream_id: request.stream_id,
            session_id: params.session_id,
            channel: request.channel.to_vec(),
            is_tether: params.is_tether,
            is_rejoin: params.is_rejoin,
            is_response: params.is_response,
            group: params.group,
            setup_status: SetupStatus::Pending,
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
            endpoint_id: Some(endpoint_id),
            spy_channel: None,
            subscribables: Vec::new(),
        };

        // The reply first, with the endpoint's channel status: a client that
        // reads it learns whether the socket is up, which for a subscriber is
        // the only thing that can be known before a publisher exists.
        events.subscription_ready(request.correlation_id, channel_status_counter_id);

        self.links.push(link);

        // Then every image that already matches (`aeron_driver_conductor.c:5121-5143`,
        // whose guard is the link's own endpoint as much as its stream and
        // session).
        let index = self.links.len() - 1;
        let matching = images.matching(&self.links[index]);

        for image_registration_id in matching {
            let _ = self.link_image(
                index,
                image_registration_id,
                images,
                counters,
                regions,
                receiver,
                now,
                events,
            );
        }

        Ok(())
    }

    /// Give a subscription a reader position in an image, and tell its client
    /// where the log buffer is
    /// (`aeron_driver_conductor_link_subscribable`'s image case, `:3547-3617`
    /// through `:5137`).
    ///
    /// The order is the same one IPC's link uses, and the same steps: allocate
    /// the counter with the *join* position in its label, add the position to
    /// the image's set — which is what a reader *is* — then seed the counter
    /// and only then answer.
    ///
    /// # Errors
    ///
    /// A failure when the counter or the position cannot be created; the
    /// subscription exists and its client has been told, so a failure here is
    /// an image the client will never read rather than a failed command.
    #[allow(clippy::too_many_arguments)] // one per collaborator
    #[allow(clippy::result_unit_err)] // the caller has nothing to do with the reason
    pub fn link_image(
        &mut self,
        link_index: usize,
        image_registration_id: i64,
        images: &mut PublicationImages,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        receiver: &ReceiverProxy,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), ()> {
        let Some(image) = images.find(image_registration_id).cloned() else {
            // The caller has just created it, so this is the collection
            // disagreeing with itself rather than a client's mistake.
            return Err(());
        };

        let Some(link) = self.links.get_mut(link_index) else {
            return Err(());
        };

        if link.reads_image(image_registration_id) {
            return Err(());
        }

        let joining_position = images.join_position(image_registration_id, counters, regions);

        let Some(counter_id) = crate::position::allocate_subscription_position(
            counters,
            regions,
            link.client_id,
            link.registration_id,
            image.session_id,
            image.stream_id,
            &link.channel,
            joining_position,
            now.ms,
        ) else {
            return Err(());
        };

        if counters
            .set_reference_id(regions, counter_id, image_registration_id)
            .is_none()
        {
            counters.free(regions, counter_id, now.ms);
            return Err(());
        }

        let position = TetherablePosition {
            counter_id,
            subscription_registration_id: link.registration_id,
            time_of_last_update_ns: now.ns,
            state: TetherState::Active,
            is_tether: link.is_tether,
            is_rejoin: link.is_rejoin,
        };

        if receiver
            .add_subscriber(image_registration_id, position)
            .is_err()
        {
            counters.free(regions, counter_id, now.ms);
            return Err(());
        }

        link.subscribables.push(SubscriptionLinkEntry {
            target: SubscriptionTarget::Image(image_registration_id),
            counter_id,
        });

        // The counter is seeded **after** the reader is in the set, which is
        // the reference's order: a reader that appears with a position of zero
        // while the image is a term ahead would look like one that had read
        // nothing, and the window would be computed from that.
        let _ = counters.set_value(regions, counter_id, joining_position);

        let correlation_id = link.registration_id;
        images.incref(image_registration_id);

        events.available_image(&ImageBuffersReady {
            correlation_id: image.registration_id,
            session_id: image.session_id,
            stream_id: image.stream_id,
            subscriber_registration_id: correlation_id,
            subscriber_position_id: counter_id,
            log_file: image.path.as_os_str().as_encoded_bytes(),
            source_identity: image.source_identity.as_bytes(),
        });

        Ok(())
    }

    /// Give every waiting subscription the image that has just appeared
    /// (`link_matching_subscriptions`, run when an image is created).
    ///
    /// # Errors
    ///
    /// When a link failed. The caller records it: a subscription that was not
    /// told about an image waits for ever otherwise.
    #[allow(clippy::result_unit_err)] // the caller has nothing to do with the reason
    #[allow(clippy::too_many_arguments)] // one per collaborator
    pub fn link_new_image(
        &mut self,
        image_registration_id: i64,
        images: &mut PublicationImages,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        receiver: &ReceiverProxy,
        now: crate::ipc_publications::Now,
        events: &mut impl ClientEvents,
    ) -> Result<(), ()> {
        let mut failures = 0;
        let mut linked = 0;

        for index in 0..self.links.len() {
            // The same rule the create-time match uses
            // ([`SubscriptionLink::matches_image`]), and the two have to agree:
            // an image that links to a subscription here is the image it reads
            // for as long as it exists.
            let link = &self.links[index];

            let matches = images.find(image_registration_id).is_some_and(|image| {
                link.matches_image(image.endpoint_id, image.stream_id, image.session_id)
            });

            if !matches {
                continue;
            }

            if self
                .link_image(
                    index,
                    image_registration_id,
                    images,
                    counters,
                    regions,
                    receiver,
                    now,
                    events,
                )
                .is_err()
            {
                failures += 1;
            } else {
                linked += 1;
            }
        }

        let _ = linked;

        if failures > 0 {
            return Err(());
        }

        Ok(())
    }
}

/// The checks a subscription's channel has to pass
/// (`validate_control_for_subscription`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:614-628`).
///
/// # Errors
///
/// [`AddSubscriptionError::Endpoint`] for a channel that cannot be listened on.
fn validate_for_subscription(channel: &UdpChannel) -> Result<(), AddSubscriptionError> {
    if channel.has_explicit_control && channel.local_control.port() == 0 {
        return Err(AddSubscriptionError::Endpoint {
            message: format!(
                "control has port=0 for subscription: channel={}",
                String::from_utf8_lossy(&channel.original_uri)
            ),
        });
    }

    Ok(())
}

/// The endpoint's own bookkeeping for a subscription that is going away
/// (`aeron_driver_conductor_unlink_from_endpoint`, `:1179-1201`).
///
/// It runs **before** the subscribable unlink, and it is the half a network
/// subscription cannot do without: the endpoint's reference count for the
/// stream drops, and at zero the receiver is told the stream has no readers
/// left (`aeron_receive_channel_endpoint_decref_to_stream`, `:720-744`) — so
/// the dispatcher stops feeding it, and an image nobody reads stops sending
/// status messages. Without this a removed subscription's image lives on, and
/// a group strategy on the far side waits for a receiver that will never
/// answer again (`aeron_min_flow_control.c:109-111`).
///
/// A link with no endpoint — an IPC one, and a spy's — has no count to drop,
/// which is why this is an `Option` rather than a branch at every call site.
fn unlink_from_endpoint(
    link: &SubscriptionLink,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    endpoints: &mut crate::receive_endpoints::ReceiveChannelEndpoints,
    receiver: Option<&ReceiverProxy>,
) {
    let (Some(receiver), Some(endpoint_id)) = (receiver, link.endpoint_id) else {
        return;
    };

    // `:1187-1198` picks which of the endpoint's counts to drop — the session's
    // when the subscription named one, the response stream's when it is a
    // response channel, the stream's otherwise. What travels here is the
    // subscription's own properties; the receiver decides, because the counts
    // are its.
    let _ = receiver.remove_subscription(endpoint_id, link.stream_id, link.session_id);

    // And if that was the last thing on the endpoint, the endpoint goes too
    // (`aeron_receive_channel_endpoint_decref_to_stream` ends in
    // `try_remove_endpoint` for exactly this case, `:736-741`).
    if !endpoints.detach_subscription(endpoint_id) {
        return;
    }

    try_remove_endpoint(endpoint_id, counters, regions, endpoints, receiver);
}

/// Let an idle endpoint go: mark it, then ask the receiver, which owns the
/// socket (`aeron_receive_channel_endpoint_try_remove_endpoint`,
/// `media/aeron_receive_channel_endpoint.c:691-702`, whose CLOSING is
/// `conductor_fields.status` at `:699`).
///
/// The `rcv-channel` counter is **not** written here: the reference writes it
/// once, ACTIVE, when the endpoint is made
/// (`aeron_driver_conductor.c:2013`) and then gives the record back
/// (`aeron_receive_channel_endpoint_delete`,
/// `media/aeron_receive_channel_endpoint.c:163-168`, which is where the counter
/// goes back — the receiver owns the endpoint, but the counters are the
/// conductor's, so the giving back happens on this side).
pub(crate) fn try_remove_endpoint(
    endpoint_id: u64,
    _counters: &mut CounterManager,
    _regions: &CounterRegions<'_>,
    endpoints: &mut crate::receive_endpoints::ReceiveChannelEndpoints,
    receiver: &ReceiverProxy,
) {
    if !endpoints.begin_release(endpoint_id) {
        return;
    }

    let _ = receiver.remove_endpoint(endpoint_id);
}

/// Detach every reader a link holds and give its counter back
/// (`aeron_driver_conductor_unlink_all_subscribable`, `:3679-3693`).
///
/// The position leaves the publication's set first — which is what the removal
/// hook counts, and what closes the log's `is_connected` byte when it was the
/// last reader — and the counter goes back after.
fn unlink_all(
    link: SubscriptionLink,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    publications: &mut IpcPublications,
    receiver: Option<&ReceiverProxy>,
    sender: Option<&SenderProxy>,
    now_ms: i64,
) {
    for entry in &link.subscribables {
        match entry.target {
            // An image's readers are removed by the receiver, which owns the
            // image; the counter goes back here either way, and the reader
            // waits at a position nothing feeds any more, which is what a
            // removal means.
            SubscriptionTarget::Image(registration_id) => {
                if let Some(receiver) = receiver {
                    let _ = receiver.remove_subscriber(registration_id, entry.counter_id);
                }
            }
            // A network publication's readers are the sender's, so the
            // position comes out of its set before the counter goes back — a
            // set that still held the id would read whatever took its place.
            SubscriptionTarget::NetworkPublication(registration_id) => {
                if let Some(sender) = sender {
                    let _ = sender.remove_subscriber(registration_id, entry.counter_id);
                }
            }
            SubscriptionTarget::IpcPublication(registration_id) => {
                if let Some(publication) = publications
                    .publications_mut()
                    .iter_mut()
                    .find(|publication| publication.registration_id == registration_id)
                {
                    publication.remove_subscriber(entry.counter_id);
                }
            }
        }

        counters.free(regions, entry.counter_id, now_ms);
    }
}

/// Give a subscription a reader position in a publication, and tell its client
/// where the log buffer is (`aeron_driver_conductor_link_subscribable`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:3547-3617`).
///
/// The order is the reference's, and each step is load-bearing:
///
/// 1. the counter is allocated with the **join position** in its label;
/// 2. its owner is the client and its *reference* is the publication — the two
///    ids a reader of `AeronStat` needs to see which publication a reader
///    belongs to;
/// 3. the position joins the publication's subscribable set, which is what
///    flips the log's `is_connected` byte to one and is why a producer must
///    not be told about the publication before this;
/// 4. the counter is **then** set to the join position: a reader that appears
///    in the set with a position of zero would look like a reader that has
///    read nothing while the publication is a term ahead — and the limit
///    would be computed from that;
/// 5. and only then does the client hear about it.
///
/// # Errors
///
/// `Err(())` when the counter cannot be allocated or the publication refuses
/// the position; the caller decides what the client is told.
fn link_subscribable(
    link: &mut SubscriptionLink,
    publication: &mut IpcPublication,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    now: crate::ipc_publications::Now,
    events: &mut impl ClientEvents,
) -> Result<(), ()> {
    let joining_position = publication.join_position(counters, regions);

    let Some(counter_id) = allocate_reader_position(
        link,
        publication.registration_id,
        publication.session_id,
        publication.stream_id,
        joining_position,
        counters,
        regions,
        now,
    ) else {
        return Err(());
    };

    if !publication.add_subscriber(TetherablePosition {
        counter_id,
        subscription_registration_id: link.registration_id,
        time_of_last_update_ns: now.ns,
        state: TetherState::Active,
        is_tether: link.is_tether,
        is_rejoin: link.is_rejoin,
    }) {
        counters.free(regions, counter_id, now.ms);
        return Err(());
    }

    publish_reader(
        link,
        SubscriptionTarget::IpcPublication(publication.registration_id),
        counter_id,
        publication.session_id,
        publication.stream_id,
        joining_position,
        publication.path_bytes().to_vec(),
        counters,
        regions,
        events,
    )
}

/// Give a **spy** subscription a reader position in a network publication
/// (`aeron_driver_conductor_link_subscribable` called from the two spy scans,
/// `aeron-driver/src/main/c/aeron_driver_conductor.c:4897-4921` and
/// `:4201-4226`).
///
/// The same five steps as [`link_subscribable`], with one missing and one
/// different:
///
/// * there is **no publication-side attach**. A spy's position does not join
///   the network publication's subscribable set here: that half of a link
///   belongs to the publication, and a network publication is the sender's.
///   The spy's position becomes something the publication counts when the
///   sender is told about it.
/// * the join position is `snd-pos` rather than the producer's
///   (`aeron_network_publication_join_position`,
///   `aeron_network_publication.h:238-241`, which reads the sender's own
///   position). A spy starts where the *stream* has got to, not where the
///   producer has, which is why one that arrives late does not read the buffer
///   from the beginning.
///
/// What the client is told is an ordinary image message whose source identity
/// is the **IPC constant** and whose log file is the publication's own
/// (`:4913-4914`): a spy reads a log buffer in this process, and the constant
/// is how the reference says so.
fn link_spy_publication(
    link: &mut SubscriptionLink,
    publication: &NetworkPublicationRecord,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    sender: &SenderProxy,
    now: crate::ipc_publications::Now,
    events: &mut impl ClientEvents,
) -> Result<(), ()> {
    let joining_position = counters
        .value(regions, publication.counters.snd_pos)
        .unwrap_or(0);

    let Some(counter_id) = allocate_reader_position(
        link,
        publication.registration_id,
        publication.session_id,
        publication.stream_id,
        joining_position,
        counters,
        regions,
        now,
    ) else {
        return Err(());
    };

    publish_reader(
        link,
        SubscriptionTarget::NetworkPublication(publication.registration_id),
        counter_id,
        publication.session_id,
        publication.stream_id,
        joining_position,
        publication.path_bytes().to_vec(),
        counters,
        regions,
        events,
    )?;

    // And only now does the **publication** hear about it, which is the
    // reader count this whole path exists for: from here the publication
    // counts the spy, its limits are computed with it, and `ssc` can make it
    // look connected. The counter is already seeded (in `publish_reader`),
    // because the sender reads it the moment the position is in the set — the
    // reference has the two in the other order only because one thread does
    // both.
    let _ = sender.add_subscriber(
        publication.registration_id,
        TetherablePosition {
            counter_id,
            subscription_registration_id: link.registration_id,
            time_of_last_update_ns: now.ns,
            state: TetherState::Active,
            is_tether: link.is_tether,
            is_rejoin: link.is_rejoin,
        },
    );

    Ok(())
}

/// The first two steps of a link: the reader's counter, with the join position
/// in its label, owned by the client and referenced by what it reads
/// (`aeron_driver_conductor_link_subscribable`, `:3561-3578`).
///
/// # Errors
///
/// `None` when there is no counter left or the reference id will not take — in
/// which case the counter has already been given back.
#[allow(clippy::too_many_arguments)] // the four ids the counter is labelled with and the two views
fn allocate_reader_position(
    link: &SubscriptionLink,
    reference_id: i64,
    session_id: i32,
    stream_id: i32,
    joining_position: i64,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    now: crate::ipc_publications::Now,
) -> Option<i32> {
    let counter_id = counter_position::allocate_subscription_position(
        counters,
        regions,
        link.client_id,
        link.registration_id,
        session_id,
        stream_id,
        &link.channel,
        joining_position,
        now.ms,
    )?;

    if counters
        .set_reference_id(regions, counter_id, reference_id)
        .is_none()
    {
        counters.free(regions, counter_id, now.ms);
        return None;
    }

    Some(counter_id)
}

/// The last three steps: the entry that makes the link a reader, the seed, and
/// the message (`aeron_driver_conductor_link_subscribable`, `:3582-3605`).
///
/// # Errors
///
/// `Err(())` when the counter will not take the join position. The reader is
/// attached by then, and undoing that would be worse than leaving it: the
/// publication already counts it and the limit will hold the producer back to
/// a position the client never read.
#[allow(clippy::too_many_arguments)] // what the message carries and what it is made from
fn publish_reader(
    link: &mut SubscriptionLink,
    target: SubscriptionTarget,
    counter_id: i32,
    session_id: i32,
    stream_id: i32,
    joining_position: i64,
    log_file: Vec<u8>,
    counters: &mut CounterManager,
    regions: &CounterRegions<'_>,
    events: &mut impl ClientEvents,
) -> Result<(), ()> {
    link.subscribables
        .push(SubscriptionLinkEntry { target, counter_id });

    if counters
        .set_value(regions, counter_id, joining_position)
        .is_none()
    {
        return Err(());
    }

    events.available_image(&ImageBuffersReady {
        correlation_id: target.registration_id(),
        session_id,
        stream_id,
        subscriber_registration_id: link.registration_id,
        subscriber_position_id: counter_id,
        log_file: &log_file,
        source_identity: IPC_CHANNEL,
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ipc_publication::{PublicationIdentity, ShareMismatch};

    /// A publication to match against, built through the real constructor so
    /// that the fields the rule reads are the ones a driver would have.
    fn publication(session_id: i32, stream_id: i32, is_exclusive: bool) -> IpcPublication {
        publication_answering(session_id, stream_id, is_exclusive, -1)
    }

    /// The same, for a publication that names what it answers: its
    /// `response-correlation-id` is the registration id of the **request**
    /// publication, which is the first link of the chain
    /// [`IpcSubscriptions::attach_response_publication`] walks.
    fn publication_answering(
        session_id: i32,
        stream_id: i32,
        is_exclusive: bool,
        response_correlation_id: i64,
    ) -> IpcPublication {
        // A counter as well as the ids: two tests run at once in one process,
        // and a log buffer is created *exclusively* — the second one to ask
        // for the same path fails rather than sharing it.
        static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "deepmsg-subscription-{}-{session_id}-{stream_id}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let path = dir.join("publication.logbuffer");

        let log = deepmsg_core::logbuffer::logfile::LogFile::create(&path, 64 * 1024, 4096, false)
            .expect("a log buffer");
        let params = crate::publication_params::PublicationParams {
            term_length: 64 * 1024,
            term_length_named: false,
            mtu_length: 1408,
            mtu_length_named: false,
            publication_window_length: 32 * 1024,
            max_resend: 0,
            entity_tag: -1,
            response_correlation_id,
            // The **request** side, which is what the chain's first link is: a
            // publication that names an `response-correlation-id` without a
            // `control-mode=response` is the one a response is *owed* to, and
            // it is its registration id the answering publication quotes.
            is_response: false,
            session_id: Some(session_id),
            linger_timeout_ns: 5_000_000_000,
            untethered_window_limit_timeout_ns: 5_000_000_000,
            untethered_linger_timeout_ns: 5_000_000_000,
            untethered_resting_timeout_ns: 10_000_000_000,
            is_sparse: true,
            signal_eos: true,
            spies_simulate_connection: false,
            starting_position: None,
            initial_term_id: 17,
        };

        let publication = IpcPublication::create(
            Box::new(log),
            PublicationIdentity {
                registration_id: i64::from(session_id),
                client_id: 7,
                session_id,
                stream_id,
                channel: b"aeron:ipc".to_vec(),
                is_exclusive,
            },
            &params,
            4096,
            crate::sys::SocketBufferLengths {
                rcvbuf: 0,
                sndbuf: 0,
            },
            1,
            2,
            crate::publication_image::IMAGE_LIVENESS_TIMEOUT_NS,
        )
        .expect("a publication");

        let _ = std::fs::remove_dir_all(&dir);

        publication
    }

    fn link(stream_id: i32, session_id: Option<i32>, is_response: bool) -> SubscriptionLink {
        SubscriptionLink {
            registration_id: 9,
            client_id: 7,
            stream_id,
            session_id,
            channel: b"aeron:ipc".to_vec(),
            is_tether: true,
            is_rejoin: false,
            is_response,
            group: InferableBoolean::Infer,
            setup_status: SetupStatus::Pending,
            is_reliable: true,
            is_sparse: true,
            endpoint_id: None,
            spy_channel: None,
            subscribables: Vec::new(),
        }
    }

    /// A link that spies on `spy_uri`, with the channel as the client wrote it
    /// — prefix and all, because that is what the counter label carries.
    fn spy_link(stream_id: i32, session_id: Option<i32>, spy_uri: &str) -> SubscriptionLink {
        let full = format!("aeron-spy:{spy_uri}");

        SubscriptionLink {
            channel: full.as_bytes().to_vec(),
            spy_channel: Some(
                crate::udp_channel::resolve_spy_channel(full.as_bytes()).expect("a spy channel"),
            ),
            ..link(stream_id, session_id, false)
        }
    }

    /// The channel a publication's endpoint was created from.
    fn publication_channel(uri: &str) -> UdpChannel {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel")
    }

    #[test]
    fn a_spy_matches_the_channel_it_spells_the_same_way() {
        let spy = spy_link(1001, None, "aeron:udp?endpoint=127.0.0.1:40123");
        let channel = publication_channel("aeron:udp?endpoint=127.0.0.1:40123");

        assert!(spy.spy_matches(&channel, 1001, 100));

        // Another stream is another publication, whatever the channel says.
        assert!(!spy.spy_matches(&channel, 1002, 100));

        // And a named session is looked for, not merely allowed: the wildcard
        // is the link that named none (`:74-79`).
        let named = spy_link(1001, Some(100), "aeron:udp?endpoint=127.0.0.1:40123");
        assert!(named.spy_matches(&channel, 1001, 100));
        assert!(!named.spy_matches(&channel, 1001, 101));
    }

    #[test]
    fn a_spy_matches_a_channel_that_names_the_same_sides_and_other_parameters() {
        // The reason the rule compares canonical forms rather than the strings
        // clients wrote: the form is the two sides and nothing else, so a spy
        // whose URI carries parameters the publisher's never mentioned is
        // reading the same channel (`media/aeron_udp_channel.c:148-208`).
        let spy = spy_link(1001, None, "aeron:udp?endpoint=127.0.0.1:40123|mtu=1408");
        let channel = publication_channel("aeron:udp?endpoint=127.0.0.1:40123");

        assert!(spy.spy_matches(&channel, 1001, 100));

        // What it does **not** do is see through two spellings of one address:
        // a name and the address it resolves to are two channels here, and a
        // spy that wrote the name reads nothing.
        let by_name = spy_link(1001, None, "aeron:udp?endpoint=localhost:40123");
        assert!(!by_name.spy_matches(&channel, 1001, 100));

        // And a different endpoint is a different channel, and no session
        // makes it the same one.
        let elsewhere = publication_channel("aeron:udp?endpoint=127.0.0.1:40124");
        assert!(!spy.spy_matches(&elsewhere, 1001, 100));
    }

    #[test]
    fn a_channel_tag_matches_a_spy_that_the_canonical_form_would_not() {
        // `tags=` is the other way two channels are the same channel, and it is
        // the one that reaches across different endpoints (`:96-98`).
        let spy = spy_link(1001, None, "aeron:udp?endpoint=127.0.0.1:40123|tags=17,3");
        let tagged_elsewhere = publication_channel("aeron:udp?endpoint=127.0.0.1:49999|tags=17,1");
        let untagged_elsewhere = publication_channel("aeron:udp?endpoint=127.0.0.1:49999");

        assert!(spy.spy_matches(&tagged_elsewhere, 1001, 100));
        assert!(
            !spy.spy_matches(&untagged_elsewhere, 1001, 100),
            "one tag is not a shared tag"
        );

        // And the stream is still necessary: a tag does not cross streams.
        assert!(!spy.spy_matches(&tagged_elsewhere, 1002, 100));

        // An untagged spy never matches by tag, whatever the publication says —
        // `AERON_URI_INVALID_TAG` is the absence of a tag, not a value two
        // channels can share (`:94-95`).
        let untagged = spy_link(1001, None, "aeron:udp?endpoint=127.0.0.1:40123");
        assert!(!untagged.spy_matches(&tagged_elsewhere, 1001, 100));
    }

    #[test]
    fn a_subscription_that_is_not_a_spy_matches_no_publication() {
        let ipc = link(1001, None, false);

        assert!(ipc.spy_channel.is_none());
        assert!(!ipc.spy_matches(
            &publication_channel("aeron:udp?endpoint=127.0.0.1:40123"),
            1001,
            100
        ));
    }

    #[test]
    fn a_subscription_without_a_session_matches_every_session_on_its_stream() {
        let subscriber = link(1001, None, false);

        assert!(subscriber.matches(&publication(100, 1001, false)));
        assert!(
            subscriber.matches(&publication(-7, 1001, false)),
            "any session"
        );
        assert!(
            !subscriber.matches(&publication(100, 1002, false)),
            "another stream"
        );
    }

    #[test]
    fn a_named_session_matches_only_that_one() {
        let subscriber = link(1001, Some(100), false);

        assert!(subscriber.matches(&publication(100, 1001, false)));
        assert!(!subscriber.matches(&publication(101, 1001, false)));
    }

    #[test]
    fn a_response_subscription_without_a_session_matches_nothing() {
        // The one asymmetry in the rule: a response channel is not looking for
        // "whatever publishes this stream", it is looking for the one stream it
        // was told to answer, so naming no session means it matches none
        // (`aeron_driver_conductor.c:110-117`).
        let response = link(1001, None, true);

        assert!(!response.matches(&publication(100, 1001, false)));

        let named = link(1001, Some(100), true);
        assert!(named.matches(&publication(100, 1001, false)));
    }

    #[test]
    fn a_subscription_reads_an_image_on_its_own_endpoint_and_no_other() {
        // `aeron_driver_conductor.c:87`: the endpoint is the first clause, and
        // it is the one a reader would leave out. Two channels can name the
        // same stream and be read on the same session, and an image is still
        // only ever read through the endpoint that holds its socket.
        let mut subscriber = link(1001, None, false);
        subscriber.endpoint_id = Some(3);

        assert!(subscriber.matches_image(3, 1001, 100));
        assert!(
            !subscriber.matches_image(4, 1001, 100),
            "another endpoint: another socket, another session"
        );

        // An IPC subscription has no endpoint, and no image is ever its.
        let ipc = link(1001, None, false);
        assert!(ipc.endpoint_id.is_none());
        assert!(!ipc.matches_image(3, 1001, 100));
    }

    #[test]
    fn a_response_subscription_reads_no_image_until_a_session_is_named() {
        // The same asymmetry as `matches`, and it is what makes the two agree:
        // a response subscription that named no session is waiting for a
        // RSP_SETUP, not looking for whatever appears
        // (`aeron_driver_conductor.c:75-79`).
        let mut response = link(1001, None, true);
        response.endpoint_id = Some(3);

        assert!(!response.matches_image(3, 1001, 100));

        // Once the setup completes, the reference clears `is_response` and pins
        // the session (`:7100-7105`), and the rule reads it like any other.
        let mut completed = link(1001, Some(100), false);
        completed.endpoint_id = Some(3);

        assert!(completed.matches_image(3, 1001, 100));
        assert!(!completed.matches_image(3, 1001, 101), "a named session");
    }

    /// The three links of the chain, built the way a driver would meet them:
    /// a request publication whose `response-correlation-id` is the response
    /// subscription's registration id, and an answering publication whose own
    /// names the request's.
    ///
    /// `io.aeron.ResponseChannelsTest::shouldErrorAddingResponseIpcPublicationOnSessionMismatch`
    /// is the oracle for the refusal; the pinning it does on the way past is
    /// what makes an IPC response subscription read at all
    /// ([`SubscriptionLink::matches`]).
    fn response_chain(
        subscription_registration_id: i64,
        sub_session_id: Option<i32>,
    ) -> (IpcSubscriptions, IpcPublication, i64) {
        let request = publication_answering(100, 1001, false, subscription_registration_id);
        let request_registration_id = request.registration_id;

        let mut subscriptions = IpcSubscriptions::new();
        let mut response_sub = link(1001, sub_session_id, true);
        response_sub.registration_id = subscription_registration_id;
        subscriptions.links.push(response_sub);

        (subscriptions, request, request_registration_id)
    }

    #[test]
    fn a_response_publication_pins_its_subscription_to_its_own_session() {
        let (mut subscriptions, request, request_registration_id) = response_chain(9, None);

        assert!(
            !subscriptions.links()[0].matches(&publication(555, 1001, false)),
            "a response subscription with no session reads nothing until one is named"
        );

        subscriptions
            .attach_response_publication(
                &[request],
                request_registration_id,
                77,
                b"aeron:ipc?control-mode=response",
                555,
            )
            .expect("it named no session, so there is nothing to disagree with");

        assert_eq!(Some(555), subscriptions.links()[0].session_id);
        assert!(
            subscriptions.links()[0].matches(&publication(555, 1001, false)),
            "and now it reads the stream it was told to answer"
        );
        assert!(
            !subscriptions.links()[0].matches(&publication(556, 1001, false)),
            "and no other"
        );
    }

    #[test]
    fn a_response_publication_that_would_answer_under_another_session_is_refused() {
        let (mut subscriptions, request, request_registration_id) = response_chain(9, Some(42));

        let error = subscriptions
            .attach_response_publication(
                &[request],
                request_registration_id,
                77,
                b"aeron:ipc?control-mode=response",
                555,
            )
            .expect_err("42 is not 555");

        assert!(
            error.starts_with("failed to create response publication (registrationId=77,"),
            "the reference's words, which a client reads as a `RegistrationException`: {error}"
        );
        assert!(
            error.contains("uses `session-id` parameter that does not match `session-id=555`"),
            "and both sessions, so the reader can see which is which: {error}"
        );
        assert_eq!(
            Some(42),
            subscriptions.links()[0].session_id,
            "and the subscription keeps the session it chose"
        );
    }

    #[test]
    fn a_chain_that_reaches_no_subscription_is_not_an_error() {
        // An ordinary publication names no correlation id, and a response
        // publication whose request publication has gone names one that is not
        // there. Neither is a client's mistake, so neither is refused
        // (`aeron_driver_conductor.c:1714-1716` guards the whole walk).
        let mut subscriptions = IpcSubscriptions::new();
        let mut response_sub = link(1001, Some(42), true);
        response_sub.registration_id = 9;
        subscriptions.links.push(response_sub);

        let ordinary = publication(100, 1001, false);

        subscriptions
            .attach_response_publication(&[ordinary], -1, 77, b"aeron:ipc", 555)
            .expect("an ordinary publication");
        assert_eq!(Some(42), subscriptions.links()[0].session_id);

        // A request publication that is there, with no subscription behind it.
        let orphan = publication_answering(100, 1001, false, 404);
        subscriptions
            .attach_response_publication(&[orphan], 100, 77, b"aeron:ipc", 555)
            .expect("a request publication nobody is waiting on");
        assert_eq!(Some(42), subscriptions.links()[0].session_id);
    }

    #[test]
    fn a_subscription_that_reads_nothing_counts_no_images() {
        let subscriptions = IpcSubscriptions::new();

        assert!(subscriptions.links().is_empty());
        assert_eq!(0, subscriptions.images());
    }

    #[test]
    fn every_add_subscription_error_has_a_code_the_client_can_read() {
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_INVALID_CHANNEL,
            AddSubscriptionError::Params(PublicationParamsError::Uri(
                crate::channel_uri::UriError::InvalidScheme
            ))
            .error_code()
        );
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_NOT_SUPPORTED,
            AddSubscriptionError::UnsupportedTransport.error_code()
        );
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            AddSubscriptionError::Link.error_code()
        );

        // And the publication errors it can wrap map onto the same two codes.
        assert_eq!(
            ERROR_CODE_GENERIC_ERROR,
            AddSubscriptionError::from(AddError::Share(ShareMismatch::Mtu {
                existing: 1,
                requested: 2
            }))
            .error_code()
        );
    }
}
