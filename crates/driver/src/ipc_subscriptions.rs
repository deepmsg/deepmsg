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
    AddSubscriptionCommand, CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED, ERROR_CODE_GENERIC_ERROR,
    ImageBuffersReady,
};
use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::channel_uri::{ChannelUri, Transport, UriError};
use crate::clients::{ClientEvents, Clients};
use crate::config::DriverConfig;
use crate::ipc_publications::{AddError, IpcPublications};
use crate::publication_images::PublicationImages;
use crate::publication_params::{PublicationParamsError, SubscriptionParams};
use crate::receive_endpoints::ReceiveChannelEndpoints;
use crate::receiver::ReceiverProxy;
use crate::subscribable::TetherState;
use crate::subscribable::TetherablePosition;
use crate::udp_channel::{ControlMode, UdpChannel};
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
}

impl SubscriptionTarget {
    /// The registration id of whatever this points at.
    pub const fn registration_id(self) -> i64 {
        match self {
            Self::IpcPublication(registration_id) | Self::Image(registration_id) => registration_id,
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
/// `aeron-driver/src/main/c/aeron_driver_conductor.h:120-162`).
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
/// (`aeron_driver_conductor.c:5903-5919`).
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
}

impl AddSubscriptionError {
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
    /// (`aeron_driver_conductor_find_subscription_by_registration_id`).
    ///
    /// A destination is added to **a subscription**, not to a channel or an
    /// endpoint: the client holds the subscription's registration id, and the
    /// endpoint is what that subscription happens to read through
    /// (`aeron_driver_conductor.c:5903-5919`).
    pub fn find(&self, registration_id: i64) -> Option<&SubscriptionLink> {
        self.links
            .iter()
            .find(|link| link.registration_id == registration_id)
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
            setup_status: SetupStatus::Pending,
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
            endpoint_id: None,
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

    /// The subscriptions reading a publication, for the caller that has to tell
    /// them it is going away (`aeron_driver_conductor_unlink_ipc_subscriptions`,
    /// `:6453-6475`, which sends one message per reader).
    pub fn readers_of(&self, publication_registration_id: i64) -> Vec<&SubscriptionLink> {
        self.links
            .iter()
            .filter(|link| link.reads(publication_registration_id))
            .collect()
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
    pub fn remove(
        &mut self,
        registration_id: i64,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut IpcPublications,
        now_ms: i64,
    ) -> bool {
        let Some(index) = self
            .links
            .iter()
            .position(|link| link.registration_id == registration_id)
        else {
            return false;
        };

        let link = self.links.swap_remove(index);

        unlink_all(link, counters, regions, publications, None, now_ms);

        true
    }

    /// Give up every subscription a client owned, without telling it anything:
    /// it is gone, and a message to a client that is not there is a message
    /// nobody reads (`aeron_client_delete`, `:1234-1250`).
    pub fn remove_for_client(
        &mut self,
        client_id: i64,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        publications: &mut IpcPublications,
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
            unlink_all(link, counters, regions, publications, None, now_ms);
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

        let channel = UdpChannel::resolve(request.channel, &uri)
            .map_err(Box::new)
            .map_err(AddSubscriptionError::Channel)?;
        let params = SubscriptionParams::resolve(&uri, config)?;

        validate_for_subscription(&channel)?;

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

        let (endpoint_id, channel_status_counter_id, new_endpoint) = endpoints
            .get_or_add(
                channel,
                &endpoint_params,
                config,
                counters,
                regions,
                request.correlation_id,
                now.ms,
            )
            .map_err(|error| AddSubscriptionError::Endpoint {
                message: error.to_string(),
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
            setup_status: SetupStatus::Pending,
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
            endpoint_id: Some(endpoint_id),
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
    now_ms: i64,
) {
    for entry in &link.subscribables {
        if entry.target.is_image() {
            // An image's readers are removed by the receiver, which owns the
            // image; the counter goes back here either way, and the reader
            // waits at a position nothing feeds any more, which is what a
            // removal means.
            if let Some(receiver) = receiver {
                let _ =
                    receiver.remove_subscriber(entry.target.registration_id(), entry.counter_id);
            }
        } else if let Some(publication) = publications
            .publications_mut()
            .iter_mut()
            .find(|publication| publication.registration_id == entry.target.registration_id())
        {
            publication.remove_subscriber(entry.counter_id);
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

    let Some(counter_id) = counter_position::allocate_subscription_position(
        counters,
        regions,
        link.client_id,
        link.registration_id,
        publication.session_id,
        publication.stream_id,
        &link.channel,
        joining_position,
        now.ms,
    ) else {
        return Err(());
    };

    let linked = counters
        .set_reference_id(regions, counter_id, publication.registration_id)
        .is_some()
        && publication.add_subscriber(TetherablePosition {
            counter_id,
            subscription_registration_id: link.registration_id,
            time_of_last_update_ns: now.ns,
            state: TetherState::Active,
            is_tether: link.is_tether,
            is_rejoin: link.is_rejoin,
        });

    if !linked {
        counters.free(regions, counter_id, now.ms);
        return Err(());
    }

    link.subscribables.push(SubscriptionLinkEntry {
        target: SubscriptionTarget::IpcPublication(publication.registration_id),
        counter_id,
    });

    if counters
        .set_value(regions, counter_id, joining_position)
        .is_none()
    {
        // The reader is attached and its counter cannot be seeded: undoing
        // that here would be worse than leaving it, because the publication
        // already counts it and the limit will hold the producer back to a
        // position the client never read.
        return Err(());
    }

    events.available_image(&ImageBuffersReady {
        correlation_id: publication.registration_id,
        session_id: publication.session_id,
        stream_id: publication.stream_id,
        subscriber_registration_id: link.registration_id,
        subscriber_position_id: counter_id,
        log_file: publication.path_bytes(),
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
            response_correlation_id: -1,
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
            setup_status: SetupStatus::Pending,
            is_reliable: true,
            is_sparse: true,
            endpoint_id: None,
            subscribables: Vec::new(),
        }
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
