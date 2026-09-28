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

use crate::channel_uri::{ChannelUri, Transport};
use crate::clients::{ClientEvents, Clients};
use crate::config::DriverConfig;
use crate::ipc_publications::{AddError, IpcPublications};
use crate::publication_params::{PublicationParamsError, SubscriptionParams};
use crate::subscribable::TetherState;
use crate::subscribable::TetherablePosition;
use crate::{ipc_publication::IpcPublication, position as counter_position};

/// The channel an IPC image reports as its source
/// (`AERON_IPC_CHANNEL`, `aeron-client/src/main/c/uri/aeron_uri.h:39`).
///
/// The *constant*, not the channel the client subscribed with: two clients can
/// each name `aeron:ipc` differently — one with parameters, one without — and
/// an image's source identity is about where the bytes come from rather than
/// about what the reader asked for.
pub const IPC_CHANNEL: &[u8] = b"aeron:ipc";

/// One (subscription, publication) pair: a reader position, and the counter it
/// is written through (`aeron_subscribable_list_entry_t`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.h:113-118`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionLinkEntry {
    /// Which publication this reader reads. The reference holds a pointer to
    /// the publication's subscribable; a registration id is the same
    /// reference here and survives the publications being reordered.
    pub publication_registration_id: i64,
    /// The `sub-pos` counter the client writes its position into.
    pub counter_id: i32,
}

/// One subscription (`aeron_subscription_link_t`,
/// `aeron-driver/src/main/c/aeron_driver_conductor.h:120-162`).
///
/// The reference's struct is wider than this and most of the difference is
/// network: an endpoint, a spy channel, a setup status and the group
/// consideration are all about a transport that has to be *built* before it can
/// be read from. An IPC subscription has nothing to set up — the log buffer it
/// reads already exists — which is why this is the flags, the identity and the
/// readers.
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
    /// Whether the channel is reliable. No effect on IPC, recorded because the
    /// link is where the reference records it.
    pub is_reliable: bool,
    /// Whether its log buffers are sparse. No effect on IPC, likewise.
    pub is_sparse: bool,
    /// What it reads: one entry per publication it was matched with.
    pub subscribables: Vec<SubscriptionLinkEntry>,
}

impl SubscriptionLink {
    /// Whether this subscription is already reading that publication
    /// (`aeron_driver_conductor_is_subscribable_linked`, `:3519-3523`).
    pub fn reads(&self, publication_registration_id: i64) -> bool {
        self.subscribables
            .iter()
            .any(|entry| entry.publication_registration_id == publication_registration_id)
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
}

impl AddSubscriptionError {
    /// The `ON_ERROR` code this failure is reported under, by the same rule as
    /// [`AddError::error_code`].
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Params(PublicationParamsError::Uri(_)) => {
                deepmsg_cnc::command::ERROR_CODE_INVALID_CHANNEL
            }
            Self::UnsupportedTransport => deepmsg_cnc::command::ERROR_CODE_NOT_SUPPORTED,
            Self::Params(_) | Self::NoClientRecord | Self::Link => ERROR_CODE_GENERIC_ERROR,
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
            is_reliable: params.is_reliable,
            is_sparse: params.is_sparse,
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
                .retain(|entry| entry.publication_registration_id != publication_registration_id);
        }
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

        unlink_all(link, counters, regions, publications, now_ms);

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
            unlink_all(link, counters, regions, publications, now_ms);
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
    now_ms: i64,
) {
    for entry in &link.subscribables {
        if let Some(publication) = publications
            .publications_mut()
            .iter_mut()
            .find(|publication| publication.registration_id == entry.publication_registration_id)
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
        publication_registration_id: publication.registration_id,
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
            is_reliable: true,
            is_sparse: true,
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
