//! The send channel endpoints a driver owns, one per channel that shares.
//!
//! Mirrors the conductor's half of
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! `get_or_add_send_channel_endpoint` (`:1961-2030`), the two lookups that
//! decide whether a channel *is* an endpoint that exists
//! (`find_existing_send_channel_endpoint`, `:224-286`), the agreement a shared
//! one has to reach (`validate_channel_against_send_channel_endpoint`,
//! `:1907-1953`) and the timeout that collects an endpoint nobody publishes
//! through any more (`:1533-1586`).
//!
//! # Sharing is by canonical form, not by URI
//!
//! `aeron:udp?endpoint=localhost:40123` and `aeron:udp?endpoint=127.0.0.1:40123`
//! are the *same endpoint* — same canonical form — and a driver that made two
//! sockets for them would send every datagram twice and report two channel
//! statuses for one channel. The one exception is `tags=`: two channels that
//! name different tags are different endpoints even when they canonicalise
//! alike (`:229-257`), because a tag is the client saying "this one is
//! mine".
//!
//! # What a shared endpoint must agree on
//!
//! Its socket buffer sizes (`:1948-1966`): the second channel is not making a
//! socket, it is using one, and a `so-rcvbuf=` that disagrees with the socket
//! that exists is a channel whose configuration would be silently ignored.
//! The reference refuses it and so does this.
//!
//! # Where the socket lives
//!
//! [`SendChannelEndpoint`] owns it, and the endpoint is *created here and
//! moved to the sender* (`aeron_driver_sender_proxy_on_add_endpoint`,
//! `:2015`): the conductor decides, the sender thread owns. The entry this
//! module keeps is the bookkeeping the conductor needs afterwards — the
//! counter id it announced, the reference count, and the state.

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::media::send_endpoint::{self, EndpointStatus, PublicationDispatch, SendChannelEndpoint};
use crate::sys;
use crate::udp_channel::{INVALID_TAG, UdpChannel};

/// One endpoint, as the conductor sees it.
#[derive(Debug)]
pub struct SendChannelEndpointEntry {
    /// The id the sender thread knows this endpoint by.
    pub id: u64,
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// The `snd-channel` counter whose value is its state.
    pub channel_status_counter_id: i32,
    /// Where it is in its life.
    pub status: EndpointStatus,
    /// How many publications send through it. At zero the endpoint is a
    /// candidate for collection (`:1533-1586`).
    pub refcount: i32,
    /// Whether the sender thread has already let it go. Both sides have to
    /// agree before the socket and the counter are released.
    pub sender_released: bool,
    /// When the endpoint last had a publication, for the collection timeout.
    pub time_of_last_activity_ns: i64,
}

/// What `get_or_add` did.
#[derive(Debug)]
pub enum EndpointOutcome {
    /// The endpoint already existed and the channel agrees with it. Nothing to
    /// hand over: the socket is already in the sender.
    Shared {
        /// The endpoint's id.
        id: u64,
        /// Its channel-status counter.
        channel_status_counter_id: i32,
    },
    /// The endpoint was created. The socket is here, and the conductor hands
    /// it to the sender.
    Created {
        /// The endpoint's id.
        id: u64,
        /// Its channel-status counter.
        channel_status_counter_id: i32,
        /// The endpoint itself, socket and all.
        endpoint: Box<SendChannelEndpoint>,
    },
}

impl EndpointOutcome {
    /// The id, whichever arm this is.
    pub const fn id(&self) -> u64 {
        match self {
            Self::Shared { id, .. } | Self::Created { id, .. } => *id,
        }
    }

    /// The channel-status counter, whichever arm this is.
    pub const fn channel_status_counter_id(&self) -> i32 {
        match self {
            Self::Shared {
                channel_status_counter_id,
                ..
            }
            | Self::Created {
                channel_status_counter_id,
                ..
            } => *channel_status_counter_id,
        }
    }

    /// Whether the endpoint was made just now.
    pub const fn is_new(&self) -> bool {
        matches!(self, Self::Created { .. })
    }
}

/// Why an endpoint could not be had.
#[derive(Debug)]
pub enum EndpointError {
    /// The channel names no endpoint, no control address and no manual
    /// control mode, so there is nothing to connect to (`:246-256`).
    NoAddress,
    /// An endpoint with that canonical form is closing; the reference asks the
    /// client to retry (`:275-284`) rather than handing over a dying socket.
    Closing,
    /// A `tags=` match whose control mode or addresses disagree
    /// (`aeron_udp_channel_matches_tag`, `:564-624`).
    TagMismatch {
        /// The tag both channels named.
        tag: i64,
    },
    /// A buffer parameter that disagrees with the socket that exists
    /// (`:1948-1966`).
    BufferMismatch {
        /// Which parameter.
        param: &'static str,
        /// What the new channel asked for.
        requested: u64,
        /// What the socket has.
        existing: u64,
    },
    /// The counter manager is full.
    NoCounter,
    /// The socket could not be opened.
    Socket(std::io::Error),
}

impl EndpointError {
    /// The `ON_ERROR` code the reference answers with
    /// (`aeron_driver_conductor_on_error`, `aeron_driver_conductor.c:2326-2358`):
    /// only the closing case is a *named* protocol code; the rest are errnos
    /// and reach the client as a generic error.
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::Closing => deepmsg_cnc::command::ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
            _ => deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
        }
    }
}

impl std::fmt::Display for EndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAddress => f.write_str(
                "URI must have explicit control, endpoint, or be manual control-mode when original",
            ),
            Self::Closing => {
                f.write_str("send_channel_endpoint found in CLOSING state, please retry")
            }
            Self::TagMismatch { tag } => write!(f, "matching tag {tag} has mismatched endpoint"),
            Self::BufferMismatch {
                param,
                requested,
                existing,
            } => write!(
                f,
                "{param}={requested} does not match existing value of {existing}"
            ),
            Self::NoCounter => f.write_str("could not allocate the channel status counter"),
            Self::Socket(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for EndpointError {}

/// The endpoints a driver sends through.
#[derive(Debug, Default)]
pub struct SendChannelEndpoints {
    entries: Vec<SendChannelEndpointEntry>,
    next_id: u64,
}

impl SendChannelEndpoints {
    /// No endpoints.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
        }
    }

    /// The endpoints, in the order they were created.
    pub fn entries(&self) -> &[SendChannelEndpointEntry] {
        &self.entries
    }

    /// The entry for an id, which is how a command coming back from the sender
    /// finds its endpoint.
    pub fn get(&self, id: u64) -> Option<&SendChannelEndpointEntry> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// The mutable entry for an id.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut SendChannelEndpointEntry> {
        self.entries.iter_mut().find(|entry| entry.id == id)
    }

    /// The endpoint a channel canonicalises to, if its tag allows it
    /// (`find_existing_send_channel_endpoint`, `:224-286`).
    ///
    /// # Errors
    ///
    /// [`EndpointError::TagMismatch`] for a tag match that disagrees on the
    /// control mode or the addresses, [`EndpointError::Closing`] when the
    /// endpoint found is on its way out.
    pub fn find(&self, channel: &UdpChannel) -> Result<Option<u64>, EndpointError> {
        // `:229-257`: a channel with a tag looks for the endpoint that already
        // answers to it, wherever its canonical form puts it.
        if channel.tag_id != INVALID_TAG {
            for entry in &self.entries {
                if matches_tag(channel, &entry.channel)? {
                    return self.usable(entry);
                }
            }
        }

        let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.channel.canonical_form == channel.canonical_form)
        else {
            return Ok(None);
        };

        // Two different, named tags are two different endpoints even on one
        // canonical form.
        if entry.channel.tag_id != INVALID_TAG
            && channel.tag_id != INVALID_TAG
            && channel.tag_id != entry.channel.tag_id
        {
            return Ok(None);
        }

        self.usable(entry)
    }

    /// The id of an endpoint that can be used, or the error that says why not.
    fn usable(&self, entry: &SendChannelEndpointEntry) -> Result<Option<u64>, EndpointError> {
        if entry.status == EndpointStatus::Closing {
            return Err(EndpointError::Closing);
        }

        // A channel with no endpoint, no control address and no manual control
        // mode has nothing to connect to (`:246-256`). Checked here rather
        // than at parse time because it is a property of the *channel*, and
        // the parse is where the reference checks it too.
        Ok(Some(entry.id))
    }

    /// Create the endpoint for a channel, or find the one it shares
    /// (`get_or_add_send_channel_endpoint`, `:1961-2030`).
    ///
    /// # Errors
    ///
    /// [`EndpointError`] for a channel nothing can be built from, a closing
    /// endpoint, a tag or buffer disagreement, or a socket that will not open.
    #[allow(clippy::too_many_arguments)] // the collaborators a create needs
    pub fn get_or_add(
        &mut self,
        channel: UdpChannel,
        params: &crate::media::TransportParams,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ns: i64,
        now_ms: i64,
    ) -> Result<EndpointOutcome, EndpointError> {
        let defaults = sys::SocketBufferLengths {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            rcvbuf: params.socket_rcvbuf as i32,
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            sndbuf: params.socket_sndbuf as i32,
        };

        if let Some(id) = self.find(&channel)? {
            let entry = self.get(id).expect("just found");
            if let Some(mismatch) = buffer_mismatch_of(&channel, entry, defaults) {
                return Err(mismatch);
            }

            return Ok(EndpointOutcome::Shared {
                id,
                channel_status_counter_id: entry.channel_status_counter_id,
            });
        }

        // `:246-256`: a channel with nothing to connect to is refused before a
        // socket is made for it.
        if !channel.has_explicit_control
            && channel.control_mode != crate::udp_channel::ControlMode::Manual
            && !channel.has_explicit_endpoint
        {
            return Err(EndpointError::NoAddress);
        }

        let endpoint = SendChannelEndpoint::create(
            channel,
            params,
            counters,
            regions,
            registration_id,
            now_ms,
        )
        .map_err(|error| match error {
            send_endpoint::SendEndpointError::NoCounter => EndpointError::NoCounter,
            send_endpoint::SendEndpointError::Socket(error) => EndpointError::Socket(error),
        })?;

        // The status the counter holds: `ACTIVE` once the socket is there,
        // which is what a client reads to answer "is this channel's socket up
        // yet" (`aeron_driver_conductor.c:2013`,
        // `aeron_counter_set_release(endpoint->channel_status.value_addr,
        // AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE)`).
        endpoint.set_status(counters, regions, EndpointStatus::Active);

        let id = self.next_id;
        self.next_id += 1;

        let channel_status_counter_id = endpoint.channel_status_counter_id();

        self.entries.push(SendChannelEndpointEntry {
            id,
            channel: endpoint.channel.clone(),
            channel_status_counter_id,
            status: EndpointStatus::Active,
            refcount: 0,
            sender_released: false,
            time_of_last_activity_ns: now_ns,
        });

        Ok(EndpointOutcome::Created {
            id,
            channel_status_counter_id,
            endpoint: Box::new(endpoint),
        })
    }

    /// Attach a publication to an endpoint
    /// (`aeron_send_channel_endpoint_add_publication` on the sender side, and
    /// the reference count the conductor keeps beside it).
    ///
    /// Returns whether the `(stream, session)` pair was free.
    pub fn attach_publication(
        &mut self,
        id: u64,
        endpoint: &mut SendChannelEndpoint,
        dispatch: PublicationDispatch,
        now_ns: i64,
    ) -> bool {
        let added = endpoint.add_publication(dispatch);

        if let Some(entry) = self.get_mut(id) {
            #[allow(clippy::cast_possible_wrap)] // a publication count is small
            {
                entry.refcount += 1;
            }
            entry.time_of_last_activity_ns = now_ns;
        }

        added
    }

    /// Detach a publication from an endpoint's count. The publication itself
    /// leaves the sender's dispatch map, which is the sender's own business.
    pub fn detach_publication(&mut self, id: u64) -> bool {
        let Some(entry) = self.get_mut(id) else {
            return false;
        };

        entry.refcount -= 1;
        true
    }

    /// Whether an endpoint nobody publishes through any more can be collected
    /// (`:1533-1586`).
    ///
    /// The reference's rule is a timeout on the last activity *and* a zero
    /// reference count, because a publication being created is a publication
    /// that has not counted itself yet.
    pub const fn is_collectable(
        entry: &SendChannelEndpointEntry,
        now_ns: i64,
        timeout_ns: i64,
    ) -> bool {
        entry.refcount <= 0 && now_ns - entry.time_of_last_activity_ns >= timeout_ns
    }

    /// Forget an endpoint once both sides have let it go.
    pub fn remove(&mut self, id: u64) -> Option<SendChannelEndpointEntry> {
        let index = self.entries.iter().position(|entry| entry.id == id)?;
        Some(self.entries.swap_remove(index))
    }
}

/// Whether a new channel answers to an existing endpoint's tag
/// (`aeron_udp_channel_matches_tag`,
/// `aeron-driver/src/main/c/media/aeron_udp_channel.c:564-624`).
///
/// Both tags have to be named and equal, the control modes have to agree, and
/// the addresses have to agree — a channel with no address at all matches
/// anything, which is the reference's `aeron_udp_channel_is_wildcard` escape.
///
/// # Errors
///
/// [`EndpointError::TagMismatch`] when the tags are equal and something else
/// is not.
fn matches_tag(channel: &UdpChannel, existing: &UdpChannel) -> Result<bool, EndpointError> {
    if channel.tag_id == INVALID_TAG
        || existing.tag_id == INVALID_TAG
        || channel.tag_id != existing.tag_id
    {
        return Ok(false);
    }

    if channel.control_mode != existing.control_mode {
        return Err(EndpointError::TagMismatch {
            tag: channel.tag_id,
        });
    }

    // A channel with no address of its own matches whatever the endpoint is.
    if !channel.has_explicit_endpoint && !channel.has_explicit_control {
        return Ok(true);
    }

    if channel.remote_data != existing.remote_data || channel.local_data != existing.local_data {
        return Err(EndpointError::TagMismatch {
            tag: channel.tag_id,
        });
    }

    Ok(true)
}

/// The buffer-parameter agreement a shared endpoint requires
/// (`validate_channel_against_send_channel_endpoint`, `:1907-1953`).
fn buffer_mismatch_of(
    channel: &UdpChannel,
    entry: &SendChannelEndpointEntry,
    defaults: sys::SocketBufferLengths,
) -> Option<EndpointError> {
    let (param, requested, existing) = send_endpoint::buffer_mismatch(
        channel,
        entry.channel.socket_rcvbuf_length,
        entry.channel.socket_sndbuf_length,
        defaults,
    )?;

    Some(EndpointError::BufferMismatch {
        param,
        requested,
        existing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::media::TransportParams;
    use crate::position as counter_position;
    use deepmsg_core::buffer::AtomicBuffer;

    #[repr(align(64))]
    struct Region(Vec<u8>);

    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            const VALUES_LENGTH: usize = 64 * 1024;
            Self {
                metadata: Region(vec![0u8; VALUES_LENGTH * 4]),
                values: Region(vec![0u8; VALUES_LENGTH]),
            }
        }

        fn open(&mut self) -> (CounterManager, CounterRegions<'_>) {
            let regions = CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.values.0).expect("aligned"),
            )
            .expect("four-to-one");
            let manager = CounterManager::new(64 * 1024, 1_000).expect("room");

            (manager, regions)
        }
    }

    fn channel(uri: &str) -> UdpChannel {
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel")
    }

    fn defaults() -> TransportParams {
        TransportParams::default()
    }

    #[test]
    fn a_channel_that_canonicalises_alike_shares_the_endpoint() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        let first = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123"),
                &defaults(),
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");
        assert!(first.is_new());

        // A second publication that names the *same* address another way — a
        // different URI, the same canonical form.
        let second = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123|mtu=1408"),
                &defaults(),
                &mut counters,
                &regions,
                8,
                2,
                2,
            )
            .expect("an endpoint");

        assert!(!second.is_new(), "one channel, one socket");
        assert_eq!(first.id(), second.id());
        assert_eq!(
            first.channel_status_counter_id(),
            second.channel_status_counter_id()
        );
        assert_eq!(1, endpoints.entries().len());
    }

    #[test]
    fn a_named_tag_keeps_a_channel_to_itself() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        let one = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123|tags=1"),
                &defaults(),
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");

        let two = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123|tags=2"),
                &defaults(),
                &mut counters,
                &regions,
                8,
                2,
                2,
            )
            .expect("an endpoint");

        assert_ne!(one.id(), two.id(), "different tags, different endpoints");
        assert_eq!(2, endpoints.entries().len());
    }

    #[test]
    fn a_tag_that_answers_to_an_endpoint_with_another_address_is_refused() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123|tags=5"),
                &defaults(),
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");

        // The same tag on a *different* address is the client contradicting
        // itself, which the reference refuses with the addresses named.
        let error = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40124|tags=5"),
                &defaults(),
                &mut counters,
                &regions,
                8,
                2,
                2,
            )
            .expect_err("refused");

        assert!(
            matches!(error, EndpointError::TagMismatch { tag: 5 }),
            "{error}"
        );
    }

    #[test]
    fn a_channel_with_nothing_to_connect_to_is_refused() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        // A tag with no endpoint, no control address and no control mode: the
        // parse accepts it — the reference's own check is the same one — and
        // there is still nowhere to send.
        let error = endpoints
            .get_or_add(
                channel("aeron:udp?tags=3"),
                &defaults(),
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect_err("refused");

        assert!(matches!(error, EndpointError::NoAddress), "{error}");
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR,
            error.error_code()
        );

        // `control-mode=manual` is the exception the reference carves out: the
        // channel has no address *yet*, and a manual endpoint gets its
        // destinations from `ADD_DESTINATION` instead.
        assert!(
            endpoints
                .get_or_add(
                    channel("aeron:udp?control-mode=manual|tags=3"),
                    &defaults(),
                    &mut counters,
                    &regions,
                    7,
                    1,
                    1,
                )
                .is_ok()
        );
    }

    #[test]
    fn a_shared_endpoint_refuses_a_buffer_parameter_it_does_not_have() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=1m"),
                &defaults(),
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");

        let error = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=2m"),
                &defaults(),
                &mut counters,
                &regions,
                8,
                2,
                2,
            )
            .expect_err("refused");

        assert!(
            matches!(
                error,
                EndpointError::BufferMismatch {
                    param: "so-sndbuf",
                    requested: 2_097_152,
                    existing: 1_048_576,
                }
            ),
            "{error}"
        );

        // Naming none is not a disagreement: it is the socket that exists.
        assert!(
            endpoints
                .get_or_add(
                    channel("aeron:udp?endpoint=127.0.0.1:40123"),
                    &defaults(),
                    &mut counters,
                    &regions,
                    9,
                    3,
                    3,
                )
                .expect("an endpoint")
                .id()
                == 1
        );
    }

    #[test]
    fn the_channel_status_counter_is_the_one_the_reference_allocates() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        let outcome = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123"),
                &defaults(),
                &mut counters,
                &regions,
                77,
                1,
                1,
            )
            .expect("an endpoint");

        let id = outcome.channel_status_counter_id();

        // Decoded the way a client decodes it, through the counter region's
        // own reader.
        let mut descriptor = None;
        regions.reader().for_each(|entry| {
            if entry.counter_id == id {
                descriptor = Some(entry.clone());
            }
        });
        let descriptor = descriptor.expect("a counter");

        assert_eq!(
            counter_position::channel_type_id::SEND_CHANNEL_STATUS,
            descriptor.type_id
        );
        assert_eq!(77, descriptor.registration_id);
        assert_eq!(
            "snd-channel: aeron:udp?endpoint=127.0.0.1:40123",
            descriptor.label
        );

        // Its key is the channel's length and the channel
        // (`aeron_channel_endpoint_status_key_layout_t`).
        let uri = "aeron:udp?endpoint=127.0.0.1:40123";
        let key = regions.reader().key(id).expect("a key");
        assert_eq!(
            i32::try_from(uri.len()).expect("small"),
            i32::from_le_bytes(key[..4].try_into().expect("four bytes"))
        );
        assert_eq!(&key[4..4 + uri.len()], uri.as_bytes());

        // The endpoint is up the moment it is registered, which is what a
        // client reads: the counter is `ACTIVE` and not `INITIALIZING`, because
        // by the time anything can look it up the socket exists.
        assert_eq!(
            Some(counter_position::channel_status::ACTIVE),
            counters.value(&regions, id)
        );
    }

    #[test]
    fn a_closing_endpoint_is_not_handed_out() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        let outcome = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123"),
                &defaults(),
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");

        endpoints.get_mut(outcome.id()).expect("it is there").status = EndpointStatus::Closing;

        let error = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123"),
                &defaults(),
                &mut counters,
                &regions,
                8,
                2,
                2,
            )
            .expect_err("refused");

        assert!(matches!(error, EndpointError::Closing), "{error}");
        assert_eq!(
            deepmsg_cnc::command::ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
            error.error_code()
        );
    }

    #[test]
    fn an_endpoint_is_collected_only_when_it_is_idle_and_unused() {
        let entry = SendChannelEndpointEntry {
            id: 1,
            channel: channel("aeron:udp?endpoint=127.0.0.1:40123"),
            channel_status_counter_id: 0,
            status: EndpointStatus::Active,
            refcount: 1,
            sender_released: false,
            time_of_last_activity_ns: 1_000,
        };

        assert!(!SendChannelEndpoints::is_collectable(&entry, 2_000, 500));

        let mut idle = entry;
        idle.refcount = 0;
        assert!(!SendChannelEndpoints::is_collectable(&idle, 1_200, 500));
        assert!(SendChannelEndpoints::is_collectable(&idle, 1_500, 500));
    }
}
