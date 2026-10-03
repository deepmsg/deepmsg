//! The send channel endpoints a driver owns, one per channel that shares.
//!
//! Mirrors the conductor's half of
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! `get_or_add_send_channel_endpoint` (`:1961-2030`), the two lookups that
//! decide whether a channel *is* an endpoint that exists
//! (`find_existing_send_channel_endpoint`, `:224-286`), the agreement a shared
//! one has to reach (`validate_channel_against_send_channel_endpoint`,
//! `:1913-1959`) and the timeout that collects an endpoint nobody publishes
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
//! Its socket buffer sizes and its MTU (`:1913-1959`): the second channel is
//! not making a socket, it is using one, and a `so-rcvbuf=` that disagrees with
//! the socket that exists is a channel whose configuration would be silently
//! ignored. The reference refuses it and so does this.
//!
//! Every one of those checks measures the arriving channel against the value
//! the endpoint **adopted** when it was made — never against the arriving
//! channel's own parameters, which would make the comparison a channel against
//! itself. That is why [`SendChannelEndpointEntry`] carries the two buffer
//! lengths the socket was opened with: the endpoint that knows them has been
//! moved to the sender by the time a second channel arrives.
//!
//! # Where the socket lives
//!
//! [`SendChannelEndpoint`] owns it, and the endpoint is *created here and
//! moved to the sender* (`aeron_driver_sender_proxy_on_add_endpoint`,
//! `:2015`): the conductor decides, the sender thread owns. The entry this
//! module keeps is the bookkeeping the conductor needs afterwards — the
//! counter id it announced, the reference count, and the state.

use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::channel_validation;
use crate::media::loss_generator::EveryNthDatagram;
use crate::media::send_endpoint::{self, EndpointStatus, SendChannelEndpoint};
use crate::port_manager::{PortRange, WildcardPortManager};
use crate::sys;
use crate::udp_channel::{INVALID_TAG, UdpChannel};

/// One endpoint, as the conductor sees it.
#[derive(Debug)]
pub struct SendChannelEndpointEntry {
    /// The id the sender thread knows this endpoint by.
    pub id: u64,
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// Where it sends **now**, which is the channel's address until a
    /// re-resolution moves it (`current_data_addr`,
    /// `media/aeron_send_channel_endpoint.h:52`).
    ///
    /// The conductor keeps its own copy because the endpoint itself lives on
    /// the sender's thread, and this is what a tag match compares against: the
    /// reference hands `aeron_udp_channel_matches_tag` the endpoint's *current*
    /// address as an override
    /// (`aeron_udp_channel_endpoints_match_with_override`'s `remote_address`,
    /// `media/aeron_udp_channel.c:31-45`, passed from
    /// `aeron_driver_conductor_find_existing_send_channel_endpoint`). Without
    /// it a publication that names a re-resolved endpoint by tag is refused
    /// with `matching tag … has mismatched endpoint` — which is what
    /// `NameReResolutionTest.shouldHandleTaggedPublication` caught.
    pub current_data_addr: SocketAddr,
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
    /// The `SO_RCVBUF` the socket was opened with: the creating channel's own
    /// number when it named one, the context's otherwise, and zero when
    /// neither did — which is the kernel's default
    /// (`aeron_send_channel_endpoint.c:100-103`).
    ///
    /// Kept here rather than read back off the endpoint because the endpoint
    /// has been *moved to the sender* by the time a second channel arrives,
    /// and because this is the value the reference compares against: what the
    /// endpoint adopted, not what the arriving channel says. A build that
    /// recomputed it from the arriving channel's parameters would be
    /// comparing a channel with itself (`:1936-1945`).
    pub socket_rcvbuf: usize,
    /// The `SO_SNDBUF`, likewise.
    pub socket_sndbuf: usize,
    /// The port the wildcard port manager is holding for this endpoint, and
    /// zero when it holds none
    /// (`aeron_send_channel_endpoint.managed_port`).
    ///
    /// On the entry rather than read back off the endpoint for the same reason
    /// the two buffer lengths are: the endpoint is on the sender's thread by
    /// the time the port has to go back.
    pub managed_port: u16,
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
    /// A channel parameter the endpoint cannot honour
    /// (`validate_channel_against_send_channel_endpoint`, `:1913-1959`).
    ///
    /// The message is the reference's own, verbatim, because it is what the
    /// client's `RegistrationException` carries — the check that produced it
    /// is [`crate::channel_validation`]'s business.
    ChannelValidation(String),
    /// The counter manager is full.
    NoCounter,
    /// The wildcard port manager had no port to give: every one in the range
    /// is spoken for (`aeron_wildcard_port_manager_allocate_open_port`,
    /// `aeron_port_manager.c:93-104`).
    ///
    /// The message is the manager's, and it travels verbatim for the same
    /// reason [`Self::ChannelValidation`]'s does: it is what a client that has
    /// run the driver out of ports reads in its `RegistrationException`
    /// (`WildcardPortManagerSystemTest.java:90`).
    Port(crate::port_manager::PortError),
    /// The socket would not bind, with the chain the reference records for it
    /// (`send_endpoint.rs::bind_report`).
    Bind(Box<deepmsg_cnc::error_log::ErrorReport>),
    /// The socket could not be opened or connected.
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
            Self::ChannelValidation(message) => f.write_str(message),
            Self::Port(error) => write!(f, "{error}"),
            Self::NoCounter => f.write_str("could not allocate the channel status counter"),
            Self::Bind(report) => f.write_str(report.text()),
            Self::Socket(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for EndpointError {}

/// The endpoints a driver sends through.
#[derive(Debug)]
pub struct SendChannelEndpoints {
    entries: Vec<SendChannelEndpointEntry>,
    next_id: u64,
    /// The loss the driver was configured to inject: one outgoing datagram
    /// in every `data_loss_drop_every` is withheld from every endpoint made
    /// from here on. `None` — the driver nobody configured — is every
    /// deployment.
    ///
    /// The reference keeps this on the driver *context*, as a supplier each
    /// endpoint's create calls
    /// (`aeron_driver_context.h:384-387`,
    /// `media/aeron_send_channel_endpoint.c:237-240`); the registry is what
    /// plays that part here.
    data_loss_drop_every: Option<u64>,
    /// `aeron.driver.connect`, set once at start-up: whether a send endpoint
    /// whose channel names an explicit endpoint connects its socket to it
    /// (`aeron_driver_context.c:527,668` read into `context->connect_enabled`,
    /// used at `media/aeron_send_channel_endpoint.c:89`).
    connect_enabled: bool,
    /// Which ports a publication whose channel named port zero is given
    /// (`context->sender_port_manager`, `aeron_driver_context.c:428-437`).
    ///
    /// The reference hangs it on the driver context because C has one object to
    /// hang it on; here it belongs to the thing that owns the endpoints the
    /// ports are for, which is also the only thing that ever touches the table
    /// — a port is taken when an endpoint is created and given back when the
    /// conductor has confirmed the sender let it go
    /// ([`Self::remove`]), both on the conductor's thread.
    sender_port_manager: WildcardPortManager,
}

impl Default for SendChannelEndpoints {
    fn default() -> Self {
        Self::new()
    }
}

impl SendChannelEndpoints {
    /// No endpoints.
    ///
    /// Not `const`, unlike [`ReceiveChannelEndpoints::new`]: the port manager's
    /// table is a `HashMap`, and a `HashMap` has a random seed to pick.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
            data_loss_drop_every: None,
            connect_enabled: crate::config::DRIVER_CONNECT_DEFAULT,
            sender_port_manager: WildcardPortManager::sender(),
        }
    }

    /// The range a publication whose channel named port zero is given one out
    /// of (`aeron_wildcard_port_manager_set_range`,
    /// `aeron_port_manager.c:58-65`), set once at start-up from the driver's
    /// settings.
    pub fn set_port_range(&mut self, range: PortRange) {
        self.sender_port_manager.set_range(range);
    }

    /// Withhold one outgoing datagram in every `drop_every` from the
    /// endpoints made here from now on.
    ///
    /// The reference's counterpart is
    /// `aeron_driver_context_set_send_channel_loss_supplier`
    /// (`aeron-driver/src/main/c/aeron_driver_context.c:2979-2989`):
    /// set once, at start-up, before anything can be created. Each endpoint
    /// gets its own generator, so each counts its own datagrams — which is
    /// what the reference's per-endpoint attach does too.
    pub fn attach_data_loss_generator(&mut self, drop_every: u64) {
        self.data_loss_drop_every = Some(drop_every);
    }

    /// Whether the endpoints made from here on connect their sockets to the
    /// endpoint their channel names (`aeron.driver.connect`).
    ///
    /// Set once at start-up, like the port range beside it: it is the context's
    /// (`aeron_driver_context.c:527`), and an endpoint made before it changed
    /// would have been made with the old answer.
    pub fn set_connect_enabled(&mut self, connect_enabled: bool) {
        self.connect_enabled = connect_enabled;
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
                if matches_tag(channel, &entry.channel, entry.current_data_addr)? {
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
    /// `config` is the driver's context, which is where the MTU chain ends
    /// when neither the endpoint nor the channel named a send buffer, and
    /// `mtu_length` is the publication's `mtu=`, its 32-byte frame header
    /// included (`params->mtu_length`, `:1925`).
    ///
    /// # Errors
    ///
    /// [`EndpointError`] for a channel nothing can be built from, a closing
    /// endpoint, a tag or parameter disagreement, or a socket that will not
    /// open.
    #[allow(clippy::too_many_arguments)] // the collaborators a create needs
    pub fn get_or_add(
        &mut self,
        channel: UdpChannel,
        params: &crate::media::TransportParams,
        config: &crate::config::DriverConfig,
        mtu_length: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ns: i64,
        now_ms: i64,
    ) -> Result<EndpointOutcome, EndpointError> {
        #[allow(clippy::cast_sign_loss)] // a buffer length is not negative
        let context_socket_sndbuf = config.socket_so_sndbuf.max(0) as usize;

        // The kernel's own buffers, which is the last arm of the MTU chain and
        // the value `default_so_sndbuf` holds in the reference
        // (`aeron_driver_context.c:1315-1320`, read at `:1930`). A host the
        // probe cannot ask leaves zeroes, and the arm is then simply absent.
        let os_default_socket_sndbuf = sys::default_socket_buffers()
            .map_or(0, |lengths| usize::try_from(lengths.sndbuf).unwrap_or(0));

        if let Some(id) = self.find(&channel)? {
            let entry = self.get(id).expect("just found");
            validate_against_endpoint(
                &channel,
                entry,
                context_socket_sndbuf,
                mtu_length,
                os_default_socket_sndbuf,
            )?;

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

        // `:1983-1992`: an endpoint that does not exist yet has adopted
        // nothing, so the chain starts at the channel.
        channel_validation::validate_mtu_for_sndbuf(
            mtu_length,
            0,
            channel.socket_sndbuf_length,
            context_socket_sndbuf,
            os_default_socket_sndbuf,
        )
        .map_err(EndpointError::ChannelValidation)?;

        let mut endpoint = SendChannelEndpoint::create(
            channel,
            &mut self.sender_port_manager,
            params,
            counters,
            regions,
            registration_id,
            self.connect_enabled,
            now_ms,
            now_ns,
        )
        .map_err(|error| match error {
            send_endpoint::SendEndpointError::NoCounter => EndpointError::NoCounter,
            send_endpoint::SendEndpointError::Bind(report) => EndpointError::Bind(report),
            send_endpoint::SendEndpointError::Socket(error) => EndpointError::Socket(error),
            send_endpoint::SendEndpointError::Port(error) => EndpointError::Port(error),
        })?;

        // The supplier's call (`media/aeron_send_channel_endpoint.c:237-240`):
        // a fresh generator per endpoint, so each counts its own datagrams.
        if let Some(drop_every) = self.data_loss_drop_every {
            endpoint.set_data_loss_generator(Box::new(EveryNthDatagram::new(drop_every)));
        }

        // The status the counter holds: `ACTIVE` once the socket is there,
        // which is what a client reads to answer "is this channel's socket up
        // yet" (`aeron_driver_conductor.c:2013`,
        // `aeron_counter_set_release(endpoint->channel_status.value_addr,
        // AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE)`).
        endpoint.set_status(counters, regions, EndpointStatus::Active);

        let id = self.next_id;
        self.next_id += 1;

        let channel_status_counter_id = endpoint.channel_status_counter_id();

        // What the socket was opened with, read off the endpoint before the
        // sender takes it: `aeron_send_channel_endpoint_create` writes these
        // two fields for exactly this purpose (`:100-103`) and the socket
        // itself is the authority on what it got.
        let socket_rcvbuf = endpoint.socket_rcvbuf;
        let socket_sndbuf = endpoint.socket_sndbuf;

        self.entries.push(SendChannelEndpointEntry {
            id,
            channel: endpoint.channel.clone(),
            current_data_addr: endpoint.remote_data_addr(),
            channel_status_counter_id,
            status: EndpointStatus::Active,
            refcount: 0,
            sender_released: false,
            time_of_last_activity_ns: now_ns,
            socket_rcvbuf,
            socket_sndbuf,
            managed_port: endpoint.managed_port(),
        });

        Ok(EndpointOutcome::Created {
            id,
            channel_status_counter_id,
            endpoint: Box::new(endpoint),
        })
    }

    /// Count a publication against the endpoint it sends through
    /// (`AERON_DRIVER_MANAGED_RESOURCE_INCREF` on the endpoint,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:4591`, once per
    /// network publication created).
    ///
    /// The count is the whole of what keeps an endpoint alive while something
    /// still publishes through it: [`Self::begin_release`] refuses an endpoint
    /// whose count is above zero, and the reference collects one only when the
    /// count has reached zero *and* its last activity is older than the linger
    /// timeout (`:1533-1586`).
    ///
    /// This build had the matching decrement and no increment, which made the
    /// count negative on the first removal and tore a shared endpoint down —
    /// its socket, its `snd-channel` counter and the label on it — while other
    /// publications were still sending through it. The reference's own
    /// `aeron_send_channel_endpoint_add_publication` (`:440-452`) is the other
    /// half of this: it is the *sender's* dispatch map, and the sender owns
    /// that object here, so there is nothing for the conductor to do with it.
    pub fn attach_publication(&mut self, id: u64, now_ns: i64) {
        if let Some(entry) = self.get_mut(id) {
            #[allow(clippy::cast_possible_wrap)] // a publication count is small
            {
                entry.refcount += 1;
            }
            entry.time_of_last_activity_ns = now_ns;
        }
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

    /// Mark an endpoint as on its way out, and say whether *this* call is the
    /// one that did it
    /// (`AERON_DRIVER_MANAGED_RESOURCE_EVENT_DECREF`'s zero case,
    /// `media/aeron_send_channel_endpoint.c:323-330`: the last publication
    /// leaving marks it CLOSING and asks the sender to remove it).
    pub fn begin_release(&mut self, id: u64) -> bool {
        let Some(entry) = self.get_mut(id) else {
            return false;
        };

        if entry.status != EndpointStatus::Active || entry.refcount > 0 {
            return false;
        }

        entry.status = EndpointStatus::Closing;

        true
    }

    /// Forget an endpoint once both sides have let it go, and give its port
    /// back.
    ///
    /// This is where the reference gives it back too, one level down:
    /// `aeron_send_channel_endpoint_delete` frees the managed port with the
    /// counters and the socket (`media/aeron_send_channel_endpoint.c:250-281`),
    /// and the conductor reaches the delete when the sender has confirmed the
    /// endpoint is gone (`aeron_send_channel_endpoint_entry_has_reached_end_of_life`).
    /// Giving it back any earlier would hand a port to a second channel while
    /// the first still has it bound.
    pub fn remove(&mut self, id: u64) -> Option<SendChannelEndpointEntry> {
        let index = self.entries.iter().position(|entry| entry.id == id)?;
        let entry = self.entries.swap_remove(index);

        self.sender_port_manager
            .free_managed_port(entry.managed_port);

        Some(entry)
    }
}

/// Whether a new channel answers to an existing endpoint's tag
/// (`aeron_udp_channel_matches_tag`,
/// `aeron-driver/src/main/c/media/aeron_udp_channel.c:564-620`).
///
/// Both tags have to be named and equal, the arriving channel's control mode
/// has to **not disagree**, and the addresses have to agree — a channel whose
/// addresses are the wildcard matches anything, which is
/// [`is_wildcard`]'s escape.
///
/// The second rule is the one worth reading twice, because it is asymmetric
/// and it is stated in terms of the *arriving* channel
/// (`aeron_udp_channel_control_modes_match`, `media/aeron_udp_channel.h:104-107`):
///
/// ```c
/// AERON_UDP_CHANNEL_CONTROL_MODE_NONE == channel->control_mode ||
/// channel->control_mode == other->control_mode
/// ```
///
/// So `aeron:udp?tags=N`, which names no control mode, joins an endpoint in
/// *any* mode — the channel is letting the endpoint say how it is controlled.
/// A channel that names a mode gets it compared, and is refused when the two
/// differ (`shouldNotAllowNormalToControlModeDynamicChangeWithTags` and its
/// siblings). Reading the rule as "the modes are equal" refuses the first case
/// and is why this clause was wrong.
///
/// # Errors
///
/// [`EndpointError::TagMismatch`] when the tags are equal and something else
/// is not.
fn matches_tag(
    channel: &UdpChannel,
    existing: &UdpChannel,
    existing_data_addr: SocketAddr,
) -> Result<bool, EndpointError> {
    if channel.tag_id == INVALID_TAG
        || existing.tag_id == INVALID_TAG
        || channel.tag_id != existing.tag_id
    {
        return Ok(false);
    }

    if channel.control_mode != crate::udp_channel::ControlMode::None
        && channel.control_mode != existing.control_mode
    {
        return Err(EndpointError::TagMismatch {
            tag: channel.tag_id,
        });
    }

    // A channel whose addresses are the wildcard matches whatever the
    // endpoint is (`aeron_udp_channel_endpoints_match_with_override`'s first
    // two lines, `:41-45`).
    if is_wildcard(channel) {
        return Ok(true);
    }

    if channel.remote_data != existing_data_addr || channel.local_data != existing.local_data {
        return Err(EndpointError::TagMismatch {
            tag: channel.tag_id,
        });
    }

    Ok(true)
}

/// `aeron_udp_channel_is_wildcard` (`media/aeron_udp_channel.h:98-102`): both
/// of a channel's data addresses are the wildcard.
///
/// Read off the **resolved** addresses rather than off whether the URI named
/// them, which is the distinction that matters: `endpoint=0.0.0.0:0` names an
/// endpoint and is still the wildcard, and the reference compares the
/// addresses because that is what it has.
fn is_wildcard(channel: &UdpChannel) -> bool {
    let wildcard =
        |address: &std::net::SocketAddr| address.ip().is_unspecified() && address.port() == 0;

    wildcard(&channel.remote_data) && wildcard(&channel.local_data)
}

/// The three checks a channel must pass before it may share an endpoint
/// (`aeron_driver_conductor_validate_channel_against_send_channel_endpoint`,
/// `:1913-1959`).
///
/// Each one measures the arriving channel against what the endpoint **has**,
/// never against what the arriving channel asks for: the two are only equal by
/// accident, and the whole point of the check is the case where they are not.
///
/// # Errors
///
/// [`EndpointError::ChannelValidation`] carrying the reference's own message —
/// the client reads it, so it is not paraphrased.
fn validate_against_endpoint(
    channel: &UdpChannel,
    entry: &SendChannelEndpointEntry,
    context_socket_sndbuf: usize,
    mtu_length: usize,
    os_default_socket_sndbuf: usize,
) -> Result<(), EndpointError> {
    // `:1925-1934`: a frame has to fit the buffer that carries it. The
    // endpoint comes first because it is the number the socket actually has,
    // and each arm names itself in the message.
    channel_validation::validate_mtu_for_sndbuf(
        mtu_length,
        entry.socket_sndbuf,
        channel.socket_sndbuf_length,
        context_socket_sndbuf,
        os_default_socket_sndbuf,
    )
    .map_err(EndpointError::ChannelValidation)?;

    // `:1936-1945` and `:1947-1956`: the buffers the socket already has. A
    // channel that names none agrees with anything — it is asking for the
    // socket that exists, which is what makes `so-sndbuf=` optional on every
    // channel after the first.
    for (param, named, adopted) in [
        (
            "so-rcvbuf",
            channel.socket_rcvbuf_length,
            entry.socket_rcvbuf,
        ),
        (
            "so-sndbuf",
            channel.socket_sndbuf_length,
            entry.socket_sndbuf,
        ),
    ] {
        channel_validation::validate_channel_buffer_length(
            param,
            named,
            adopted,
            &channel.original_uri,
            &entry.channel.original_uri,
        )
        .map_err(EndpointError::ChannelValidation)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::config::DriverConfig;
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

    /// The transport parameters the driver would open this channel with:
    /// the channel's own numbers where it named one, the context's otherwise
    /// (`network_publications::transport_params`). This is also what an
    /// endpoint *adopts*, which is the value the second channel is measured
    /// against.
    fn params(uri: &str) -> TransportParams {
        crate::network_publications::transport_params(&DriverConfig::default(), &channel(uri))
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
                &DriverConfig::default(),
                0,
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
                &DriverConfig::default(),
                0,
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

    /// A publication whose channel named port zero is given one out of the
    /// driver's range, and gives it back when the sender has let it go.
    ///
    /// The three moments the oracle walks
    /// (`WildcardPortManagerSystemTest.java:98-113`, the publication half): a
    /// channel that named zero gets the range's port, a second one is refused
    /// while the first holds it, and the port comes round again once the first
    /// is gone — which is the whole of what makes a managed range a range
    /// rather than a leak.
    #[test]
    fn a_channel_that_named_no_port_is_given_one_until_it_is_gone() {
        const MANAGED: u16 = 40300;

        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();
        endpoints.set_port_range(PortRange {
            low: MANAGED,
            high: MANAGED,
        });

        // A dynamic sender names a `control=`, which is the only shape of
        // sender the manager gives a port to
        // (`aeron_port_manager.c:142-159`).
        let first = endpoints
            .get_or_add(
                channel("aeron:udp?control=127.0.0.1:0|control-mode=dynamic"),
                &defaults(),
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");

        let first_id = first.id();

        assert_eq!(
            Some(MANAGED),
            endpoints.get(first_id).map(|entry| entry.managed_port)
        );

        // A *different* channel — a different wildcard address — is a
        // different endpoint, and the one port in the range is spoken for.
        let refused = endpoints.get_or_add(
            channel("aeron:udp?control=0.0.0.0:0|control-mode=dynamic"),
            &defaults(),
            &DriverConfig::default(),
            0,
            &mut counters,
            &regions,
            8,
            2,
            2,
        );

        assert_eq!(
            "no available ports in range 40300 40300",
            refused.expect_err("a full range").to_string()
        );
        assert_eq!(1, endpoints.entries().len(), "a refusal adds nothing");

        // The sender has let the first one go — the socket with it, which is
        // the order the conductor is told in (`SenderEvent::EndpointRemoved`,
        // handled by `release_send_endpoint`) and the reason the port is freed
        // *there* rather than when the endpoint is first marked CLOSING.
        drop(first);

        assert!(endpoints.remove(first_id).is_some());

        let second = endpoints
            .get_or_add(
                channel("aeron:udp?control=0.0.0.0:0|control-mode=dynamic"),
                &defaults(),
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                9,
                3,
                3,
            )
            .expect("the port came back");

        assert_eq!(
            Some(MANAGED),
            endpoints.get(second.id()).map(|entry| entry.managed_port)
        );
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
                &DriverConfig::default(),
                0,
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
                &DriverConfig::default(),
                0,
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
                &DriverConfig::default(),
                0,
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
                &DriverConfig::default(),
                0,
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

    /// The three publications of `shouldAllowDynamicControlModeWithTags`,
    /// which is the case that was red: a channel that names **no control mode**
    /// joins an endpoint in any mode, because it is letting the endpoint say
    /// how the channel is controlled (`aeron_udp_channel_control_modes_match`,
    /// `media/aeron_udp_channel.h:104-107`).
    ///
    /// Each of the `control=` tests below binds its **own** port. A dynamic
    /// endpoint binds the address `control=` names, and the tests in a binary
    /// run in parallel — three of them sharing `23454` is `AddrInUse` two runs
    /// in five, which is how the first version of this arrived.
    #[test]
    fn a_channel_that_names_no_control_mode_joins_one_that_does() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();
        let config = DriverConfig::default();

        let dynamic = "aeron:udp?control-mode=dynamic|control=localhost:23454|tags=200";
        const BARE: &str = "aeron:udp?tags=200";

        let first = endpoints
            .get_or_add(
                channel(dynamic),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                1,
                1,
                1,
            )
            .expect("an endpoint");
        let second = endpoints
            .get_or_add(
                channel(dynamic),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                2,
                2,
                2,
            )
            .expect("the same channel");
        let third = endpoints
            .get_or_add(
                channel(BARE),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                3,
                3,
                3,
            )
            .expect("a channel that named no control mode");

        assert_eq!(first.id(), second.id());
        assert_eq!(
            first.id(),
            third.id(),
            "naming no control mode is not naming a different one"
        );
        assert_eq!(1, endpoints.entries().len());
    }

    /// The other direction, which must stay refused: a channel that *does*
    /// name a mode is compared against the endpoint's.
    #[test]
    fn a_channel_that_names_a_control_mode_is_compared_against_the_endpoints() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();
        let config = DriverConfig::default();

        endpoints
            .get_or_add(
                channel("aeron:udp?control=localhost:23464|endpoint=localhost:23465|tags=200"),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                1,
                1,
                1,
            )
            .expect("an endpoint");

        let error = endpoints
            .get_or_add(
                channel("aeron:udp?control-mode=dynamic|control=localhost:23464|tags=200"),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                2,
                2,
                2,
            )
            .expect_err("refused");

        assert!(
            matches!(error, EndpointError::TagMismatch { tag: 200 }),
            "{error}"
        );
    }

    /// And the address clause is a clause of its own: a channel that names no
    /// control mode is still refused when it names a *different* address, so
    /// the refusal above is not the mode check firing on everything.
    #[test]
    fn a_channel_that_names_no_control_mode_is_still_refused_on_a_different_address() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();
        let config = DriverConfig::default();

        endpoints
            .get_or_add(
                channel("aeron:udp?control-mode=dynamic|control=localhost:23474|tags=200"),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                1,
                1,
                1,
            )
            .expect("an endpoint");

        let error = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=localhost:23475|tags=200"),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                2,
                2,
                2,
            )
            .expect_err("refused");

        assert!(
            matches!(error, EndpointError::TagMismatch { tag: 200 }),
            "{error}"
        );
    }

    /// `is_wildcard` reads the **resolved** addresses, not whether the URI
    /// named them: `endpoint=0.0.0.0:0` names one and is still the wildcard
    /// (`media/aeron_udp_channel.h:98-102`).
    #[test]
    fn a_wildcard_address_is_the_wildcard_even_when_it_was_named() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();
        let config = DriverConfig::default();

        let first = endpoints
            .get_or_add(
                channel("aeron:udp?control-mode=dynamic|control=localhost:23484|tags=7"),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                1,
                1,
                1,
            )
            .expect("an endpoint");

        let second = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=0.0.0.0:0|tags=7"),
                &defaults(),
                &config,
                0,
                &mut counters,
                &regions,
                2,
                2,
                2,
            )
            .expect("the wildcard matches anything");

        assert_eq!(first.id(), second.id());
        assert_eq!(1, endpoints.entries().len());
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
                &DriverConfig::default(),
                0,
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
                    &DriverConfig::default(),
                    0,
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

        // `params` and not a bare `TransportParams::default()`: the endpoint
        // adopts what the socket was opened with, and a channel that named
        // `so-sndbuf=1m` is a socket that was opened with one. A test whose
        // parameters said zero would be testing a different endpoint from the
        // one the channel describes.
        const FIRST: &str = "aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=1m";
        const SECOND: &str = "aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=2m";

        endpoints
            .get_or_add(
                channel(FIRST),
                &params(FIRST),
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                7,
                1,
                1,
            )
            .expect("an endpoint");

        let error = endpoints
            .get_or_add(
                channel(SECOND),
                &params(SECOND),
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                8,
                2,
                2,
            )
            .expect_err("refused");

        // The reference's sentence, both channels named: the arriving channel
        // is measured against the endpoint's *adopted* 1m, and a build that
        // measured it against the arriving channel's own parameters would
        // find 2m equal to 2m and let it through.
        assert!(
            matches!(
                error,
                EndpointError::ChannelValidation(ref message) if message ==
                    "so-sndbuf=2097152 does not match existing value of 1048576: \
                     existingChannel=aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=1m \
                     channel=aeron:udp?endpoint=127.0.0.1:40123|so-sndbuf=2m"
            ),
            "{error}"
        );

        // Naming none is not a disagreement: it is the socket that exists.
        assert!(
            endpoints
                .get_or_add(
                    channel("aeron:udp?endpoint=127.0.0.1:40123"),
                    &defaults(),
                    &DriverConfig::default(),
                    0,
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
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                77,
                1,
                1,
            )
            .expect("an endpoint");

        let EndpointOutcome::Created { endpoint, .. } = &outcome else {
            panic!("the first channel creates the endpoint");
        };
        let bound = endpoint.local_address().expect("a bound address");
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
        // The label is the channel **and the address the socket was bound to**
        // (`aeron_position.c:229-244`, called from
        // `media/aeron_send_channel_endpoint.c:202-210`). The channel named no
        // local port, so the kernel chose one, and the label is where a reader
        // finds which.
        let uri = "aeron:udp?endpoint=127.0.0.1:40123";
        assert_eq!(format!("snd-channel: {uri} {bound}"), descriptor.label);

        // Its key is the channel's length and the channel
        // (`aeron_channel_endpoint_status_key_layout_t`).
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
    fn the_local_sockaddr_counter_is_the_address_the_socket_was_bound_to() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        let outcome = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123"),
                &defaults(),
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                78,
                1,
                1,
            )
            .expect("an endpoint");

        let EndpointOutcome::Created { endpoint, .. } = &outcome else {
            panic!("the first channel creates the endpoint");
        };

        let id = endpoint.local_sockaddr_counter_id();
        let mut descriptor = None;
        regions.reader().for_each(|entry| {
            if entry.counter_id == id {
                descriptor = Some(entry.clone());
            }
        });
        let descriptor = descriptor.expect("a counter");

        // Type 14, registered to this endpoint
        // (`aeron_counter_local_sockaddr_indicator_allocate`,
        // `aeron_position.c:276-311`).
        assert_eq!(counter_position::LOCAL_SOCKADDR_TYPE_ID, descriptor.type_id);
        assert_eq!(78, descriptor.registration_id);

        // Its value is the endpoint's state, as the channel status's is: the
        // counter being there is the news.
        assert_eq!(
            Some(counter_position::channel_status::ACTIVE),
            counters.value(&regions, id)
        );

        // And its **key** is the address — which is the whole reason it
        // exists, because this channel names no local port and the kernel
        // chose one. The layout is the channel status's id, the length, then
        // the text (`aeron_local_sockaddr_key_layout_t`).
        let bound = endpoint
            .local_address()
            .expect("a bound address")
            .to_string();

        // The socket's address, not the channel's: the channel's `endpoint` is
        // where this endpoint *sends*, and it is a different address entirely.
        assert_ne!("127.0.0.1:40123", bound);

        let key = regions.reader().key(id).expect("a key");
        assert_eq!(
            outcome.channel_status_counter_id().to_le_bytes().as_slice(),
            &key[..4]
        );
        assert_eq!(
            i32::try_from(bound.len()).expect("small"),
            i32::from_le_bytes(key[4..8].try_into().expect("four bytes"))
        );
        assert_eq!(
            bound,
            String::from_utf8_lossy(&key[8..8 + bound.len()]).to_string()
        );

        // The label says the same thing in text, with the channel status id
        // first (`aeron_position.c:295-297`).
        assert_eq!(
            format!(
                "snd-local-sockaddr: {} {bound}",
                outcome.channel_status_counter_id()
            ),
            descriptor.label
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
                &DriverConfig::default(),
                0,
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
                &DriverConfig::default(),
                0,
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
            current_data_addr: "127.0.0.1:40123".parse().expect("an address"),
            channel_status_counter_id: 0,
            status: EndpointStatus::Active,
            refcount: 1,
            sender_released: false,
            time_of_last_activity_ns: 1_000,
            socket_rcvbuf: 0,
            socket_sndbuf: 0,
            managed_port: 0,
        };

        assert!(!SendChannelEndpoints::is_collectable(&entry, 2_000, 500));

        let mut idle = entry;
        idle.refcount = 0;
        assert!(!SendChannelEndpoints::is_collectable(&idle, 1_200, 500));
        assert!(SendChannelEndpoints::is_collectable(&idle, 1_500, 500));
    }

    /// The count a publication puts on the endpoint it sends through, and the
    /// whole reason it is there: an endpoint something still publishes through
    /// is not collected, however many publications have already left it
    /// (`AERON_DRIVER_MANAGED_RESOURCE_INCREF` on the endpoint,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:4591`, against
    /// `begin_release`'s zero case,
    /// `media/aeron_send_channel_endpoint.c:323-330`).
    ///
    /// This build had the decrement and not the increment: the count went
    /// negative at the first removal, so the endpoint went out from under the
    /// publications still on it — its socket, its `snd-channel` counter, and
    /// the label a reader finds the channel and the bound port in
    /// (`io.aeron.ResponseChannelsTest::shouldCreateNewSendChannelWithoutPrototype`).
    #[test]
    fn an_endpoint_lives_until_the_last_publication_on_it_has_gone() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = SendChannelEndpoints::new();

        let id = endpoints
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40123"),
                &defaults(),
                &DriverConfig::default(),
                0,
                &mut counters,
                &regions,
                77,
                1,
                1,
            )
            .expect("an endpoint")
            .id();

        // Two publications on one endpoint, which is the shape a response
        // channel has: a prototype and the session publication that follows it.
        endpoints.attach_publication(id, 2);
        endpoints.attach_publication(id, 2);

        // The first leaves, and the endpoint is not a candidate — once for the
        // one still on it, and once more because the count did not move.
        assert!(endpoints.detach_publication(id));
        assert!(!endpoints.begin_release(id), "one publication is on it");
        assert!(
            !endpoints.begin_release(id),
            "and the count is still above zero"
        );

        // The second leaves, and now it is.
        assert!(endpoints.detach_publication(id));
        assert!(
            endpoints.begin_release(id),
            "nothing is publishing through it"
        );
    }
}
