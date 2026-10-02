//! The receive channel endpoints a driver owns, one per channel that shares.
//!
//! Mirrors the conductor's other half of
//! `aeron-driver/src/main/c/aeron_driver_conductor.c`:
//! `get_or_add_receive_channel_endpoint` (`:2046-2115`) and the lookups that
//! decide whether a subscription joins an endpoint that exists
//! (`find_existing_receive_channel_endpoint`, `:288-340`).
//!
//! The sharing rule is the send side's, for the same reason: two subscriptions
//! to one channel are one socket, and a driver that made two would read every
//! datagram twice and answer each of them twice. The difference is what the
//! *channel* means on this side. A send endpoint binds the interface; a receive
//! endpoint binds the endpoint parameter — so `aeron:udp?endpoint=localhost:40123`
//! and `aeron:udp?endpoint=127.0.0.1:40123` are one receiving socket, and a
//! subscription that names `localhost:40124` is a *different* one even though
//! it is the same publisher's port as its counterpart's.
//!
//! # Reference counts
//!
//! An endpoint lives while something reads through it: the count here is
//! subscriptions, and the images an endpoint serves are counted separately —
//! an endpoint with no subscription but a live image is still an endpoint a
//! client is reading (`image_ref_count`,
//! `aeron-driver/src/main/c/media/aeron_receive_channel_endpoint.h:52-56`).

use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::channel_validation;
use crate::media::receive_endpoint::{
    EndpointStatus, ReceiveChannelEndpoint, ReceiveEndpointError,
};
use crate::sys;
use crate::udp_channel::{ControlMode, INVALID_TAG, UdpChannel};

/// One endpoint, as the conductor sees it.
#[derive(Debug)]
pub struct ReceiveChannelEndpointEntry {
    /// The id the receiver thread knows this endpoint by.
    pub id: u64,
    /// The channel it was created for.
    pub channel: UdpChannel,
    /// Where the endpoint's **own destination** answers control frames now —
    /// its channel's `local_control` until a re-resolution moves it
    /// (`current_control_addr` of the one destination whose channel is the
    /// endpoint's own and which named an explicit control,
    /// `media/aeron_receive_channel_endpoint.c:668-689`).
    ///
    /// It is here rather than read off the endpoint because the endpoint itself
    /// has been moved to the receiver by the time a second subscription
    /// arrives, and it is the address a **tag match** is measured against:
    /// a subscription that names the same endpoint by tag after its control
    /// name resolved somewhere else must join it, not open a second one.
    pub control_addr: Option<SocketAddr>,
    /// The `rcv-channel` counter whose value is its state.
    pub channel_status_counter_id: i32,
    /// Where it is in its life.
    pub status: EndpointStatus,
    /// How many subscriptions read through it.
    pub refcount: i32,
    /// How many images it serves (`image_ref_count`).
    pub image_refcount: i32,
    /// Whether the receiver thread has already let it go.
    pub receiver_released: bool,
    /// The `SO_RCVBUF` the socket was opened with: the creating channel's own
    /// number when it named one, the context's otherwise, and zero when
    /// neither did (`aeron_udp_channel_socket_so_rcvbuf`,
    /// `media/aeron_udp_channel.c:629-632`).
    ///
    /// The endpoint itself carries these too, but it has been *moved to the
    /// receiver* by the time a second subscription arrives — and this is the
    /// value the arriving channel is measured against, not its own.
    pub socket_rcvbuf: usize,
    /// The `SO_SNDBUF`, likewise.
    pub socket_sndbuf: usize,
    /// How many destinations the endpoint has (`destinations.length`,
    /// `aeron_driver_conductor.c:2184`): one for a channel that named an
    /// endpoint, none for a `control-mode=manual` one (`:2099-2114`), and it
    /// grows as an MDS subscription adds them.
    ///
    /// Counted here because the agreement check is gated on it and the
    /// endpoint that owns the list is on the receiver's thread. An endpoint
    /// that has gained destinations is no longer the shape it was made in, so
    /// a channel joining it is not asking for the socket that exists.
    pub destination_count: usize,
}

/// Why a receive endpoint could not be had.
#[derive(Debug)]
pub enum ReceiveEndpointErrorKind {
    /// The counter manager is full.
    NoCounter,
    /// The socket could not be opened or bound — most often because another
    /// process already holds the port, which for a subscriber is the ordinary
    /// case of two clients naming the same channel.
    Socket(std::io::Error),
    /// A channel parameter the endpoint cannot honour
    /// (`aeron_driver_conductor.c:2116-2223`).
    ///
    /// The message is the reference's own, verbatim, because it is what the
    /// client's `RegistrationException` carries — the checks that produce it
    /// are [`crate::channel_validation`]'s business.
    ChannelValidation(String),
    /// The **bind** failed. The composition arrives already started — the
    /// syscall, the transport and the destination each wrote a line — and this
    /// layer adds the correlation id the reference names here
    /// (`aeron_driver_conductor.c:2110`).
    Bind(deepmsg_cnc::error_log::ErrorReport),
}

impl std::fmt::Display for ReceiveEndpointErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCounter => f.write_str("could not allocate the receive channel status counter"),
            Self::Socket(error) => write!(f, "{error}"),
            Self::ChannelValidation(message) => f.write_str(message),
            Self::Bind(report) => f.write_str(report.text()),
        }
    }
}

impl std::error::Error for ReceiveEndpointErrorKind {}

/// The endpoints a driver receives through, and the receiver ids they hold.
/// The id the first receiver of a driver introduces itself with
/// (`aeron_driver_context.c:1306-1313`).
fn first_receiver_id() -> i64 {
    loop {
        let id = i64::from(sys::random_i32()) * i64::from(sys::random_i32());

        if 0 != id {
            return id;
        }
    }
}

#[derive(Debug)]
pub struct ReceiveChannelEndpoints {
    entries: Vec<ReceiveChannelEndpointEntry>,
    next_id: u64,
    /// The id the next endpoint introduces itself with
    /// (`context->next_receiver_id++`,
    /// `aeron-driver/src/main/c/media/aeron_receive_channel_endpoint.c:92`).
    next_receiver_id: i64,
}

impl Default for ReceiveChannelEndpoints {
    fn default() -> Self {
        Self::new()
    }
}

impl ReceiveChannelEndpoints {
    /// No endpoints, and the first receiver id.
    ///
    /// That id is **random and not one** (`aeron_driver_context.c:1303-1313`):
    /// the product of two randomised `int32`s, retried until it is not zero.
    /// A driver that started at one would hand out the same receiver ids as
    /// every other driver, and a sender that keeps its receivers keyed by that
    /// id — which is what a group strategy does
    /// (`aeron_min_flow_control.c:185`) — would take two readers on two drivers
    /// for one reader reporting twice.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
            next_receiver_id: first_receiver_id(),
        }
    }

    /// The endpoints, in the order they were created.
    pub fn entries(&self) -> &[ReceiveChannelEndpointEntry] {
        &self.entries
    }

    /// The entry for an id.
    pub fn get(&self, id: u64) -> Option<&ReceiveChannelEndpointEntry> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// The mutable entry for an id.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut ReceiveChannelEndpointEntry> {
        self.entries.iter_mut().find(|entry| entry.id == id)
    }

    /// The endpoint a channel canonicalises to, if there is one
    /// (`find_existing_receive_channel_endpoint`, `:288-340`).
    pub fn find(&self, channel: &UdpChannel) -> Option<u64> {
        // `:189-208`: a channel with a tag looks for the endpoint that already
        // answers to it, wherever its canonical form puts it — which is what
        // makes a tagged subscription join an endpoint whose control name has
        // since resolved somewhere else.
        if channel.tag_id != INVALID_TAG {
            for entry in &self.entries {
                if Self::matches_tag(channel, entry) {
                    return Some(entry.id);
                }
            }
        }

        let entry = self
            .entries
            .iter()
            .find(|entry| entry.channel.canonical_form == channel.canonical_form)?;

        // `:212-218`: two different, named tags are two endpoints, even on one
        // canonical form.
        if entry.channel.tag_id != INVALID_TAG
            && channel.tag_id != INVALID_TAG
            && channel.tag_id != entry.channel.tag_id
        {
            return None;
        }

        Some(entry.id)
    }

    /// Whether a tagged channel joins this endpoint
    /// (`aeron_receive_channel_endpoint_matches_tag`, `:668-689`, which hands
    /// `aeron_udp_channel_matches_tag` the endpoint's **current control address**
    /// as the local-side override).
    ///
    /// The two rules are the send side's, with the receiving half's addresses: a
    /// channel that named no address at all is the wildcard and matches whatever
    /// the endpoint is, and a channel that named one has to agree on both sides —
    /// its data address against the endpoint's, its control address against the one
    /// the endpoint's destination uses **now**.
    fn matches_tag(channel: &UdpChannel, entry: &ReceiveChannelEndpointEntry) -> bool {
        if channel.tag_id == INVALID_TAG
            || entry.channel.tag_id == INVALID_TAG
            || channel.tag_id != entry.channel.tag_id
        {
            return false;
        }

        if channel.control_mode != crate::udp_channel::ControlMode::None
            && channel.control_mode != entry.channel.control_mode
        {
            return false;
        }

        if Self::is_wildcard(channel) {
            return true;
        }

        let control_matches = entry.control_addr.map_or(
            channel.local_control == entry.channel.local_control,
            |addr| channel.local_control == addr,
        );

        channel.remote_data == entry.channel.remote_data && control_matches
    }

    /// `aeron_udp_channel_is_wildcard` (`media/aeron_udp_channel.h:98-102`): both of
    /// a channel's data addresses are the wildcard, which is what `aeron:udp?tags=`
    /// names.
    fn is_wildcard(channel: &UdpChannel) -> bool {
        channel.remote_data.ip().is_unspecified() && channel.local_data.ip().is_unspecified()
    }

    /// Create the endpoint for a channel, or find the one it shares.
    ///
    /// `initial_window_length` is `params->initial_window_length`: the
    /// subscription's `rcv-wnd=`, or the driver's own when it named none
    /// (`aeron_driver_uri.c:466`, `:502`). It is what the receive side's one
    /// *window* check measures, in place of the send side's MTU check.
    ///
    /// # Errors
    ///
    /// The socket's error, when one has to be opened and cannot be, and
    /// [`ReceiveEndpointErrorKind::ChannelValidation`] for a parameter the
    /// endpoint that exists cannot honour.
    #[allow(clippy::too_many_arguments)] // the collaborators a create needs
    pub fn get_or_add(
        &mut self,
        channel: UdpChannel,
        params: &crate::media::TransportParams,
        config: &crate::config::DriverConfig,
        initial_window_length: usize,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        now_ms: i64,
        now_ns: i64,
    ) -> Result<(u64, i32, Option<Box<ReceiveChannelEndpoint>>), ReceiveEndpointErrorKind> {
        // The kernel's own receive buffer, the arm the window check falls to
        // when the channel named no `so-rcvbuf` and the context has none
        // either (`context->os_buffer_lengths`, `aeron_driver_context.c:1315-1320`,
        // read at `aeron_driver_conductor.c:2119`).
        let os_default_socket_rcvbuf = usize::try_from(Self::os_defaults().rcvbuf).unwrap_or(0);

        if let Some(id) = self.find(&channel) {
            let entry = self.get(id).expect("just found");

            // `:2184`: the comparison is skipped for a `control-mode=manual`
            // channel and for an endpoint that no longer has exactly one
            // destination. A manual endpoint's destinations arrive one at a
            // time from clients, so it has no single socket shape to agree
            // with; and a channel that named none is asking for the socket
            // that exists rather than making one.
            if channel.control_mode != ControlMode::Manual && entry.destination_count == 1 {
                validate_against_endpoint(
                    &channel,
                    entry,
                    params.socket_rcvbuf,
                    params.socket_sndbuf,
                    initial_window_length,
                    os_default_socket_rcvbuf,
                )?;
            }

            return Ok((id, entry.channel_status_counter_id, None));
        }

        // `:2116-2130`: a window that does not fit the buffer the socket is
        // about to be opened with is refused before the socket is made. The
        // buffer is the arriving channel's own — at this point there is no
        // endpoint to have one.
        channel_validation::validate_initial_window_for_rcvbuf(
            initial_window_length,
            params.socket_rcvbuf,
            os_default_socket_rcvbuf,
            &channel.original_uri,
            None,
        )
        .map_err(ReceiveEndpointErrorKind::ChannelValidation)?;

        let receiver_id = self.next_receiver_id;
        self.next_receiver_id += 1;

        // `:105` reads the group tag before anything else is built, and takes
        // the channel's own `gtag=` when it named one: a channel that names
        // none is not a channel that named `-1`
        // (`aeron_receive_channel_endpoint_set_group_tag`, `:39-46`).
        let group_tag = channel.group_tag.or(config.receiver_group_tag);

        let endpoint = ReceiveChannelEndpoint::create(
            channel,
            group_tag,
            params,
            receiver_id,
            config.stream_session_limit,
            counters,
            regions,
            registration_id,
            now_ms,
            now_ns,
        )
        .map_err(|error| match error {
            ReceiveEndpointError::NoCounter => ReceiveEndpointErrorKind::NoCounter,
            ReceiveEndpointError::Socket(error) => ReceiveEndpointErrorKind::Socket(error),
            ReceiveEndpointError::Bind(mut report) => {
                // `:2110`: `AERON_APPEND_ERR("correlation_id=%" PRId64, …)`.
                report.append(
                    "aeron_driver_conductor_get_or_add_receive_channel_endpoint",
                    "aeron_driver_conductor.c",
                    2110,
                    &format!("correlation_id={registration_id}"),
                );
                ReceiveEndpointErrorKind::Bind(report)
            }
        })?;

        // The same status the send side writes, for the same reason: a
        // subscription's counter is how a client learns its socket is up
        // (`aeron_driver_conductor.c:2160`).
        endpoint.set_status(counters, regions, EndpointStatus::Active);

        let id = self.next_id;
        self.next_id += 1;

        let channel_status_counter_id = endpoint.channel_status_counter_id();

        // What the socket was opened with and how many destinations it
        // started with, read off the endpoint before the receiver takes it —
        // `aeron_receive_channel_endpoint_create` is given a destination for
        // every channel but a manual one (`aeron_driver_conductor.c:2099-2114`).
        let socket_rcvbuf = endpoint.socket_rcvbuf;
        let socket_sndbuf = endpoint.socket_sndbuf;
        let destination_count = endpoint.destination_count();

        // `:668-679`: the override is the endpoint's **own** destination's
        // current control address, and only when that destination is the one
        // the endpoint was made for and named an explicit control.
        let control_addr = endpoint
            .channel
            .has_explicit_control
            .then_some(endpoint.channel.local_control);

        self.entries.push(ReceiveChannelEndpointEntry {
            id,
            channel: endpoint.channel.clone(),
            control_addr,
            channel_status_counter_id,
            status: EndpointStatus::Active,
            refcount: 0,
            image_refcount: 0,
            receiver_released: false,
            socket_rcvbuf,
            socket_sndbuf,
            destination_count,
        });

        Ok((id, channel_status_counter_id, Some(Box::new(endpoint))))
    }

    /// A subscription joined an endpoint.
    pub fn attach_subscription(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.refcount += 1;
        }
    }

    /// A subscription left. Returns whether the endpoint is now idle — no
    /// subscriptions *and* no images, which is when it may be released
    /// (`try_remove_endpoint`).
    pub fn detach_subscription(&mut self, id: u64) -> bool {
        let Some(entry) = self.get_mut(id) else {
            return false;
        };

        entry.refcount -= 1;

        entry.refcount <= 0 && entry.image_refcount <= 0
    }

    /// Mark an endpoint as on its way out, and say whether *this* call is the
    /// one that did it (`aeron_receive_channel_endpoint_try_remove_endpoint`,
    /// `media/aeron_receive_channel_endpoint.c:691-702`).
    ///
    /// The condition is the reference's: every stream count at zero — which is
    /// what [`Self::detach_subscription`] has just answered — and no images.
    /// A second call on the same endpoint answers `false`, so the receiver is
    /// asked once.
    pub fn begin_release(&mut self, id: u64) -> bool {
        let Some(entry) = self.get_mut(id) else {
            return false;
        };

        if entry.status != EndpointStatus::Active || entry.refcount > 0 || entry.image_refcount > 0
        {
            return false;
        }

        entry.status = EndpointStatus::Closing;

        true
    }

    /// An image was added to an endpoint.
    pub fn attach_image(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.image_refcount += 1;
        }
    }

    /// An image left an endpoint.
    pub fn detach_image(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.image_refcount -= 1;
        }
    }

    /// An MDS subscription added a destination to an endpoint
    /// (`aeron_driver_conductor.c:5901-5940`).
    pub fn attach_destination(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.destination_count += 1;
        }
    }

    /// A destination left an endpoint (`:6120-6160`).
    ///
    /// Saturating because a removal names a channel the endpoint may not
    /// have — the reference searches its list and finds nothing
    /// (`aeron_receive_channel_endpoint_remove_destination`) — and a count
    /// that went under the truth would open the agreement check on an
    /// endpoint it should be skipped for.
    pub fn detach_destination(&mut self, id: u64) {
        if let Some(entry) = self.get_mut(id) {
            entry.destination_count = entry.destination_count.saturating_sub(1);
        }
    }

    /// Forget an endpoint both sides have let go.
    pub fn remove(&mut self, id: u64) -> Option<ReceiveChannelEndpointEntry> {
        let index = self.entries.iter().position(|entry| entry.id == id)?;

        Some(self.entries.swap_remove(index))
    }

    /// The socket buffer lengths a driver opens a receive endpoint with, from
    /// its settings and the channel's parameters.
    pub fn transport_params(
        config: &crate::config::DriverConfig,
        channel: &UdpChannel,
    ) -> crate::media::TransportParams {
        crate::media::TransportParams {
            socket_rcvbuf: if channel.socket_rcvbuf_length != 0 {
                channel.socket_rcvbuf_length
            } else {
                usize::try_from(config.socket_so_rcvbuf).unwrap_or(0)
            },
            socket_sndbuf: if channel.socket_sndbuf_length != 0 {
                channel.socket_sndbuf_length
            } else {
                usize::try_from(config.socket_so_sndbuf).unwrap_or(0)
            },
            multicast_if_index: channel.interface_index,
            ttl: if channel.multicast_ttl != 0 {
                channel.multicast_ttl
            } else {
                config.socket_multicast_ttl
            },
        }
    }

    /// The receiver window a subscription's channel resolves to: its own
    /// `rcv-wnd=`, or the driver's when it named none
    /// (`params.initial_window_length`, `uri/aeron_driver_uri.c:466`, `:502`).
    ///
    /// Read here rather than at the image because this is where a channel URI
    /// is read at all — and because the agreement check below measures a
    /// window against a receive buffer before any image exists.
    pub fn initial_window_length(
        config: &crate::config::DriverConfig,
        channel: &UdpChannel,
    ) -> usize {
        if channel.receiver_window_length != 0 {
            channel.receiver_window_length
        } else {
            #[allow(clippy::cast_sign_loss)] // a window length is not negative
            {
                config.receiver_window_length.max(0) as usize
            }
        }
    }

    /// The kernel's default socket buffers, for a caller comparing a channel's
    /// parameters against them. Zeroes when the kernel cannot be asked.
    pub fn os_defaults() -> sys::SocketBufferLengths {
        sys::default_socket_buffers().unwrap_or(sys::SocketBufferLengths {
            rcvbuf: 0,
            sndbuf: 0,
        })
    }
}

/// The checks a subscription must pass before it may share an endpoint
/// (`aeron_driver_conductor.c:2184-2223`).
///
/// The receive side's three, and they are not the send side's three: where a
/// publication measures its MTU against the send buffer, a subscription
/// measures its **receiver window** against the receive buffer, and the order
/// of the two buffer comparisons is reversed (`:2202`, `:2213`).
///
/// One asymmetry is the reference's and is reproduced deliberately: the window
/// is measured against the *arriving* channel's receive buffer (`:2193`),
/// not the endpoint's, so a channel that narrows its own `so-rcvbuf` without
/// narrowing `rcv-wnd=` is refused even when the socket it is joining is
/// wider.
///
/// # Errors
///
/// [`ReceiveEndpointErrorKind::ChannelValidation`] carrying the reference's
/// own message — the client reads it, so it is not paraphrased.
fn validate_against_endpoint(
    channel: &UdpChannel,
    entry: &ReceiveChannelEndpointEntry,
    socket_rcvbuf: usize,
    socket_sndbuf: usize,
    initial_window_length: usize,
    os_default_socket_rcvbuf: usize,
) -> Result<(), ReceiveEndpointErrorKind> {
    channel_validation::validate_initial_window_for_rcvbuf(
        initial_window_length,
        socket_rcvbuf,
        os_default_socket_rcvbuf,
        &channel.original_uri,
        Some(&entry.channel.original_uri),
    )
    .map_err(ReceiveEndpointErrorKind::ChannelValidation)?;

    // The buffers the socket already has, named against the ones this channel
    // asks for. `socket_sndbuf`/`socket_rcvbuf` are the *arriving* channel's
    // resolved lengths, not its raw `so-sndbuf=` — the reference passes
    // `aeron_udp_channel_socket_so_*` here where the send side passes the raw
    // URI value, so a channel that names nothing is compared at the context's
    // length rather than being skipped. A channel on the same context is
    // therefore still in agreement; one that named a different length is not.
    for (param, named, adopted) in [
        ("so-sndbuf", socket_sndbuf, entry.socket_sndbuf),
        ("so-rcvbuf", socket_rcvbuf, entry.socket_rcvbuf),
    ] {
        channel_validation::validate_channel_buffer_length(
            param,
            named,
            adopted,
            &channel.original_uri,
            &entry.channel.original_uri,
        )
        .map_err(ReceiveEndpointErrorKind::ChannelValidation)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::config::DriverConfig;
    use crate::media::TransportParams;
    use deepmsg_core::buffer::AtomicBuffer;

    /// A receiver window no socket's receive buffer can be smaller than, for
    /// the tests that are not about the window.
    const SMALL_WINDOW: usize = 1024;

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

    /// The transport parameters the driver would open this channel with:
    /// the channel's own numbers where it named one, the context's otherwise.
    /// This is also what an endpoint *adopts*, which is the value the second
    /// channel is measured against.
    fn params(uri: &str) -> TransportParams {
        ReceiveChannelEndpoints::transport_params(&DriverConfig::default(), &channel(uri))
    }

    /// The window this channel resolves to, the way the conductor reads it.
    fn window(uri: &str) -> usize {
        ReceiveChannelEndpoints::initial_window_length(&DriverConfig::default(), &channel(uri))
    }

    /// The label the counter with this id carries, read the way a client reads
    /// it — through the counter region's own reader.
    fn label(regions: &CounterRegions<'_>, id: i32) -> String {
        let mut found = None;
        regions.reader().for_each(|entry| {
            if entry.counter_id == id {
                found = Some(entry.clone());
            }
        });

        found.expect("a counter").label
    }

    /// Two drivers must not introduce their receivers with the same id
    /// (`aeron_driver_context.c:1306-1313`).
    ///
    /// A sender keys its receivers by that id — the liveness tracker does, and
    /// so does every group strategy (`aeron_min_flow_control.c:185`) — so a
    /// driver that started counting at one would make another driver's
    /// receivers look like its own reporting twice.
    #[test]
    fn a_driver_hands_out_receiver_ids_no_other_driver_hands_out() {
        let first = first_receiver_id();
        assert_ne!(0, first, "the reference retries until it is not zero");

        assert!(
            (0..8).any(|_| first_receiver_id() != first),
            "a constant is not a random start"
        );
    }

    #[test]
    fn the_label_names_the_address_the_endpoint_is_bound_to() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let (_, id, _) = ReceiveChannelEndpoints::default()
            .get_or_add(
                channel("aeron:udp?endpoint=127.0.0.1:40223"),
                &TransportParams::default(),
                &DriverConfig::default(),
                // A window no receive buffer can be smaller than: this test is
                // about the label, and the window check has its own tests in
                // `channel_validation`.
                SMALL_WINDOW,
                &mut counters,
                &regions,
                77,
                1,
                1_000_000,
            )
            .expect("an endpoint");

        // The reference writes the name, the channel, and the address the
        // socket was **actually** bound to (`aeron_position.c:229-244`, called
        // from `aeron_driver_conductor.c:2157-2165`). The channel named a port,
        // so the address is that port — and a reader that gets the whole label
        // knows which socket it is looking at.
        assert_eq!(
            "rcv-channel: aeron:udp?endpoint=127.0.0.1:40223 127.0.0.1:40223",
            label(&regions, id)
        );
    }

    #[test]
    fn a_manual_endpoint_with_no_destination_yet_has_no_address_to_name() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();

        let (_, id, _) = ReceiveChannelEndpoints::default()
            .get_or_add(
                channel("aeron:udp?control-mode=manual"),
                &TransportParams::default(),
                &DriverConfig::default(),
                SMALL_WINDOW,
                &mut counters,
                &regions,
                77,
                1,
                1_000_000,
            )
            .expect("an endpoint");

        // A manual channel starts with no destination, so there is no socket
        // and no address — and the reference writes the label anyway
        // (`aeron_receive_channel_endpoint_bind_addr_and_port` answers with an
        // empty string, `:316-329`), so it ends in a space. That trailing space
        // is the reference's own output, not a typo in this test.
        assert_eq!(
            "rcv-channel: aeron:udp?control-mode=manual ",
            label(&regions, id)
        );
    }

    #[test]
    fn a_shared_endpoint_refuses_a_buffer_parameter_it_does_not_have() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = ReceiveChannelEndpoints::new();
        let config = DriverConfig::default();

        const FIRST: &str = "aeron:udp?endpoint=127.0.0.1:40243|so-rcvbuf=1m";
        const SECOND: &str = "aeron:udp?endpoint=127.0.0.1:40243|so-rcvbuf=2m";
        const SILENT: &str = "aeron:udp?endpoint=127.0.0.1:40243";

        endpoints
            .get_or_add(
                channel(FIRST),
                &params(FIRST),
                &config,
                window(FIRST),
                &mut counters,
                &regions,
                7,
                1,
                1_000_000,
            )
            .expect("an endpoint");

        let error = endpoints
            .get_or_add(
                channel(SECOND),
                &params(SECOND),
                &config,
                window(SECOND),
                &mut counters,
                &regions,
                8,
                2,
                1_000_000,
            )
            .expect_err("refused");

        assert!(
            matches!(
                error,
                ReceiveEndpointErrorKind::ChannelValidation(ref message) if message ==
                    "so-rcvbuf=2097152 does not match existing value of 1048576: \
                     existingChannel=aeron:udp?endpoint=127.0.0.1:40243|so-rcvbuf=1m \
                     channel=aeron:udp?endpoint=127.0.0.1:40243|so-rcvbuf=2m"
            ),
            "{error}"
        );

        // And the asymmetry with the send side, which is the reference's and
        // not an accident of this port: a receive channel that names *no*
        // buffer is compared at the **context's** length
        // (`aeron_udp_channel_socket_so_rcvbuf`, passed at `:2213`), where a
        // publication's is compared at the raw URI value and so is skipped
        // when it named none. The driver's 128k is not the socket's 1m, so
        // this is a refusal where the send side would have said nothing.
        let error = endpoints
            .get_or_add(
                channel(SILENT),
                &params(SILENT),
                &config,
                window(SILENT),
                &mut counters,
                &regions,
                9,
                3,
                1_000_000,
            )
            .expect_err("the context's 128k is not the socket's 1m");

        assert!(
            matches!(
                error,
                ReceiveEndpointErrorKind::ChannelValidation(ref message)
                    if message.starts_with("so-rcvbuf=131072 does not match existing value of 1048576:")
            ),
            "{error}"
        );
    }

    #[test]
    fn a_manual_channel_is_not_compared_with_the_endpoint_it_joins() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = ReceiveChannelEndpoints::new();
        let config = DriverConfig::default();

        // These two *do* canonicalise alike — `control-mode` is not part of
        // the canonical form, the two addresses are — which is what makes the
        // clause reachable at all. A manual channel's destinations arrive one
        // at a time from clients, so it is asking to *join* a socket rather
        // than to describe one, and the reference leaves it alone entirely.
        const FIRST: &str = "aeron:udp?endpoint=127.0.0.1:40253|so-rcvbuf=1m";
        const MANUAL: &str = "aeron:udp?endpoint=127.0.0.1:40253|control-mode=manual|so-rcvbuf=2m";

        let (id, _, _) = endpoints
            .get_or_add(
                channel(FIRST),
                &params(FIRST),
                &config,
                window(FIRST),
                &mut counters,
                &regions,
                7,
                1,
                1_000_000,
            )
            .expect("an endpoint");

        let (second, _, _) = endpoints
            .get_or_add(
                channel(MANUAL),
                &params(MANUAL),
                &config,
                window(MANUAL),
                &mut counters,
                &regions,
                8,
                2,
                1_000_000,
            )
            .expect("a manual channel is not compared");

        assert_eq!(id, second, "the clause is only reachable when they share");
        assert_eq!(1, endpoints.entries().len());
    }

    #[test]
    fn an_endpoint_that_has_gained_destinations_is_not_compared_with_them() {
        let mut fixture = Fixture::new();
        let (mut counters, regions) = fixture.open();
        let mut endpoints = ReceiveChannelEndpoints::new();
        let config = DriverConfig::default();

        const FIRST: &str = "aeron:udp?endpoint=127.0.0.1:40263|so-rcvbuf=1m";
        const SECOND: &str = "aeron:udp?endpoint=127.0.0.1:40263|so-rcvbuf=2m";

        let (id, _, _) = endpoints
            .get_or_add(
                channel(FIRST),
                &params(FIRST),
                &config,
                window(FIRST),
                &mut counters,
                &regions,
                7,
                1,
                1_000_000,
            )
            .expect("an endpoint");

        // `:2184`'s second clause. Nothing in this build can add a destination
        // to an endpoint that is not manual — only an MDS subscription may,
        // and an MDS subscription is manual — so the count is moved by hand to
        // show that the clause is what decides, and not the luck of the count
        // being one.
        endpoints.attach_destination(id);

        let (second, _, _) = endpoints
            .get_or_add(
                channel(SECOND),
                &params(SECOND),
                &config,
                window(SECOND),
                &mut counters,
                &regions,
                8,
                2,
                1_000_000,
            )
            .expect("an endpoint with two destinations is not the shape it was made in");

        assert_eq!(id, second, "the second channel joins the same endpoint");
        assert_eq!(1, endpoints.entries().len());
    }
}
