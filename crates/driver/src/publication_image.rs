//! The receive half of the data plane: a stream rebuilt from the wire.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_publication_image.c`. An image is
//! what a *subscription* reads: the same log buffer shape an IPC publication
//! has, filled not by a local producer but by datagrams, and read by the same
//! client code either way.
//!
//! # Four things an image does, in order of how often
//!
//! 1. **Takes packets** ([`PublicationImage::insert_packet`]): validate, write
//!    the frames into the term at their offsets, and say how far the stream has
//!    been received. A packet that arrives twice is not written twice, because
//!    a term slot that already holds a frame is left alone — which is what
//!    makes retransmission idempotent.
//! 2. **Rebuilds the reader's position** ([`PublicationImage::track_rebuild`]):
//!    how far can a reader be moved without skipping a hole?
//! 3. **Says what it has** ([`PublicationImage::send_pending_status_message`]):
//!    a status message is the receiver's only voice, and it is where flow
//!    control comes from.
//! 4. **Notices silence** ([`PublicationImage::on_time_event`]): an image whose
//!    sender has stopped is drained, then lingers, then is done — and each of
//!    those is a client-visible event.
//!
//! # Who owns an image
//!
//! The conductor creates it — it is the one that can allocate a log buffer and
//! a counter — and the **receiver thread** owns it afterwards, for the same
//! reason the sender owns the publications: the thread that reads the term is
//! the thread that writes it. What the conductor keeps is a record, and the one
//! thing that crosses back is a reader: a subscription's position is added to
//! this image's [`Subscribable`] by a command, because the *counter* is what
//! both sides actually share.

use std::net::SocketAddr;

use deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED;
use deepmsg_cnc::{CounterManager, CounterRegions};
use deepmsg_core::logbuffer::descriptor;
use deepmsg_core::logbuffer::logfile::LogFile;
use deepmsg_core::logbuffer::position::{Position, RawTail, index_by_term};
use deepmsg_core::logbuffer::repair;

use crate::congestion_control::CongestionControl;
use crate::loss_detector::{Gap, LossDetector};
use crate::media::receive_endpoint::ReceiveChannelEndpoint;
use crate::protocol::{DataFrame, FrameHeader, header_flags};
use crate::publication_params::SubscriptionParams;
use crate::subscribable::{Subscribable, TetherState, TetherablePosition};
use crate::system_counters::{self, System};

/// How long a status message may go unsent before one is sent anyway
/// (`aeron.status.message.timeout`, 200 ms).
pub const STATUS_MESSAGE_TIMEOUT_NS: i64 = 200_000_000;

/// How long a destination may go unheard from before it is no longer a place a
/// round-trip measurement is sent to
/// (`AERON_RECEIVE_DESTINATION_TIMEOUT_NS`,
/// `media/aeron_receive_destination.h:24`, five seconds).
pub const RECEIVE_DESTINATION_TIMEOUT_NS: i64 = 5 * 1000 * 1000 * 1000;

/// What an image is built with when nothing configures one
/// (`AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT`, `aeron_driver_context.c:204`).
///
/// The value a driver actually uses is
/// [`DriverConfig::image_liveness_timeout_ns`](crate::config::DriverConfig::image_liveness_timeout_ns);
/// this is the default that setting starts from, and what a test that builds an
/// image by hand passes.
pub const IMAGE_LIVENESS_TIMEOUT_NS: i64 = crate::config::IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT;

/// How many status-message periods a drained image waits before lingering
/// (`AERON_IMAGE_SM_EOS_MULTIPLE`,
/// `aeron-driver/src/main/c/aeron_publication_image.h:35`).
pub const IMAGE_SM_EOS_MULTIPLE: i64 = 5;

/// The value [`PublicationImage::response_session_id`] holds when this image
/// owes nobody a response setup
/// (`AERON_PUBLICATION_RESPONSE_NULL_RESPONSE_SESSION_ID`,
/// `aeron-driver/src/main/c/aeron_publication_image.c:28`).
///
/// It is deliberately **not** a valid session id and deliberately not zero: a
/// session id is an `i32`, and the sentinel is far outside that range, which is
/// the whole of how "is there one?" is answered
/// (`aeron_publication_image_check_and_get_response_session_id`, `:59-66`
/// casts to `i32` and asks whether the cast was lossless).
/// Written through `u64` because the reference writes the same bit pattern as
/// `INT64_C(0xF000000000000000)`, which C makes negative — the value here is
/// that same negative number, not the unsigned one.
pub const RESPONSE_NULL_SESSION_ID: i64 = 0xF000_0000_0000_0000u64 as i64;

/// The three outcomes of the untethered state machine. They live with the
/// positions they are about ([`crate::subscribable::UntetheredEvent`]) because
/// a publication's own readers reach them too, not just an image's.
pub use crate::subscribable::UntetheredEvent;

/// Where an image is in its life
/// (`aeron_publication_image_state_t`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageState {
    /// Taking packets.
    Active,
    /// The sender has stopped (or said it did): drain what is left.
    Draining,
    /// Drained, waiting for its readers to finish.
    Linger,
    /// Done; the conductor may release it.
    Done,
}

/// What an image's counters are
/// (`aeron_counter_receiver_hwm_allocate` / `_position_allocate`,
/// `aeron-driver/src/main/c/aeron_position.c:155-202`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageCounters {
    /// `rcv-hwm`: the highest position a packet has been seen for.
    pub rcv_hwm: i32,
    /// `rcv-pos`: how far a reader may safely be moved.
    pub rcv_pos: i32,
    /// `rcv-naks-sent`: the gap reports this image has asked for
    /// (`aeron_counter_receiver_naks_sent_allocate`,
    /// `aeron-driver/src/main/c/aeron_driver_conductor.c:6680-6683`).
    pub rcv_naks_sent: i32,
}

/// An image: a stream rebuilt from datagrams.
/// One place this image hears from
/// (`aeron_publication_image_connection_t`, `aeron_publication_image.h:37-49`).
///
/// The reference keys these by **receive destination** and keeps the control
/// address beside it (`:555-576`). This build has no receive destinations yet —
/// they arrive with multi-destination channels — so a connection is found by
/// the source its packets come from, which for the single implicit-unicast
/// source of a P1-4 channel is the same thing.
///
/// Every field is here because the shape is what the multi-destination work
/// needs; with one connection, `is_eos` and `eos_position` have exactly one
/// entry to describe and the image's own `eos_position` is that entry's.
#[derive(Clone, Copy, Debug)]
pub struct Connection {
    /// Which of the endpoint's destinations this connection arrived on
    /// (`aeron_publication_image_connection_t.destination`,
    /// `aeron_publication_image.h:41`).
    ///
    /// It is what every frame an image sends back is sent **through**: the
    /// control address says *where* the message goes, this says *which socket*
    /// it leaves from. The two are not the same question, and for a
    /// multi-destination endpoint they do not have the same answer — a status
    /// message out of the wrong destination's socket is one the far end's
    /// kernel drops without a word.
    pub destination: crate::media::receive_endpoint::DestinationId,
    /// Where status messages and NAKs for this connection go (`control_addr`),
    /// taken from the channel's control address or from the first packet that
    /// arrived (`aeron_publication_image_connection_set_control_address`,
    /// `:30-37`).
    pub control_address: Option<SocketAddr>,
    /// When anything was last seen on this connection
    /// (`time_of_last_activity_ns`), which is what liveness is measured
    /// against.
    pub time_of_last_activity_ns: i64,
    /// When a **frame** was last seen on it (`time_of_last_frame_ns`).
    pub time_of_last_frame_ns: i64,
    /// Whether this connection has said the stream is over.
    ///
    /// **Not yet written.** The reference sets this and the position beside it
    /// in `aeron_publication_image_all_eos` (`:594-612`), which is called from
    /// the packet path because it needs to know *which* connection ended.
    /// [`PublicationImage::on_heartbeat`] is where the end of the stream is
    /// seen here and it is not given the source, so with one connection the
    /// image's own `eos_position` is what is used and these two describe a
    /// shape that nothing fills yet. They are filled when multiple connections
    /// make "which one ended" a question worth asking.
    pub is_eos: bool,
    /// Where the stream ended, as this connection said
    /// (`connection->eos_position`) — not yet written, see [`Connection::is_eos`].
    pub eos_position: i64,
}

pub struct PublicationImage {
    /// The conductor's registration id for this image, which is what
    /// `ON_AVAILABLE_IMAGE` names and what a public `AeronStat` shows.
    pub registration_id: i64,
    /// The session the sender is running.
    pub session_id: i32,
    /// The stream.
    pub stream_id: i32,
    /// The term id the stream started at.
    pub initial_term_id: i32,
    /// The term the sender was writing when this image was made — where the
    /// stream picks up.
    pub active_term_id: i32,
    /// How long a term is.
    pub term_length: i32,
    /// `log2(term_length)`.
    pub position_bits_to_shift: u32,
    /// The sender's MTU, from its `SETUP`.
    pub mtu_length: i32,
    /// Which receive endpoint this image belongs to.
    pub endpoint_id: u64,
    /// The channel, as the client wrote it.
    pub channel: Vec<u8>,
    /// The log buffer a reader maps.
    pub log: Box<LogFile>,
    /// The counters a client reads.
    pub counters: ImageCounters,
    /// Who is reading, and how far (`subscribable`).
    pub subscribers: Subscribable,
    /// Where this image hears from, one entry per source
    /// (`connections`, `aeron_publication_image.h:79-85`).
    ///
    /// Status messages and NAKs go to **each** of these, not to one address
    /// (`:901-925`), which is what lets an image with several receivers answer
    /// all of them. With one source there is one entry, and the behaviour is
    /// what it was.
    pub connections: Vec<Connection>,
    /// When a packet was last seen, which is what decides draining.
    pub time_of_last_packet_ns: i64,
    /// Who sends this stream, formatted the way a source identity is
    /// (`aeron_publication_image.c:336-343`). It is what a loss report record
    /// carries beside the channel, so a reader of `LossStat` can tell which
    /// sender's packets went missing.
    pub source_identity: Vec<u8>,
    /// Where this image's record is in the loss report file, or `None` when
    /// none has been made yet or the file refused one
    /// (`aeron_publication_image.c:123-155`).
    ///
    /// The file is a fixed length and never wraps: a record that would not fit
    /// leaves the image with [`PublicationImage::loss_report_given_up`] set
    /// and **no** further attempts, which is what the reference does when
    /// `create_entry` fails.
    loss_report_entry_offset: Option<usize>,
    /// Whether the record was refused — the reference's
    /// `loss_report = NULL`, and the point at which this image stops being a
    /// reporter at all.
    loss_report_given_up: bool,
    /// The span of the last report made, which is what keeps a hole that is
    /// found again and again from being counted again and again
    /// (`:452-479`).
    loss_report_term_id: i32,
    loss_report_term_offset: i32,
    loss_report_length: usize,
    /// Whether a subscription has ever been linked to this image.
    ///
    /// The reference links a subscription to the image **before** the receiver
    /// is given it — `aeron_driver_conductor.c:6763` links the subscribable,
    /// `:6782` hands the image over — so an image it creates is never one with
    /// no readers, and its `has_working_positions` test (`:1318`) never has to
    /// tell "not linked yet" from "everyone left".
    ///
    /// Here the two are separate commands on the receiver's queue, and the time
    /// event at the end of a pass can run between them. An image that had just
    /// been created would then be drained for having no subscribers, and would
    /// answer a sender that has done nothing wrong with an end-of-stream. This
    /// flag stands in for the reference's ordering: until a subscription has
    /// been linked, "no subscribers" is not yet a fact about the image, and the
    /// liveness timeout is what retires one nobody ever links.
    pub has_been_linked: bool,
    /// Whether the sender has said the stream is over.
    pub is_end_of_stream: bool,
    /// Whether the status messages being sent carry the end-of-stream flag.
    pub is_sending_eos_sm: bool,
    /// Whether the sender revoked the stream (a `REVOKED` flag on an
    /// end-of-stream heartbeat).
    pub is_revoked: bool,
    /// Where the stream ended, as the last end-of-stream frame said
    /// (`connection->eos_position`): the position of that frame, not the high
    /// water mark, which may have moved on.
    pub eos_position: i64,
    /// Why this image was rejected, when it was — an image that cannot be
    /// built still has to tell its sender so (`:974-996`).
    ///
    /// Bytes and not a string: the reason is a client's, it crosses the wire in
    /// a `REJECT_IMAGE` and goes back out in an `ERR` frame, and nothing
    /// between the two is entitled to decide it was UTF-8.
    pub invalidation_reason: Option<Vec<u8>>,
    /// The position the next status message reports.
    next_sm_position: i64,
    /// The window the next status message advertises.
    next_sm_receiver_window_length: i32,
    /// What the last status message said, which is what bounds an over-run.
    last_sm_position: i64,
    /// How far the terms have been zeroed behind the readers
    /// (`aeron_publication_image_clean_buffer_to`, `:430-451`).
    clean_position: i64,
    /// The position past which an arrival is an over-run.
    last_overrun_threshold: i64,
    /// The change number the last status message was sent for.
    last_sm_change_number: i64,
    /// Bumped whenever the position or the window a status message would carry
    /// moves: an image sends one because *something changed* (`:936`).
    sm_change_number: i64,
    /// When the next status message is due.
    next_sm_deadline_ns: i64,
    /// How often a status message is forced (`status.message.timeout`).
    sm_timeout_ns: i64,
    /// The strategy this image runs, named by the endpoint channel's `cc=`
    /// (`congestion_control`, `aeron_publication_image.c:290`).
    ///
    /// It is where both windows come from, and — for `cubic` — the thing the
    /// rebuild and the round-trip measurements go through. The image keeps no
    /// window of its own beyond the one the next status message will carry.
    congestion_control: CongestionControl,
    /// The largest window it will ever offer (`max_window_length`).
    max_receiver_window_length: i32,
    /// How long this image may go quiet before it drains.
    liveness_timeout_ns: i64,
    /// The holes in this image's stream, and the timer on the one being asked
    /// for (`aeron_publication_image_t.loss_detector`).
    loss_detector: LossDetector,
    /// How long a reader may fall behind before it is put aside
    /// (`untethered-window-limit-timeout`, from the *channel's* parameters —
    /// an image is created by a `SETUP`, so the subscription that reads it
    /// inherits these rather than setting them).
    pub untethered_window_limit_timeout_ns: i64,
    /// Whether the readers still have to be told this image is gone
    /// (`aeron_driver_conductor_image_transition_to_linger`, `:1642-1675`,
    /// which runs as the image leaves DRAINING for LINGER).
    ///
    /// A flag rather than a question about the state, because it is about a
    /// **transition**: an image that has already said so does not say it again,
    /// and one that reached LINGER the other way — a revoked image — leaves
    /// this false and is told when it is released instead.
    linger_notice: bool,
    /// Whether this image asks its sender for the frames it is missing.
    ///
    /// `reliable=false` is the one channel parameter that changes what an image
    /// *does* rather than what it advertises: a hole is filled with a padding
    /// frame instead of being asked for, and the data in it is simply not
    /// recovered (`aeron_publication_image.c:1024-1066`). It is stored here
    /// because the branch is taken on the receiver thread, long after the
    /// subscription parameters it came from are gone
    /// (`aeron_publication_image_conductor_fields_stct.is_reliable`, `.h:57`).
    pub is_reliable: bool,
    /// How long it lingers before it is either closed or rested.
    pub untethered_linger_timeout_ns: i64,
    /// And how long it rests before it is woken.
    pub untethered_resting_timeout_ns: i64,
    /// Where it is in its life.
    pub state: ImageState,
    /// When the state last changed, for the linger timeout.
    pub time_of_last_state_change_ns: i64,
    /// The session this image owes a `RSP_SETUP` to, or
    /// [`RESPONSE_NULL_SESSION_ID`] when it owes one to nobody.
    ///
    /// Written by the conductor, once a response publication has named this
    /// image and linked to it (`aeron_driver_conductor.c:4227-4232`), and
    /// cleared once the publication has heard from a live receiver of its own
    /// (`aeron_driver_conductor_on_response_connected`, `:7117-7131`). Until
    /// then the image says the session on every status-message period, because
    /// the publisher cannot ask again and has nothing else to learn it from.
    response_session_id: i64,
    /// Whether the conductor has asked for the next status message to be sent
    /// now rather than when it is due
    /// (`aeron_publication_image_request_next_sm_deadline_reset`,
    /// `aeron_publication_image.h:363-373`).
    ///
    /// A response setup rides the status-message timer — an image has no other
    /// timer — so a conductor that has just set a session id would otherwise
    /// wait out the remainder of the period before the frame the publisher is
    /// blocked on goes anywhere.
    is_next_sm_deadline_reset_requested: bool,
}

/// Where a stream starts, and the term geometry that says so, from the `SETUP`
/// that described it (`aeron_publication_image.c:377-392`).
///
/// The stream begins at the sender's **active** term and offset, not at zero:
/// an image is created for a stream that is already running, and a subscription
/// that links to that image begins reading at the same place
/// (`aeron_publication_image.h:376-396`).
///
/// The fallback for a term length that is not a power of two is the one
/// `aeron_publication_image_create` has always used, kept here so that the two
/// callers agree on it.
pub fn stream_start(setup: &crate::protocol::SetupFrame) -> (i64, u32) {
    let bits = deepmsg_core::logbuffer::position::bits_to_shift(setup.term_length).unwrap_or(16);

    let position = Position::new(
        setup.active_term_id,
        setup.term_offset,
        bits,
        setup.initial_term_id,
    )
    .raw();

    (position, bits)
}

impl PublicationImage {
    /// Create an image over a log buffer the conductor has already mapped
    /// (`aeron_publication_image_create`,
    /// `aeron-driver/src/main/c/aeron_publication_image.c:140-330`).
    ///
    /// `setup` is the frame that started it: the stream begins where the
    /// sender's *active* term and offset say, not at zero, because a
    /// subscription that arrives late joins a stream already in progress.
    #[allow(clippy::too_many_arguments)] // one per field the create needs
    pub fn create(
        registration_id: i64,
        endpoint_id: u64,
        destination: crate::media::receive_endpoint::DestinationId,
        channel: &[u8],
        log: Box<LogFile>,
        setup: &crate::protocol::SetupFrame,
        source: SocketAddr,
        control_address: SocketAddr,
        counters: ImageCounters,
        congestion_control: CongestionControl,
        channel_window_length: i32,
        sm_timeout_ns: i64,
        liveness_timeout_ns: i64,
        page_size: usize,
        untethered: SubscriptionParams,
        group_semantics: bool,
        multicast_backoff: crate::loss_detector::MulticastBackoff,
        nak_unicast_delay_ns: i64,
        now_ns: i64,
    ) -> Self {
        let (initial_position, bits) = stream_start(setup);

        // The two windows are the strategy's and are not computed here at all:
        // the reference reads them off it (`aeron_publication_image.c:377-380`)
        // and keeps no window of its own, which is what makes `cc=` mean
        // anything. `static` answers the channel's window cut to half a term —
        // `aeron_receiver_window_length`, which the strategy applies — and
        // `cubic` the congestion window it starts at.
        let window = congestion_control.initial_window_length();
        let max_window = congestion_control.max_window_length();

        // The tails, so a reader that maps this file sees the stream where it
        // starts rather than at zero (`aeron_publication_image.c:250-300`).
        log.initialise_tails(
            setup.initial_term_id,
            Some((setup.active_term_id, setup.term_offset)),
        );

        {
            if let Some(metadata) = log.metadata() {
                let init = descriptor::LogMetadataInit {
                    end_of_stream_position: i64::MAX,
                    is_connected: 0,
                    active_transport_count: 0,
                    correlation_id: registration_id,
                    initial_term_id: setup.initial_term_id,
                    mtu_length: setup.mtu,
                    term_length: setup.term_length,
                    page_size: i32::try_from(page_size).unwrap_or(4096),
                    publication_window_length: 0,
                    // **Uncut by the term**, which is what the reference writes
                    // here: it passes `params.initial_window_length` straight
                    // through (`aeron_publication_image.c:250-262`), and that
                    // is the context default or the channel's own `rcv-wnd=`
                    // (`aeron_driver_uri.c:466`, `:502`) with no cap on it. The
                    // cap belongs to whoever uses the value as a window.
                    receiver_window_length: channel_window_length,
                    socket_sndbuf_length: 0,
                    os_default_socket_sndbuf_length: 0,
                    os_max_socket_sndbuf_length: 0,
                    socket_rcvbuf_length: 0,
                    os_default_socket_rcvbuf_length: 0,
                    os_max_socket_rcvbuf_length: 0,
                    max_resend: 0,
                    session_id: setup.session_id,
                    stream_id: setup.stream_id,
                    entity_tag: -1,
                    response_correlation_id: -1,
                    linger_timeout_ns: 0,
                    untethered_window_limit_timeout_ns: 0,
                    untethered_linger_timeout_ns: 0,
                    untethered_resting_timeout_ns: 0,
                    // The two the channel and the `SETUP` decide, rather than
                    // the image: whether this stream is one of a group, and
                    // whether this subscription exists to carry the answers to
                    // a request (`aeron_publication_image.c:277-278`, which
                    // reads `treat_as_multicast` and `params.is_response`).
                    //
                    // Both are read off the *channel*'s URI and the frame that
                    // opened the stream — not off the subscription that happens
                    // to be reading, which is why a second reader of the same
                    // stream sees the same two bytes.
                    group: u8::from(group_semantics),
                    is_response: untethered.is_response,
                    rejoin: false,
                    // The two the *subscription* decides, rather than the
                    // channel or the sender. `reliable` is not decoration: it
                    // is the image's own behaviour, whether it asks for its
                    // holes or fills them (`aeron_publication_image.c:307`,
                    // `:1024`), and the metadata carries a copy of it for
                    // whoever reads the image later (`:280`).
                    //
                    // Both are read off the channel's parameters because an
                    // image is created by a `SETUP` and has no subscription of
                    // its own at that moment. The reference reads `reliable`
                    // off the subscription link being linked and `sparse` off
                    // the *oldest* subscription matching the image
                    // (`aeron_driver_conductor_is_oldest_subscription_sparse`),
                    // which is the same subscription here until two of them
                    // with different `sparse` share one image — recorded in
                    // `docs/compat.md`.
                    reliable: untethered.is_reliable,
                    sparse: untethered.is_sparse,
                    signal_eos: true,
                    spies_simulate_connection: false,
                    tether: false,
                    is_exclusive: false,
                };

                let _ = descriptor::initialise(&metadata, &init);
            }
        }

        Self {
            registration_id,
            session_id: setup.session_id,
            stream_id: setup.stream_id,
            initial_term_id: setup.initial_term_id,
            active_term_id: setup.active_term_id,
            term_length: setup.term_length,
            position_bits_to_shift: bits,
            mtu_length: setup.mtu,
            endpoint_id,
            channel: channel.to_vec(),
            log,
            counters,
            subscribers: Subscribable::new(registration_id),
            connections: vec![Connection {
                // The `SETUP` arrived on a destination's socket, and that is
                // the one every answer to this session leaves through. The
                // create is given it for exactly this (`aeron_publication_image_create`'s
                // `destination`, `aeron_publication_image.h:170`) and the image
                // keeps no copy of its own — the reference hands it straight to
                // the first connection it tracks
                // (`new_connection->destination = destination`,
                // `aeron_publication_image.c:1131`).
                destination,
                control_address: Some(control_address),
                time_of_last_activity_ns: now_ns,
                time_of_last_frame_ns: now_ns,
                is_eos: false,
                eos_position: 0,
            }],
            time_of_last_packet_ns: now_ns,
            // The source identity is the reference's own formatting of the
            // address the first packet came from — the same string
            // `ON_AVAILABLE_IMAGE` carries (`aeron_publication_image.c:336-343`).
            source_identity: crate::udp_channel::format_source_identity(source)
                .unwrap_or_default()
                .into_bytes(),
            loss_report_entry_offset: None,
            loss_report_given_up: false,
            loss_report_term_id: 0,
            loss_report_term_offset: 0,
            loss_report_length: 0,
            has_been_linked: false,
            is_end_of_stream: false,
            linger_notice: false,
            is_sending_eos_sm: false,
            is_revoked: false,
            eos_position: initial_position,
            invalidation_reason: None,
            next_sm_position: initial_position,
            congestion_control,
            next_sm_receiver_window_length: window,
            last_sm_position: initial_position,
            clean_position: initial_position,
            last_overrun_threshold: initial_position + i64::from(setup.term_length / 2),
            last_sm_change_number: 0,
            sm_change_number: 0,
            // The first status message is due at once: the sender is still
            // saying `SETUP` until one arrives, and this is the answer it is
            // waiting for.
            next_sm_deadline_ns: now_ns - 1,
            sm_timeout_ns,
            max_receiver_window_length: max_window,
            liveness_timeout_ns,
            // The channel's own delays, when it named one. `nak-delay=` is the
            // whole of what a subscription may say about how its gaps are asked
            // for (`aeron_publication_image.c:100-118`), and until this line
            // the parameter was parsed by nobody and changed nothing.
            //
            // `reliable=false` outranks it: the reference checks that first and
            // returns a static generator at zero, so an unreliable channel's
            // gap is fillable the moment it is seen and `nak-delay=` on the
            // same channel is never read (`:92-95`, which returns before the
            // two branches below it).
            //
            // And group semantics outrank it too, which is the same reading in
            // the other direction: a group gets the randomised generator
            // instead of a fixed pair (`:97-100`), so `nak-delay=` on a
            // group's channel is a parameter the reference reads and then
            // ignores.
            loss_detector: LossDetector::for_channel(
                registration_id,
                untethered.is_reliable,
                group_semantics,
                untethered.nak_delay_ns,
                nak_unicast_delay_ns,
                multicast_backoff,
            ),
            is_reliable: untethered.is_reliable,
            untethered_window_limit_timeout_ns: untethered.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: untethered.untethered_linger_timeout_ns,
            untethered_resting_timeout_ns: untethered.untethered_resting_timeout_ns,
            state: ImageState::Active,
            time_of_last_state_change_ns: now_ns,
            response_session_id: RESPONSE_NULL_SESSION_ID,
            is_next_sm_deadline_reset_requested: false,
        }
    }

    /// Tell this image to say `response_session_id` to the publication that
    /// asked for a response channel
    /// (`aeron_publication_image_set_response_session_id`,
    /// `aeron_publication_image.h:352-356`).
    ///
    /// The two together — the session and the request that the timer be brought
    /// forward — are one act in the reference's caller
    /// (`aeron_driver_conductor.c:4227-4232`), and they are one act here too:
    /// an image told a session it will not say for another period has been told
    /// nothing the publisher can use.
    pub fn set_response_session_id(&mut self, response_session_id: i64) {
        self.response_session_id = response_session_id;
        self.is_next_sm_deadline_reset_requested = true;
    }

    /// Stop owing a response setup
    /// (`aeron_publication_image_remove_response_session_id`, `:1389-1392`).
    pub fn remove_response_session_id(&mut self) {
        self.set_response_session_id(RESPONSE_NULL_SESSION_ID);
    }

    /// The session to say in a response setup, when there is one
    /// (`aeron_publication_image_check_and_get_response_session_id`, `:59-66`).
    ///
    /// "There is one" is a question about the *range*: a session id is an
    /// `i32`, so a value that does not survive the narrowing is the sentinel
    /// rather than a session.
    fn response_session_id_to_send(&self) -> Option<i32> {
        i32::try_from(self.response_session_id).ok()
    }

    /// Give up the log buffer, for a caller that is about to unmap and unlink
    /// it (`LogFile::remove`).
    pub fn into_log(self) -> Box<LogFile> {
        self.log
    }

    /// The log buffer's path, which `ON_AVAILABLE_IMAGE` hands the client.
    pub fn path_bytes(&self) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt;

        self.log.path().as_os_str().as_bytes().to_vec()
    }

    /// Add a reader's position to this image
    /// (`aeron_driver_conductor_link_subscribable`'s image case).
    pub fn add_subscriber(&mut self, position: TetherablePosition) -> bool {
        let mut hooks = NoHooks;
        self.subscribers.add_position(position, &mut hooks);

        // From here on "no subscribers" means every reader has gone, which is
        // what the drain decision is for ([`PublicationImage::has_been_linked`]).
        self.has_been_linked = true;

        true
    }

    /// Remove a reader's position.
    pub fn remove_subscriber(&mut self, counter_id: i32) -> bool {
        let mut hooks = NoHooks;
        self.subscribers
            .remove_position(counter_id, &mut hooks)
            .is_some()
    }

    /// Whether anything is reading it.
    pub fn has_subscribers(&self) -> bool {
        self.subscribers.has_working_positions()
    }

    /// Whether the stream has been read to its end (`is_drained`): everything
    /// received has been read.
    pub fn is_drained(&self, counters: &CounterManager, regions: &CounterRegions<'_>) -> bool {
        let hwm = counters.value(regions, self.counters.rcv_hwm).unwrap_or(0);
        let min = self
            .subscribers
            .min_active_position(counters, regions)
            .unwrap_or(i64::MAX);

        min >= hwm
    }

    /// Counters' ids, for the conductor's record.
    pub const fn counters(&self) -> ImageCounters {
        self.counters
    }

    /// Whether this image asks for the frames it is missing, or fills the holes
    /// they left (`aeron_publication_image_conductor_fields_stct.is_reliable`,
    /// `.h:57`). Read by the receiver, which is where the branch is taken.
    pub const fn is_reliable(&self) -> bool {
        self.is_reliable
    }

    /// Cover the hole the scan found with a padding frame, so that readers can
    /// move past it — what an unreliable image does instead of asking its
    /// sender for the frames (`aeron_publication_image_send_pending_loss`,
    /// `:1053-1066`).
    ///
    /// Nothing goes on the network, and the frame that lands is padding: the
    /// data in the hole is never recovered, which is the whole of what
    /// `reliable=false` buys and the whole of what it costs.
    ///
    /// Returns whether the fill happened. It does not when something has landed
    /// in the hole since it was scanned — a retransmission for another reader
    /// of the same image, or a packet that was merely out of order — because
    /// covering it would throw that frame away
    /// (`concurrent/aeron_term_gap_filler.c:26-35`).
    pub fn fill_gap(&mut self, gap: Gap) -> bool {
        let index = index_by_term(self.initial_term_id, gap.term_id);

        let Some(term) = self.log.term(index) else {
            return false;
        };
        let Some(metadata) = self.log.metadata() else {
            return false;
        };

        repair::fill_gap(
            &term,
            &metadata.as_read_only(),
            gap.term_offset.unsigned_abs() as usize,
            gap.length,
            gap.term_id,
        )
        .is_some()
    }

    /// Report loss to the driver's loss report file
    /// (`aeron_publication_image_report_loss`, `aeron_driver_publication_image.c:123-155`
    /// — the file itself is `crates/cnc/src/loss_report.rs`).
    ///
    /// Two things here are the reference's and neither is obvious:
    ///
    /// * the **first** report creates the record, with the channel and the
    ///   source, and every later one only adds to it — so a reader sees one
    ///   row per stream, not one per hole;
    /// * a hole that is found again is only counted for the part that is
    ///   **new**: the reference remembers the span it last reported and
    ///   subtracts the overlap (`:470-478`), which is what keeps a hole the
    ///   loss detector keeps rediscovering from inflating the total.
    ///
    /// A file that refuses the record — it is a fixed length and never wraps —
    /// is the end of it for this image: [`PublicationImage::loss_report_given_up`]
    /// goes up and nothing is tried again, which is the reference's
    /// `loss_report = NULL`.
    ///
    /// # Returns
    ///
    /// The bytes this call added to the report, which is what the caller adds
    /// to its work count.
    pub fn report_loss(
        &mut self,
        file: &deepmsg_cnc::loss_report::LossReportFile,
        cursor: &mut usize,
        gap: &Gap,
        timestamp_ms: i64,
    ) -> usize {
        if self.loss_report_given_up {
            return 0;
        }

        let gap_length = i64::try_from(gap.length).unwrap_or(i64::MAX);
        let gap_offset = i64::from(gap.term_offset);
        let end_offset = i64::from(self.loss_report_term_offset)
            + i64::try_from(self.loss_report_length).unwrap_or(i64::MAX);

        let bytes_lost = if gap.term_id != self.loss_report_term_id || gap_offset >= end_offset {
            gap_length
        } else if gap_offset + gap_length > end_offset {
            // Only the part past the span already reported.
            gap_offset + gap_length - end_offset
        } else {
            // Inside what was already reported: nothing new, and the reference
            // does not call its reporter at all.
            return 0;
        };

        let Some(buffer) = file.writable() else {
            self.loss_report_given_up = true;
            return 0;
        };

        let reported = match self.loss_report_entry_offset {
            Some(offset) => deepmsg_cnc::loss_report::record_observation(
                &buffer,
                offset,
                bytes_lost,
                timestamp_ms,
            )
            .is_some(),
            None => {
                let entry = deepmsg_cnc::loss_report::LossReportEntry {
                    observation_count: 1,
                    total_bytes_lost: bytes_lost,
                    first_observation_timestamp: timestamp_ms,
                    last_observation_timestamp: timestamp_ms,
                    session_id: self.session_id,
                    stream_id: self.stream_id,
                    channel: self.channel.clone(),
                    source: self.source_identity.clone(),
                };

                match entry.encode(&buffer, *cursor) {
                    Some(()) => {
                        self.loss_report_entry_offset = Some(*cursor);
                        *cursor += entry.record_length();
                        true
                    }
                    None => false,
                }
            }
        };

        if !reported {
            // Full, or the offset was not in the file: the reference gives up
            // on this image rather than retrying.
            self.loss_report_given_up = true;
            return 0;
        }

        self.loss_report_term_id = gap.term_id;
        self.loss_report_term_offset = gap.term_offset;
        self.loss_report_length = gap.length;

        usize::try_from(bytes_lost).unwrap_or(0)
    }

    /// How far the reader has been moved, as the counters hold it.
    pub fn position(&self, counters: &CounterManager, regions: &CounterRegions<'_>) -> i64 {
        counters.value(regions, self.counters.rcv_pos).unwrap_or(0)
    }

    /// How far packets have been seen.
    pub fn hwm_position(&self, counters: &CounterManager, regions: &CounterRegions<'_>) -> i64 {
        counters.value(regions, self.counters.rcv_hwm).unwrap_or(0)
    }

    /// Take a packet (`aeron_publication_image_insert_packet`, `:740-860`).
    ///
    /// The `source` is where the packet came from, which is what a status
    /// message answers to when the channel named no control address; the
    /// `destination` is the socket it arrived on, which is what the answer
    /// leaves through.
    ///
    /// Returns how many bytes were accepted, which is the packet's length for
    /// anything that was written and zero for anything that was not. Three
    /// kinds of packet are *accepted without being written*, and all three are
    /// the protocol working rather than failing:
    ///
    /// * a **heartbeat** — a zero-length DATA frame — which carries liveness
    ///   and the end-of-stream flag and nothing else;
    /// * a packet behind the last status message this image sent, which is the
    ///   cooling tail of a retransmission — counted as an under-run, and the
    ///   one case among these that is still allowed to keep the connection
    ///   alive;
    /// * a packet already in the term, which `insert_packet`'s
    ///   "write only into an empty slot" rule refuses.
    #[allow(clippy::too_many_arguments)] // the packet, its place, and the counters
    pub fn insert_packet(
        &mut self,
        term_id: i32,
        term_offset: i32,
        packet: &[u8],
        source: SocketAddr,
        destination: crate::media::receive_endpoint::DestinationId,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> usize {
        if term_id.wrapping_sub(self.initial_term_id) < 0 {
            return 0;
        }

        if self.invalidation_reason.is_some() {
            return 0;
        }

        let term_length = self.term_length.unsigned_abs() as i32;
        let Some(payload_length) = validate_packet(term_length, term_offset, packet) else {
            system.increment(system_counters::id::INVALID_PACKETS);
            return 0;
        };

        let is_heartbeat = payload_length == 0;
        let packet_position = Position::new(
            term_id,
            term_offset,
            self.position_bits_to_shift,
            self.initial_term_id,
        )
        .raw();
        let proposed_position = packet_position + i64::from(payload_length);

        if proposed_position > self.last_overrun_threshold {
            // An over-run: the sender ignored the window this endpoint
            // advertised. Counted, not written (`is_flow_control_over_run`).
            system.increment(system_counters::id::FLOW_CONTROL_OVER_RUNS);
            return 0;
        }

        if is_heartbeat {
            let window_bottom = (self.last_sm_position - i64::from(term_length)).max(0);

            if packet_position >= window_bottom {
                self.track_connection(None, source, destination, now_ns);
                self.time_of_last_packet_ns = now_ns;
                self.on_heartbeat(packet, packet_position, counters, regions, system);
            } else {
                system.increment(system_counters::id::FLOW_CONTROL_UNDER_RUNS);
            }

            return 0;
        }

        if self.is_flow_control_under_run(packet_position, system) {
            // Behind the last status message: not written, because the term it
            // belongs to may already have been reused and writing it would put
            // a past term's frames back among the current one's. A sender
            // inside the window we advertised may be retransmitting something
            // we do still want, so that much of the band keeps the connection
            // alive (`:841-844`).
            if proposed_position
                >= self.last_sm_position - i64::from(self.max_receiver_window_length)
            {
                self.track_connection(None, source, destination, now_ns);
            }
        } else {
            self.track_connection(None, source, destination, now_ns);
            self.time_of_last_packet_ns = now_ns;

            let index = Position::from_raw(packet_position).index(self.position_bits_to_shift);
            let term_offset =
                Position::from_raw(packet_position).term_offset(self.position_bits_to_shift);
            let written = usize::try_from(term_offset.unsigned_abs()).unwrap_or(0);

            if let Some(term) = self.log.term(index) {
                let _ = deepmsg_core::logbuffer::repair::insert_packet(&term, written, packet);
            }

            system_counters::propose_max(
                counters,
                regions,
                self.counters.rcv_hwm,
                proposed_position,
            );
        }

        packet.len()
    }

    /// Whether a packet is behind the last status message this image sent, and
    /// the counter that says so.
    ///
    /// `aeron_publication_image_is_flow_control_under_run`
    /// (`aeron_publication_image.h:252-262`) counts *every* packet that falls
    /// behind — not only the ones outside the window we advertised — so the
    /// increment lives in the predicate and both of its callers get it.
    fn is_flow_control_under_run(&self, packet_position: i64, system: &System<'_>) -> bool {
        let under_run = packet_position < self.last_sm_position;

        if under_run {
            system.increment(system_counters::id::FLOW_CONTROL_UNDER_RUNS);
        }

        under_run
    }

    /// What an end-of-stream heartbeat does (`:778-827`, the `is_heartbeat`
    /// arm).
    ///
    /// A heartbeat proposes the position it arrived at and nothing more: the
    /// payload length `validate_packet` returned for it was zero, so the
    /// reference's `proposed_position = packet_position + payload_length`
    /// (`:772`) is the packet's own position, and that is what is proposed at
    /// `:821`. Adding a header's worth on top would advertise a high-water mark
    /// for bytes no sender is going to send, which the loss detector reads as a
    /// gap and reports forever.
    fn on_heartbeat(
        &mut self,
        packet: &[u8],
        packet_position: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        system: &System<'_>,
    ) {
        system.increment(system_counters::id::HEARTBEATS_RECEIVED);

        let Some(header) = FrameHeader::read(packet) else {
            return;
        };

        let _ =
            system_counters::propose_max(counters, regions, self.counters.rcv_hwm, packet_position);

        if header.flags & header_flags::EOS == 0 || self.is_end_of_stream {
            return;
        }

        // The end of the stream: where it ended, and whether it was revoked.
        self.eos_position = packet_position;
        let eos_position = self.find_eos_position();
        self.is_end_of_stream = true;

        if header.flags & header_flags::REVOKED != 0 {
            self.is_revoked = true;
            system.increment(system_counters::id::PUBLICATION_IMAGES_REVOKED);
        }

        if let Some(metadata) = self.log.metadata() {
            let _ =
                metadata.store_i64_release(descriptor::END_OF_STREAM_POSITION_OFFSET, eos_position);

            // `:800`: the byte goes in the **image's** metadata, which is the
            // file a client's `Image` maps — `Image.isPublicationRevoked`
            // reads exactly this byte, and an image told the stream was
            // revoked without it is an image whose reader cannot say why it
            // went away.
            if self.is_revoked {
                let _ = metadata.store_u8_relaxed(descriptor::IS_PUBLICATION_REVOKED_OFFSET, 1);
            }
        }
    }

    /// Where the stream ended: the position of the frame that said so
    /// (`aeron_publication_find_eos_position`, `:616-630`, which takes the
    /// largest of its connections' positions).
    const fn find_eos_position(&self) -> i64 {
        self.eos_position
    }

    /// A destination this image now also hears from
    /// (`aeron_publication_image_add_destination`, `:229-236`).
    ///
    /// When a client adds a destination to a subscription, every image already
    /// running on that subscription's endpoint gets a connection for it
    /// (`aeron_driver_receiver.c:489-495`) — so that the status messages and
    /// NAKs about this stream go to the new source as well as to the ones that
    /// were already there.
    ///
    /// A destination the image already has a connection for is not added
    /// twice, and the identity is the **handle**: one destination is one
    /// socket. The reference reaches the same place from the other side, by
    /// looking the connection up before it calls this
    /// (`aeron_publication_image_track_connection`, `:564-571`), and a handle
    /// is never handed out twice, so a second one here is a caller asking again
    /// rather than a second place answers go.
    ///
    /// `destination` is the socket the new connection answers through — the one
    /// the client's destination opened, which is not the endpoint's first and
    /// must not be confused with it.
    pub fn add_destination(
        &mut self,
        control_address: Option<SocketAddr>,
        destination: crate::media::receive_endpoint::DestinationId,
        now_ns: i64,
    ) {
        if self
            .connections
            .iter()
            .any(|connection| connection.destination == destination)
        {
            return;
        }

        self.connections.push(Connection {
            destination,
            control_address,
            time_of_last_activity_ns: now_ns,
            time_of_last_frame_ns: now_ns,
            is_eos: false,
            eos_position: 0,
        });
    }

    /// Let a destination go (`aeron_publication_image_remove_destination`,
    /// `aeron_publication_image.h:225`).
    ///
    /// The connection goes with it, and that is the reference's rule too: the
    /// reader's side of a destination is its socket, so a destination that has
    /// been taken off the endpoint has no socket left to answer through.
    pub fn remove_destination(
        &mut self,
        destination: crate::media::receive_endpoint::DestinationId,
    ) {
        self.connections
            .retain(|connection| connection.destination != destination);
    }

    /// A `SETUP` from a destination the image may not be able to answer yet
    /// (`aeron_publication_image_add_connection_if_unknown`, `:639-644`).
    ///
    /// This is what a session two publications share looks like from the
    /// image's side: the second publication's `SETUP` arrives on the second
    /// destination, and without a connection for it the image has no socket to
    /// send the status message the second sender is waiting for — so it never
    /// connects and its offers stay refused (`aeron_data_packet_dispatcher.c:499-502`,
    /// `shouldMergeStreamsFromMultiplePublicationsWithSameParams`).
    ///
    /// It is the same connection a data packet would find or make. Only the
    /// address it starts with is different, and the reference's is the same
    /// one: a destination whose channel named a `control=` is answered there
    /// (`:1131-1132`) rather than at whoever happened to write, and the caller
    /// resolves it because the handle knows only which socket it is.
    pub fn add_connection_if_unknown(
        &mut self,
        control_address: SocketAddr,
        source: SocketAddr,
        destination: crate::media::receive_endpoint::DestinationId,
        now_ns: i64,
    ) {
        self.track_connection(Some(control_address), source, destination, now_ns);
    }

    /// The connection a packet arrived on, adding one if this is the first the
    /// image has heard from that destination
    /// (`aeron_publication_image_track_connection`, `:557-592`).
    ///
    /// The lookup is by **destination** (`:564-571`:
    /// `array[i].destination == destination`), not by the address a packet came
    /// from: one destination is one socket and one connection. A second source
    /// writing to a destination the image already answers refreshes the
    /// connection that is there; it does not add a transport, and the count of
    /// transports is what a client reads (`active_transport_count`).
    ///
    /// A connection the image is meeting for the first time starts with the
    /// address the caller resolved for that destination — [`Self::add_destination`]'s
    /// rule, `:1131-1132` — and one that has none yet takes the source this
    /// packet came from (`:585-589`), which is how an implicit-unicast image
    /// learns where to answer.
    ///
    /// The `destination` is the socket the packet came in on: for a connection
    /// the image is meeting for the first time it is what the connection will
    /// answer through, and for one it already has it is the same socket it was
    /// created with.
    fn track_connection(
        &mut self,
        control_address: Option<SocketAddr>,
        source: SocketAddr,
        destination: crate::media::receive_endpoint::DestinationId,
        now_ns: i64,
    ) {
        let index = match self
            .connections
            .iter()
            .position(|connection| connection.destination == destination)
        {
            Some(index) => index,
            None => {
                self.connections.push(Connection {
                    destination,
                    control_address,
                    time_of_last_activity_ns: now_ns,
                    time_of_last_frame_ns: now_ns,
                    is_eos: false,
                    eos_position: 0,
                });

                self.connections.len() - 1
            }
        };

        let connection = &mut self.connections[index];

        if connection.control_address.is_none() {
            connection.control_address = Some(source);
        }

        connection.time_of_last_activity_ns = now_ns;
        connection.time_of_last_frame_ns = now_ns;

        self.time_of_last_packet_ns = now_ns;
    }

    /// Advance what a reader may read, and decide whether a status message is
    /// due (`aeron_publication_image_track_rebuild`, `:481-556`).
    ///
    /// The rule for the position is the whole of the rebuild: a reader may be
    /// moved to the *start of the first gap* and no further, so a subscriber
    /// never sees a hole — it waits at the hole until a retransmission fills it.
    /// What counts as a hole is the loss detector's question
    /// ([`crate::loss_detector`]), and until it has run the position moves only
    /// as far as the contiguous frames go.
    pub fn track_rebuild(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> Option<Gap> {
        let hwm_position = self.hwm_position(counters, regions);

        // Both ends in one pass, and `None` when nobody is reading: there is
        // then no position to advance and no window to offer. Status messages
        // still go out on their timeout, which is what keeps the sender's view
        // of this endpoint alive.
        let (min_sub_pos, max_sub_pos) =
            self.subscribers.active_position_range(counters, regions)?;

        let rcv_pos = counters.value(regions, self.counters.rcv_pos).unwrap_or(0);
        let rebuild_position = rcv_pos.max(max_sub_pos);

        // Where the reader may go, and which hole is in the way
        // (`aeron_publication_image_track_rebuild`'s call to
        // `aeron_loss_detector_scan`, `:519-528`).
        let index = Position::from_raw(rebuild_position).index(self.position_bits_to_shift);
        let scan = match self.log.term(index) {
            Some(term) => self.loss_detector.scan(
                &term.as_read_only(),
                rebuild_position,
                hwm_position,
                self.term_length.unsigned_abs() as i32,
                self.position_bits_to_shift,
                self.initial_term_id,
                now_ns,
            ),
            None => crate::loss_detector::ScanResult {
                rebuild_offset: Position::from_raw(rebuild_position)
                    .term_offset(self.position_bits_to_shift),
                loss_found: false,
                nak: None,
            },
        };

        let term_offset =
            Position::from_raw(rebuild_position).term_offset(self.position_bits_to_shift);
        let rebuilt = (rebuild_position - i64::from(term_offset)) + i64::from(scan.rebuild_offset);

        system_counters::propose_max(counters, regions, self.counters.rcv_pos, rebuilt);

        // The strategy is asked what window this rebuild leaves, and whether
        // the reader has to be told now (`:534-541`). What it is given is the
        // whole of what the reference gives it, `loss_found` included — CUBIC
        // shrinks on a loss and cannot see one from anywhere else.
        let rebuild = self.congestion_control.on_track_rebuild(
            counters,
            regions,
            now_ns,
            min_sub_pos,
            self.next_sm_position,
            hwm_position,
            rebuild_position,
            rebuilt,
            scan.loss_found,
        );

        let window_length = rebuild.window_length;
        let threshold = window_length / 4;

        // Three reasons to send one, and the first two are the reference's
        // (`:543-553`): the strategy said so, the reader has moved a quarter of
        // a window, or the window itself changed — which is how a CUBIC image
        // tells its sender that a loss just cost it half of it.
        if rebuild.should_force_sm
            || min_sub_pos > self.next_sm_position + i64::from(threshold)
            || window_length != self.next_sm_receiver_window_length
        {
            // A term behind the slowest reader is a term that reader is done
            // with, and cleaning it here — in the same breath as the status
            // message that reports it — is what the reference does (`:551`).
            self.clean_buffer_to(min_sub_pos - i64::from(self.term_length.unsigned_abs() as i32));
            self.schedule_status_message(min_sub_pos, window_length, counters, regions, now_ns);
        }

        scan.nak
    }

    /// Note what the next status message should say
    /// (`aeron_publication_image_schedule_status_message`).
    fn schedule_status_message(
        &mut self,
        position: i64,
        window_length: i32,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) {
        self.next_sm_position = position;
        self.next_sm_receiver_window_length = window_length;
        self.sm_change_number += 1;

        let _ = (counters, regions, now_ns);
    }

    /// Zero the terms the readers have finished with, a chunk at a time
    /// (`aeron_publication_image_clean_buffer_to`, `:430-451`).
    ///
    /// Without this a reused term still holds the frames it held a full buffer
    /// ago, and `insert_packet`'s "write only into an empty slot" rule — the
    /// same one the reference's term rebuilder applies
    /// (`aeron_term_rebuilder.h:30`) — refuses to replace them. The reader
    /// then walks the *old* frames and returns messages from three terms back,
    /// with nothing raised and no counter moved.
    ///
    /// Everything past the first eight bytes is zeroed first, and the
    /// frame-length word goes to zero last with a **release**: a reader that
    /// sees a zero length stops, and one that already saw the old length finds
    /// the bytes it describes still untouched. Zeroing the length first would
    /// let a reader see a frame whose body had been cleared underneath it.
    fn clean_buffer_to(&mut self, position: i64) {
        if position <= self.clean_position {
            return;
        }

        let term_length = i64::from(self.term_length.unsigned_abs() as i32);
        let index = Position::from_raw(self.clean_position).index(self.position_bits_to_shift);
        let clean_offset = Position::from_raw(self.clean_position)
            .term_offset(self.position_bits_to_shift)
            .unsigned_abs() as usize;

        let bytes_left_in_term = term_length as usize - clean_offset;
        let bytes_to_clean = (position - self.clean_position) as usize;
        let length = bytes_to_clean.min(bytes_left_in_term);

        let Some(term) = self.log.term(index) else {
            return;
        };

        let body = length.saturating_sub(std::mem::size_of::<i64>());
        if term
            .zero(clean_offset + std::mem::size_of::<i64>(), body)
            .is_none()
        {
            return;
        }

        if term.store_i64_release(clean_offset, 0).is_none() {
            return;
        }

        self.clean_position += length as i64;
    }

    /// Send a status message if one is due
    /// (`aeron_publication_image_send_pending_status_message`, `:862-990`).
    ///
    /// Returns how many were sent. The three reasons to send one are a change
    /// the last one did not report, a timeout, and an image that was rejected —
    /// which is not a status message at all but an `ERR`, since a sender whose
    /// image was refused has to be told.
    ///
    /// # Errors
    ///
    /// The socket's error, for the caller to record.
    pub fn send_pending_status_message(
        &mut self,
        endpoint: &mut crate::media::receive_endpoint::ReceiveChannelEndpoint,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        system: &System<'_>,
        now_ns: i64,
    ) -> std::io::Result<usize> {
        // A requested reset is applied before the deadline is read, which is
        // the whole of what makes it a request rather than a second timer
        // (`:868-874`). The deadline goes to `now_ns - 1` and not to `now_ns`,
        // because the test below is strict.
        if self.is_next_sm_deadline_reset_requested {
            self.is_next_sm_deadline_reset_requested = false;
            self.next_sm_deadline_ns = now_ns - 1;
        }

        let has_timed_out = self.next_sm_deadline_ns < now_ns;

        // A rejected image has nothing to say about positions any more.
        // Everything it sends from here on is the refusal itself, as an `ERR`
        // frame per connection rather than a status message — and it is sent
        // **again on every period that times out**, because a datagram that
        // goes missing must not leave a publisher waiting for a reason nobody
        // will send twice (`aeron_publication_image.c:877-896`).
        //
        // The reference's guard is `is_sm_enabled && next_sm_deadline_ns <
        // now`, where the flag is cleared once the image leaves its active
        // state (`:1394-1400`). This build does not model that flag — the
        // status-message path below has always run on the deadline alone — and
        // it does not have to here: an image that was just rejected is active
        // in both, which is the only state this branch is reached in.
        if self.invalidation_reason.is_some() {
            let mut sent = 0usize;

            if has_timed_out {
                {
                    let reason = self.invalidation_reason.as_deref().unwrap_or_default();

                    for connection in &self.connections {
                        let Some(control_address) = connection.control_address else {
                            continue;
                        };

                        if endpoint
                            .send_error_frame(
                                connection.destination,
                                control_address,
                                self.stream_id,
                                self.session_id,
                                ERROR_CODE_IMAGE_REJECTED,
                                reason,
                                system,
                            )
                            .is_ok()
                        {
                            sent += 1;
                        }
                    }
                }

                self.next_sm_deadline_ns = now_ns + self.sm_timeout_ns;
            }

            return Ok(usize::from(sent > 0));
        }

        if self.connections.is_empty() {
            return Ok(0);
        }

        if self.sm_change_number == self.last_sm_change_number && !has_timed_out {
            return Ok(0);
        }

        // The response setup goes first, and on the same timer as the status
        // message (`:899-923`): it is the answer to a `SETUP` that asked for a
        // response channel, and the publisher is waiting on it before it can
        // complete the handshake, so it is not something to be deferred until
        // the ordinary message is due.
        //
        // The connections it goes to are the ones an ordinary status message
        // would go to, which here is every connection with somewhere to answer
        // — this build never drops a connection, so it applies no liveness test
        // to either loop (`aeron_publication_image_connection_is_alive` has no
        // counterpart here yet).
        if has_timed_out {
            if let Some(response_session_id) = self.response_session_id_to_send() {
                for connection in &self.connections {
                    let Some(control_address) = connection.control_address else {
                        continue;
                    };

                    endpoint.send_response_setup(
                        connection.destination,
                        control_address,
                        self.stream_id,
                        self.session_id,
                        response_session_id,
                    )?;
                }
            }
        }

        let term_id = Position::from_raw(self.next_sm_position)
            .term_id(self.position_bits_to_shift, self.initial_term_id);
        let term_offset =
            Position::from_raw(self.next_sm_position).term_offset(self.position_bits_to_shift);

        let flags = if self.is_sending_eos_sm {
            header_flags::SM_EOS
        } else {
            0
        };

        // One status message per connection that is still there
        // (`aeron_publication_image_send_pending_status_message`, `:901-925`):
        // each receiver is told the position and window it needs to hear, and
        // a connection with nowhere to answer is skipped rather than guessed
        // at.
        let mut sent = 0usize;

        for connection in &self.connections {
            let Some(control_address) = connection.control_address else {
                continue;
            };

            sent += endpoint.send_sm(
                connection.destination,
                control_address,
                self.stream_id,
                self.session_id,
                term_id,
                term_offset,
                self.next_sm_receiver_window_length,
                flags,
            )?;
        }

        if sent > 0 {
            system.increment(system_counters::id::STATUS_MESSAGES_SENT);
        }

        let _ = (counters, regions);

        self.last_sm_position = self.next_sm_position;
        self.last_overrun_threshold = self.next_sm_position + i64::from(self.term_length / 2);
        self.last_sm_change_number = self.sm_change_number;
        self.next_sm_deadline_ns = now_ns + self.sm_timeout_ns;

        // A status message is also the moment the metadata is told how many
        // senders are still there (`aeron_publication_image.c:987`), which is
        // a *release* store in the reference because a client reads it from
        // another process (`AERON_SET_RELEASE`).
        self.publish_active_transport_count(now_ns);

        Ok(usize::from(sent > 0))
    }

    /// Write [`Self::active_transport_count`] into the log buffer's metadata,
    /// which is where a client reads it.
    pub fn publish_active_transport_count(&mut self, now_ns: i64) {
        let count = self.active_transport_count(now_ns);

        if let Some(metadata) = self.log.metadata() {
            let _ = metadata.store_i32_relaxed(descriptor::ACTIVE_TRANSPORT_COUNT_OFFSET, count);
        }
    }

    /// Ask every live connection to measure a round trip
    /// (`aeron_publication_image_initiate_rttm`, `:1075-1110`).
    ///
    /// Three conditions, all the reference's. The strategy has to want a
    /// measurement at all — `should_measure_rtt`, which is false for the static
    /// window and false for CUBIC until a setting turns it on. The connection
    /// has to be **alive**: a destination that has an address to answer at, and
    /// has been heard from within `AERON_RECEIVE_DESTINATION_TIMEOUT_NS`
    /// (`:632-637`). And the request goes out **through the destination it
    /// measures**, with the `REPLY` flag, so the answer comes back the way the
    /// data does.
    ///
    /// The flag is the whole of the protocol: an RTTM that arrives with it is a
    /// request (the far end echoes the timestamp), and one that arrives without
    /// it is an answer, which is what [`Self::on_rttm`] measures.
    pub fn initiate_rttm(
        &mut self,
        endpoint: &mut ReceiveChannelEndpoint,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> usize {
        let _ = (counters, regions);

        if !self.congestion_control.should_measure_rtt(now_ns) {
            return 0;
        }

        let mut work = 0;

        for index in 0..self.connections.len() {
            let destination = self.connections[index].destination;
            let Some(control_address) = self.connections[index].control_address else {
                continue;
            };

            if !Self::connection_is_alive(&self.connections[index], now_ns) {
                continue;
            }

            if endpoint
                .send_rttm(
                    destination,
                    control_address,
                    self.stream_id,
                    self.session_id,
                    now_ns,
                    0,
                    crate::protocol::header_flags::RTTM_REPLY,
                )
                .is_ok()
            {
                self.congestion_control.on_rttm_sent(now_ns);
                work += 1;
            }
        }

        work
    }

    /// A measurement came back (`aeron_publication_image_on_rttm`, `:849-857`):
    /// the round trip is what has passed since the timestamp the **far end**
    /// echoed, less the delta it reported for its own handling.
    ///
    /// Wrapping arithmetic, as the C's is: both numbers come off the wire and
    /// neither is this driver's clock.
    pub fn on_rttm(
        &mut self,
        frame: &crate::protocol::RttmFrame,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) {
        let rtt_in_ns = now_ns
            .wrapping_sub(frame.echo_timestamp)
            .wrapping_sub(frame.reception_delta);

        self.congestion_control
            .on_rttm(counters, regions, now_ns, rtt_in_ns);
    }

    /// Whether a connection is one an RTTM may be sent through
    /// (`aeron_publication_image_connection_is_alive`, `:632-637`).
    fn connection_is_alive(connection: &Connection, now_ns: i64) -> bool {
        connection.control_address.is_some()
            && now_ns
                < connection
                    .time_of_last_activity_ns
                    .wrapping_add(RECEIVE_DESTINATION_TIMEOUT_NS)
    }

    /// How many senders have been heard from recently
    /// (`aeron_update_active_transport_count`,
    /// `aeron_publication_image.c:40-57`).
    ///
    /// It counts **connections**, and a connection is a destination — not an
    /// interface, and not a source address. That matters most for a group: a
    /// multicast channel builds exactly one destination
    /// (`aeron_driver_conductor.c:2099-2132`), so a multicast image's count is
    /// **zero or one** however many members are reading it, and it is one only
    /// while frames are still arriving. What the number is for is the client's
    /// answer to "is anybody publishing to me", which is why the test is the
    /// image's own liveness timeout rather than "has it ever been seen".
    pub fn active_transport_count(&self, now_ns: i64) -> i32 {
        let active = self
            .connections
            .iter()
            .filter(|connection| {
                now_ns
                    < connection
                        .time_of_last_frame_ns
                        .saturating_add(self.liveness_timeout_ns)
            })
            .count();

        i32::try_from(active).unwrap_or(i32::MAX)
    }

    /// The untethered subscriptions' state machine
    /// (`aeron_publication_image_check_untethered_subscriptions`, `:1165-1283`).
    ///
    /// A **tethered** reader — which is the default — is never put aside: the
    /// loop below only advances its timestamp. An untethered one is the
    /// publisher's escape valve: a reader that falls more than a window behind
    /// the fastest one is put down (so it stops holding the stream back), and
    /// then either woken when it catches up on time or closed if it was not
    /// rejoining.
    ///
    /// The window the "behind" test uses is the *image's* own advertised
    /// window, and the reader's allowance is three quarters of it — a reader is
    /// late when it is a full window behind the fastest reader and a quarter
    /// of a window past that again
    /// (`untethered_window_limit = (max_sub_pos - window) + window / 4`).
    pub fn check_untethered_subscriptions(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> Vec<UntetheredEvent> {
        let mut events = Vec::new();

        let max_sub_pos = self
            .subscribers
            .max_active_position(counters, regions)
            .unwrap_or(0);

        let window_length = i64::from(self.next_sm_receiver_window_length);
        let untethered_window_limit = (max_sub_pos - window_length) + (window_length / 4);

        let positions = self.subscribers.positions().to_vec();

        for position in positions {
            if position.is_tether {
                // A tethered reader keeps its claim on the stream whatever it
                // does; only its timestamp moves.
                let _ = self
                    .subscribers
                    .set_state(position.counter_id, position.state, now_ns);
                continue;
            }

            let current = counters.value(regions, position.counter_id).unwrap_or(0);

            match position.state {
                TetherState::Active => {
                    if current > untethered_window_limit {
                        let _ = self.subscribers.set_state(
                            position.counter_id,
                            TetherState::Active,
                            now_ns,
                        );
                    } else if now_ns
                        > position.time_of_last_update_ns + self.untethered_window_limit_timeout_ns
                    {
                        events.push(UntetheredEvent::Unavailable {
                            subscription_registration_id: position.subscription_registration_id,
                            counter_id: position.counter_id,
                        });

                        let _ = self.subscribers.set_state(
                            position.counter_id,
                            TetherState::Linger,
                            now_ns,
                        );
                    }
                }
                TetherState::Linger => {
                    if now_ns > position.time_of_last_update_ns + self.untethered_linger_timeout_ns
                    {
                        if position.is_rejoin {
                            let _ = self.subscribers.set_state(
                                position.counter_id,
                                TetherState::Resting,
                                now_ns,
                            );
                        } else {
                            let _ = self.subscribers.set_state(
                                position.counter_id,
                                TetherState::Closed,
                                now_ns,
                            );

                            // The position stays where it is with an id that
                            // is no longer a counter: the reference's
                            // `AERON_NULL_COUNTER_ID` (`:1225-1240`). Removing
                            // it here would be a second way for a reader to
                            // leave this set, and the two would have to agree
                            // about the hooks, the count and the order.
                            let _ = self.subscribers.clear_counter_id(position.counter_id);

                            events.push(UntetheredEvent::Closed {
                                counter_id: position.counter_id,
                            });
                        }
                    }
                }
                TetherState::Resting => {
                    if now_ns > position.time_of_last_update_ns + self.untethered_resting_timeout_ns
                    {
                        let join_position = self
                            .subscribers
                            .min_active_position(counters, regions)
                            .unwrap_or(0);

                        let _ = counters.set_value(regions, position.counter_id, join_position);
                        let _ = self.subscribers.set_state(
                            position.counter_id,
                            TetherState::Active,
                            now_ns,
                        );

                        events.push(UntetheredEvent::Available {
                            subscription_registration_id: position.subscription_registration_id,
                            counter_id: position.counter_id,
                            join_position,
                        });
                    }
                }
                TetherState::Closed => {}
            }
        }

        events
    }

    /// A time event for this image (`on_time_event`, `:1294-1355`).
    ///
    /// The three states are the whole of an image's life after it has been
    /// built, and the two transitions that matter are both "the sender stopped
    /// talking": an image with no reader, or one that has gone quiet past its
    /// liveness timeout, starts draining; a drained one waits a few status
    /// message periods for its readers and then lingers.
    ///
    /// Returns whether the state changed.
    pub fn on_time_event(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> bool {
        match self.state {
            ImageState::Active => {
                // `:1307-1311`: a revoked publication takes the image out of
                // ACTIVE at once. It is the one case that does not wait for the
                // stream to go quiet — the sender has said the stream is over
                // and a reader that keeps waiting is reading nothing.
                if self.is_revoked {
                    self.state = ImageState::Draining;
                    self.time_of_last_state_change_ns = now_ns;
                    self.is_sending_eos_sm = true;
                    return true;
                }

                let quiet = now_ns > self.time_of_last_packet_ns + self.liveness_timeout_ns;
                let drained = self.is_end_of_stream && self.is_drained(counters, regions);

                if (self.has_been_linked && !self.has_subscribers()) || quiet || drained {
                    self.state = ImageState::Draining;
                    self.time_of_last_state_change_ns = now_ns;
                    self.is_sending_eos_sm = true;
                    return true;
                }
            }
            ImageState::Draining => {
                let waited =
                    self.time_of_last_state_change_ns + IMAGE_SM_EOS_MULTIPLE * self.sm_timeout_ns;
                let done = self.is_drained(counters, regions) && now_ns > waited;

                if done || self.is_revoked {
                    if self.is_revoked {
                        self.is_sending_eos_sm = true;
                    }

                    // The transition a reader hears about, and only the
                    // ordinary one: a revoked image says so by its heartbeats
                    // and is announced when it is released.
                    self.linger_notice = !self.is_revoked;
                    self.state = ImageState::Linger;
                    self.time_of_last_state_change_ns = now_ns;
                    return true;
                }
            }
            ImageState::Linger => {
                let expired = now_ns > self.time_of_last_state_change_ns + self.liveness_timeout_ns;

                // A revoked image lingers for nobody: this build tells the
                // readers the image is gone when it reaches DONE rather than on
                // the way into LINGER, so a revoked one has to get there
                // without waiting out a liveness window that exists for
                // senders that might come back. A revoked one will not.
                if self.is_revoked || !self.has_subscribers() || expired {
                    self.state = ImageState::Done;
                    return true;
                }
            }
            ImageState::Done => {}
        }

        false
    }

    /// Whether this image has just left DRAINING for LINGER, once
    /// (`aeron_driver_conductor_image_transition_to_linger`, `:1642-1675`).
    pub fn take_linger_notice(&mut self) -> bool {
        std::mem::take(&mut self.linger_notice)
    }

    /// Reject the image, with the reason (`aeron_publication_image_invalidate`,
    /// `:1383-1389`): a sender whose image could not be built is told with an
    /// `ERR` frame rather than left sending into silence.
    ///
    /// All this does is keep the words. The frames go out from
    /// [`Self::send_pending_status_message`], on the status message's own
    /// timer — the reference splits it the same way, and for the same reason:
    /// this runs on the thread that *decides*, and that one runs on the thread
    /// that *sends*.
    pub fn invalidate(&mut self, reason: &[u8]) {
        self.invalidation_reason = Some(reason.to_vec());
    }

    /// The reason this image was rejected, if it was.
    pub fn invalidation_reason(&self) -> Option<&[u8]> {
        self.invalidation_reason.as_deref()
    }

    /// The window this image offers, which a reader's congestion control would
    /// read back.
    pub const fn window_length(&self) -> i32 {
        self.next_sm_receiver_window_length
    }

    /// The initial window, for a caller that wants to report the configured
    /// value rather than the current one.
    pub fn initial_window_length(&self) -> i32 {
        self.congestion_control.initial_window_length()
    }

    /// The strategy this image runs, for a caller that has to know what it took
    /// — the conductor, which gives an image's counters back
    /// (`aeron_cubic_…_fini`, `aeron_congestion_control.c:342-352`).
    pub const fn congestion_control(&self) -> &CongestionControl {
        &self.congestion_control
    }

    /// The term length, as the metadata holds it.
    pub const fn term_length(&self) -> i32 {
        self.term_length
    }

    /// The end-of-stream position the metadata carries.
    pub fn end_of_stream_position(&self) -> Option<i64> {
        self.log.metadata().and_then(|metadata| {
            metadata.load_i64_acquire(descriptor::END_OF_STREAM_POSITION_OFFSET)
        })
    }
}

/// A `Subscribable` whose hooks do nothing: an image has no `is_connected` byte
/// to write, because that byte describes a *publication's* socket
/// (`aeron_ipc_publication.h:130-144` is the only hook the reference installs
/// this way).
struct NoHooks;

impl crate::subscribable::SubscribableHooks for NoHooks {
    fn position_added(&mut self, _position: &TetherablePosition) {}

    fn position_removed(&mut self, _position: &TetherablePosition, _working_before: usize) {}
}

/// `aeron_publication_image_validate_packet` (`:645-738`), minus the
/// timestamping an ATS channel would add.
///
/// Returns how far into the term the packet's frames reach: zero for a
/// heartbeat, the end of the last frame it carried when every frame in it is
/// contiguous and complete, and `None` when the packet is not one an image may
/// take.
///
/// The return is an offset rather than the packet's length because of the PAD
/// that fills out the end of a term: the last datagram of a term carries a
/// final DATA frame and the PAD behind it, and what the caller advances its
/// high-water mark by is where the frames end, not how many bytes arrived
/// (`:732-737`).
fn validate_packet(term_length: i32, term_offset: i32, packet: &[u8]) -> Option<i32> {
    if term_offset < 0
        || term_offset >= term_length
        || term_offset % crate::protocol::FRAME_ALIGNMENT as i32 != 0
    {
        return None;
    }

    // A heartbeat is a whole data header with a frame length of zero.
    if is_heartbeat(packet) {
        return Some(0);
    }

    let mut offset = 0usize;
    let mut next_offset = i64::from(term_offset);
    let mut last_type = -1i16;

    // The guard is a whole data header, not just a frame header (`:730`): a
    // frame that cannot hold one is a trailing fragment, not a frame.
    while offset + DataFrame::LENGTH <= packet.len() {
        let frame = FrameHeader::read(&packet[offset..])?;

        if frame.frame_length <= 0 {
            break;
        }

        last_type = frame.frame_type;

        // Only DATA and PAD belong in a term (`0 == (frame_type & 0xFFFE)`,
        // `:685-688`). A PAD is a data header with a zero payload, so the term
        // offset below reads out of the same place in either type — which is
        // why it is not taken through the type-gated [`DataFrame::read`].
        if frame.frame_type != crate::protocol::frame_type::DATA
            && frame.frame_type != crate::protocol::frame_type::PAD
        {
            break;
        }

        let frame_term_offset = DataFrame::term_offset_of(&packet[offset..])?;
        if i64::from(frame_term_offset) != next_offset {
            break;
        }

        let aligned = i64::from(deepmsg_core::logbuffer::position::align_up(
            frame.frame_length,
            crate::protocol::FRAME_ALIGNMENT as i32,
        ));
        next_offset += aligned;

        if next_offset > i64::from(term_length) {
            break;
        }

        offset += usize::try_from(aligned).unwrap_or(usize::MAX);
    }

    if offset != packet.len()
        && (offset < packet.len() || last_type != crate::protocol::frame_type::PAD)
    {
        return None;
    }

    i32::try_from(offset).ok()
}

/// Whether a packet is a heartbeat: a data header whose frame length says the
/// frame carries nothing (`aeron_publication_image_is_heartbeat`,
/// `aeron_publication_image.h:237-240`).
///
/// The length is exact, not a minimum — `AERON_DATA_HEADER_LENGTH == length`
/// (`:239`). A longer packet whose first frame length is zero is a packet with
/// trailing bytes, which the caller refuses as invalid rather than reading as
/// a heartbeat.
fn is_heartbeat(packet: &[u8]) -> bool {
    let Some(header) = FrameHeader::read(packet) else {
        return false;
    };

    packet.len() == DataFrame::LENGTH && header.frame_length == 0
}

/// The raw tails an image's log holds, for a caller that wants to see them.
pub fn tail_of(log: &LogFile, index: usize) -> Option<RawTail> {
    let metadata = log.metadata()?;
    let offset =
        descriptor::TERM_TAIL_COUNTERS_OFFSET + index * descriptor::TERM_TAIL_COUNTER_STRIDE;

    metadata.load_i64_acquire(offset).map(RawTail::from_raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::congestion_control::{CongestionControl, Strategy};
    use crate::media::receive_endpoint::DestinationId;
    use crate::protocol::FrameHeader;
    use crate::subscribable::TetherState;
    use deepmsg_core::buffer::AtomicBuffer;
    use deepmsg_core::logbuffer::frame::Frame;

    const TERM_LENGTH: i32 = 64 * 1024;

    /// The window the fixture's channel names, in its `rcv-wnd=`. It is
    /// deliberately **larger than half a term**, so that the two values the
    /// window has — the channel's and the one an image may advertise — are
    /// different numbers a test can tell apart.
    const CHANNEL_WINDOW: i32 = 128 * 1024;
    const SESSION_ID: i32 = 42;
    const STREAM_ID: i32 = 1001;
    const INITIAL_TERM_ID: i32 = 1_000;

    /// The two counter regions, held so that a test can build a view over them
    /// per call — the same fixture shape the rest of this crate's tests use,
    /// because a view borrows the bytes and cannot be stored beside them.
    struct Counters {
        metadata: Vec<u8>,
        values: Vec<u8>,
    }

    impl Counters {
        fn new() -> Self {
            Self {
                metadata: vec![0u8; 64 * 1024 * 4],
                values: vec![0u8; 64 * 1024],
            }
        }

        fn open(&mut self) -> CounterRegions<'_> {
            CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.values).expect("aligned"),
            )
            .expect("four-to-one")
        }
    }

    /// The strategy the fixture's image runs, built the way the conductor
    /// builds one (`CongestionControl::create`) over the fixture's own counter
    /// manager — so a CUBIC image here has real `rcv-cc-cubic-*` counters to
    /// write into.
    fn strategy_for(
        strategy: Strategy,
        counters: &mut CounterManager,
        holder: &mut Counters,
        term_length: i32,
    ) -> CongestionControl {
        let regions = holder.open();

        CongestionControl::create(
            strategy,
            &crate::config::DriverConfig::default(),
            counters,
            &regions,
            7,
            SESSION_ID,
            STREAM_ID,
            b"aeron:udp?endpoint=127.0.0.1:40123",
            1408,
            term_length,
            CHANNEL_WINDOW,
            0,
            0,
        )
        .expect("a strategy")
    }

    /// The directory an image's log buffer lands in, removed when it goes.
    struct TempDir(std::path::PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct Fixture {
        _dir: TempDir,
        image: PublicationImage,
        counters: CounterManager,
        holder: Counters,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with(false, false)
        }

        /// The same, with the two bytes the channel and the `SETUP` decide set
        /// rather than absent (`aeron_publication_image.c:277-278`).
        fn with(group_semantics: bool, is_response: bool) -> Self {
            Self::with_nak_delay(group_semantics, is_response, None)
        }

        /// The same, with the channel naming a `nak-delay` — the one parameter
        /// a subscription can say about its own gap timer
        /// (`aeron_publication_image.c:100-118`).
        fn with_nak_delay(
            group_semantics: bool,
            is_response: bool,
            nak_delay_ns: Option<i64>,
        ) -> Self {
            Self::build(group_semantics, is_response, nak_delay_ns, true, true)
        }

        /// The same, for a channel that said `reliable=false`
        /// (`aeron_publication_image.c:92-95`, `:1024`).
        fn unreliable() -> Self {
            Self::build(false, false, None, false, true)
        }

        /// The same, for a channel that said `sparse=false` — the other
        /// parameter whose byte an image copies out of a subscription
        /// (`aeron_publication_image.c:281`).
        fn dense() -> Self {
            Self::build(false, false, None, true, false)
        }

        /// The same image under a **CUBIC** strategy, for the tests that need
        /// a window that moves.
        fn cubic() -> Self {
            Self::build_with(Strategy::Cubic, false, false, None, true, true)
        }

        fn build(
            group_semantics: bool,
            is_response: bool,
            nak_delay_ns: Option<i64>,
            is_reliable: bool,
            is_sparse: bool,
        ) -> Self {
            Self::build_with(
                Strategy::Static,
                group_semantics,
                is_response,
                nak_delay_ns,
                is_reliable,
                is_sparse,
            )
        }

        fn build_with(
            strategy: Strategy,
            group_semantics: bool,
            is_response: bool,
            nak_delay_ns: Option<i64>,
            is_reliable: bool,
            is_sparse: bool,
        ) -> Self {
            static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("deepmsg-image-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("a temp directory");

            let log = deepmsg_core::logbuffer::logfile::LogFile::create(
                &dir.join("image.logbuffer"),
                TERM_LENGTH,
                4096,
                false,
            )
            .expect("a log buffer");

            let mut holder = Counters::new();
            let mut counters = CounterManager::new(64 * 1024, 1_000).expect("room");
            let (rcv_hwm, rcv_pos, rcv_naks_sent) = {
                let regions = holder.open();

                let hwm = counters
                    .allocate(&regions, 3, &[], b"rcv-hwm", 1)
                    .expect("a counter");
                let pos = counters
                    .allocate(&regions, 5, &[], b"rcv-pos", 1)
                    .expect("a counter");
                let naks = counters
                    .allocate(&regions, 20, &[], b"rcv-naks-sent", 1)
                    .expect("a counter");

                (hwm, pos, naks)
            };

            let setup = crate::protocol::SetupFrame {
                term_offset: 0,
                session_id: SESSION_ID,
                stream_id: STREAM_ID,
                initial_term_id: INITIAL_TERM_ID,
                active_term_id: INITIAL_TERM_ID,
                term_length: TERM_LENGTH,
                mtu: 1408,
                ttl: 0,
            };

            let uri = "aeron:udp?endpoint=127.0.0.1:40123";
            let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
            let channel = crate::udp_channel::UdpChannel::resolve(uri.as_bytes(), &parsed)
                .expect("a channel");

            let mut untethered = crate::publication_params::SubscriptionParams::defaults(
                &crate::config::DriverConfig::default(),
            );
            untethered.is_response = is_response;
            untethered.nak_delay_ns = nak_delay_ns;
            untethered.is_reliable = is_reliable;
            untethered.is_sparse = is_sparse;

            let image = PublicationImage::create(
                7,
                1,
                DestinationId::FIRST,
                &channel.original_uri,
                Box::new(log),
                &setup,
                "127.0.0.1:5555".parse().expect("an address"),
                "127.0.0.1:5555".parse().expect("an address"),
                ImageCounters {
                    rcv_hwm,
                    rcv_pos,
                    rcv_naks_sent,
                },
                strategy_for(strategy, &mut counters, &mut holder, TERM_LENGTH),
                // The channel's own window, uncut: what the metadata carries.
                CHANNEL_WINDOW,
                STATUS_MESSAGE_TIMEOUT_NS,
                IMAGE_LIVENESS_TIMEOUT_NS,
                4096,
                untethered,
                group_semantics,
                crate::loss_detector::MulticastBackoff::new(
                    crate::config::NAK_MULTICAST_GROUP_SIZE_DEFAULT,
                    crate::config::NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT,
                ),
                crate::loss_detector::NAK_UNICAST_DELAY_NS,
                0,
            );

            Self {
                _dir: TempDir(dir),
                image,
                counters,
                holder,
            }
        }
    }

    #[test]
    fn an_image_asks_for_gaps_at_the_delay_its_channel_named() {
        // The step between the parameter being *read* and the parameter doing
        // anything: a `nak-delay` that reaches the subscription and stops there
        // changes no behaviour at all, which is exactly the state this slice
        // exists to end. Everything up to here is tested elsewhere — the
        // parser, and the detector's own arithmetic — and neither notices if
        // this line is missing.
        let named = Fixture::with_nak_delay(false, false, Some(2_000_000));
        assert_eq!(
            (2_000_000, 200_000_000),
            named.image.loss_detector.delays(),
            "the channel's delay, and its retry at the driver's ratio"
        );

        let silent = Fixture::with_nak_delay(false, false, None);
        assert_eq!(
            (
                crate::loss_detector::NAK_UNICAST_DELAY_NS,
                crate::loss_detector::NAK_UNICAST_DELAY_NS
                    * crate::loss_detector::NAK_UNICAST_RETRY_RATIO
            ),
            silent.image.loss_detector.delays(),
            "and a channel that named nothing keeps the driver's own"
        );
    }

    /// A channel that said `reliable=false`, followed all the way to what the
    /// image *does* with it.
    ///
    /// This is the shape G1-4 had to learn: `reliable` has been parsed into
    /// [`SubscriptionParams`] since P1 and read by nobody, so a test of the
    /// parser, or of the metadata byte, or of the detector, can each pass while
    /// the image behaves exactly as it did before. The three assertions here
    /// are the three places the one parameter has to arrive — and the third,
    /// [`PublicationImage::fill_gap`], is the behaviour itself: an unreliable
    /// image is the one that covers a hole rather than asking for it.
    #[test]
    fn an_unreliable_image_waits_for_nothing_and_fills_its_own_holes() {
        let mut fixture = Fixture::unreliable();

        assert!(!fixture.image.is_reliable());

        // Zero and zero: the hole is fillable the moment it is seen, and the
        // reference returns this generator before it reads `nak-delay=`
        // (`aeron_publication_image.c:92-95`).
        assert_eq!((0, 0), fixture.image.loss_detector.delays());

        // And the fill is the image's own: the hole it was handed becomes a
        // padding frame in its term, which is what lets a reader past it.
        let gap = Gap {
            term_id: INITIAL_TERM_ID,
            term_offset: 64,
            length: 128,
        };
        assert!(fixture.image.fill_gap(gap));

        let term = fixture.image.log.term(0).expect("a term");
        let filled = deepmsg_core::logbuffer::frame::Frame::new(&term, 64);
        assert_eq!(Some(128), filled.frame_length());
        assert!(filled.is_padding());
        assert_eq!(
            Some(INITIAL_TERM_ID),
            filled.term_id(),
            "the padding carries the term it is in, not the template's"
        );
    }

    /// The same parameters, in the file: the two bytes a client that maps the
    /// image reads to decide how the stream it is looking at behaves and how
    /// its buffer was written (`aeron_publication_image.c:280-281`).
    ///
    /// The pair is the judgement again — one image of each kind — so a
    /// hardcoded answer satisfies neither half. It was hardcoded before: an
    /// image wrote `reliable=true, sparse=false` whatever its channel said,
    /// which is `true` for the first and the *opposite* of the driver's own
    /// default for the second (`TERM_BUFFER_SPARSE_FILE_DEFAULT`).
    #[test]
    fn an_image_records_whether_it_is_reliable_and_whether_its_buffer_is_sparse() {
        for (is_reliable, is_sparse, fixture) in [
            (true, true, Fixture::new()),
            (false, true, Fixture::unreliable()),
            (true, false, Fixture::dense()),
        ] {
            let bytes =
                std::fs::read(fixture._dir.0.join("image.logbuffer")).expect("the log buffer");
            let block =
                &bytes[deepmsg_core::logbuffer::logfile::LogFile::log_length(TERM_LENGTH, 4096)
                    .expect("a length")
                    - descriptor::METADATA_LENGTH..];

            assert_eq!(
                7,
                i64::from_le_bytes(
                    block[descriptor::CORRELATION_ID_OFFSET..descriptor::CORRELATION_ID_OFFSET + 8]
                        .try_into()
                        .expect("eight bytes")
                ),
                "the block being read has to be the one this image wrote"
            );
            assert_eq!(
                u8::from(is_reliable),
                block[descriptor::RELIABLE_OFFSET],
                "reliable={is_reliable} has to reach the metadata"
            );
            assert_eq!(
                u8::from(is_sparse),
                block[descriptor::SPARSE_OFFSET],
                "sparse={is_sparse} has to reach the metadata"
            );
        }
    }

    /// The two metadata bytes that belong to the channel and the `SETUP`
    /// rather than to the image: whether this stream is one of a group, and
    /// whether this subscription exists to carry the answers to a request
    /// (`aeron_publication_image.c:277-278`, which writes `treat_as_multicast`
    /// and `params.is_response`).
    ///
    /// Read back off the file rather than off the image, because the file is
    /// the contract: it is what a reader that maps the image sees, and the only
    /// place a tool that reads bytes can learn either fact. The pair is the
    /// judgement — one fixture that is neither, one that is both — so a
    /// hardcoded answer satisfies neither half.
    #[test]
    fn an_image_says_whether_it_is_a_group_and_whether_it_answers_a_request() {
        for (group_semantics, is_response) in [(false, false), (true, true)] {
            let fixture = Fixture::with(group_semantics, is_response);
            let bytes =
                std::fs::read(fixture._dir.0.join("image.logbuffer")).expect("the log buffer");

            // The metadata block sits at the **end** of a log buffer, not the
            // front (`LogFile::create`: the length less the block), so the
            // offsets below are relative to it.
            let block =
                &bytes[deepmsg_core::logbuffer::logfile::LogFile::log_length(TERM_LENGTH, 4096)
                    .expect("a length")
                    - descriptor::METADATA_LENGTH..];

            // The anchor, so that a zero below cannot be "this is not the block
            // the image wrote": the registration id the fixture passes is 7.
            assert_eq!(
                7,
                i64::from_le_bytes(
                    block[descriptor::CORRELATION_ID_OFFSET..descriptor::CORRELATION_ID_OFFSET + 8]
                        .try_into()
                        .expect("eight bytes")
                ),
                "the block being read has to be the one this image wrote"
            );

            assert_eq!(
                u8::from(group_semantics),
                block[descriptor::GROUP_OFFSET],
                "group_semantics={group_semantics} has to reach the metadata"
            );
            assert_eq!(
                u8::from(is_response),
                block[descriptor::IS_RESPONSE_OFFSET],
                "is_response={is_response} has to reach the metadata"
            );
        }
    }

    /// A data packet carrying one frame, **padded to the aligned frame
    /// length** — which is what a sender puts on the wire and what
    /// `validate_packet` requires: the frames in a datagram are aligned, so a
    /// packet's length is always a multiple of the frame alignment.
    /// A destination a client adds joins every image already running on that
    /// endpoint (`aeron_driver_receiver.c:489-495`), so a stream that is already
    /// up sends its status messages and NAKs to the new source as well.
    ///
    /// A destination is the identity, not the address: the same handle is not
    /// a second connection, a second handle is — even when it answers at the
    /// same address, which is what two clients behind one address look like.
    /// One with no address yet is a connection the first packet to arrive on it
    /// will place.
    #[test]
    fn an_image_takes_a_destination_once() {
        let mut fixture = Fixture::new();
        let before = fixture.image.connections.len();
        let address: SocketAddr = "127.0.0.1:40124".parse().expect("an address");

        let second = DestinationId::for_test(1);

        fixture.image.add_destination(Some(address), second, 1_000);
        assert_eq!(before + 1, fixture.image.connections.len());

        fixture.image.add_destination(Some(address), second, 2_000);
        assert_eq!(
            before + 1,
            fixture.image.connections.len(),
            "the same destination is not a second connection"
        );

        fixture
            .image
            .add_destination(Some(address), DestinationId::for_test(2), 3_000);
        assert_eq!(
            before + 2,
            fixture.image.connections.len(),
            "a second destination is a connection of its own, even at the same \
             address — the address-keyed lookup this replaced collapsed them"
        );

        fixture
            .image
            .add_destination(None, DestinationId::for_test(3), 4_000);
        assert_eq!(
            before + 3,
            fixture.image.connections.len(),
            "one with no address yet is still a connection"
        );
    }

    /// The `SETUP` case: a session the image already serves, arriving on a
    /// destination it has no connection for — which is what two publications of
    /// one session look like from here
    /// (`aeron_publication_image_add_connection_if_unknown`, `:639-644`).
    ///
    /// The connection it takes starts at the address the caller resolved for
    /// that destination, not at the one the `SETUP` came from: a channel that
    /// named a `control=` is answered there (`:1131-1132`), and being answered
    /// is the whole of what the second sender is waiting for — an image that
    /// keeps one socket leaves it offering into a stream nobody hears about.
    #[test]
    fn a_setup_from_a_second_destination_is_a_connection() {
        let mut fixture = Fixture::new();
        let before = fixture.image.connections.len();
        let source: SocketAddr = "127.0.0.1:5556".parse().expect("a source");
        let control: SocketAddr = "127.0.0.1:40125".parse().expect("a control address");
        let second = DestinationId::for_test(1);

        fixture
            .image
            .add_connection_if_unknown(control, source, second, 1_000);

        assert_eq!(before + 1, fixture.image.connections.len());

        let connection = fixture.image.connections.last().expect("a connection");
        assert_eq!(second, connection.destination);
        assert_eq!(
            Some(control),
            connection.control_address,
            "the destination's address, not the one the SETUP came from"
        );

        // A retransmission of the same `SETUP` is not a second connection.
        fixture
            .image
            .add_connection_if_unknown(control, source, second, 2_000);

        assert_eq!(before + 1, fixture.image.connections.len());
        assert_eq!(
            2_000,
            fixture
                .image
                .connections
                .last()
                .expect("a connection")
                .time_of_last_activity_ns,
            "and it is the retransmission's business to keep it alive"
        );
    }

    fn packet(term_id: i32, term_offset: i32, payload: &[u8]) -> Vec<u8> {
        let frame = crate::protocol::DataFrame {
            term_offset,
            session_id: SESSION_ID,
            stream_id: STREAM_ID,
            term_id,
            reserved_value: 0,
        };

        let aligned = deepmsg_core::logbuffer::position::align_up(
            i32::try_from(32 + payload.len()).expect("small"),
            32,
        );
        let mut bytes = vec![0u8; usize::try_from(aligned).expect("small")];
        assert!(
            frame
                .write_with_flags(
                    &mut bytes,
                    crate::protocol::header_flags::BEGIN | crate::protocol::header_flags::END
                )
                .is_some()
        );
        bytes[32..32 + payload.len()].copy_from_slice(payload);

        // The frame's own length, which a writer sets from what it wrote —
        // `DataFrame::write` writes the *fixed* header, whose length is the
        // header's.
        let header = FrameHeader {
            frame_length: i32::try_from(32 + payload.len()).expect("small"),
            version: crate::protocol::VERSION,
            flags: crate::protocol::header_flags::BEGIN | crate::protocol::header_flags::END,
            frame_type: crate::protocol::frame_type::DATA,
        };
        assert!(header.write(&mut bytes).is_some());

        bytes
    }

    /// One frame as it sits in a datagram: a frame header, then the data
    /// header's `term_offset` at `(aeron_udp_protocol.h:59)`.
    ///
    /// `frame_length` is the whole frame, header included. The datagram grows
    /// by the **aligned** length, because the validator advances its read
    /// offset by `AERON_ALIGN(frame_length)` — the frames inside a term are
    /// aligned, so a datagram carries the padding between them
    /// (`aeron_publication_image.c:695`, `:728`).
    fn push_frame(bytes: &mut Vec<u8>, frame_type: i16, frame_length: i32, term_offset: i32) {
        let start = bytes.len();
        bytes.resize(start + 32, 0);

        let header = FrameHeader {
            frame_length,
            version: crate::protocol::VERSION,
            flags: crate::protocol::header_flags::BEGIN | crate::protocol::header_flags::END,
            frame_type,
        };
        assert!(header.write(&mut bytes[start..]).is_some());
        bytes[start + 8..start + 12].copy_from_slice(&term_offset.to_le_bytes());

        let aligned = deepmsg_core::logbuffer::position::align_up(frame_length, 32);
        bytes.resize(start + usize::try_from(aligned).expect("small"), 0);
    }

    /// The last datagram of a term is a DATA frame and the PAD that fills the
    /// term out, in one packet — and the PAD is not a reason to refuse it
    /// (`aeron_publication_image.c:685-688`).
    #[test]
    fn a_data_frame_followed_by_a_pad_is_valid() {
        let mut bytes = Vec::new();
        push_frame(&mut bytes, crate::protocol::frame_type::DATA, 1032, 64416);
        push_frame(&mut bytes, crate::protocol::frame_type::PAD, 64, 65472);

        assert_eq!(bytes.len(), 1120);

        // The PAD reaches the end of the term, so what the packet's frames
        // reach is the term boundary — not the 1120 bytes that arrived, and
        // certainly not the 1032 of the DATA frame on its own.
        assert_eq!(validate_packet(65536, 64416, &bytes), Some(1120));

        // A sender may leave the PAD's fill off the end of the datagram: the
        // frames before it are complete, and the reference accepts a trailing
        // PAD whether or not its fill arrived (`:732-737`).
        assert_eq!(validate_packet(65536, 64416, &bytes[..1088]), Some(1120));
    }

    /// A PAD with nothing in front of it is a packet of its own.
    #[test]
    fn a_pad_alone_is_valid() {
        let mut bytes = Vec::new();
        push_frame(&mut bytes, crate::protocol::frame_type::PAD, 64, 65472);

        assert_eq!(bytes.len(), 64);
        assert_eq!(validate_packet(65536, 65472, &bytes), Some(64));
    }

    /// A term offset that is not on a frame boundary is refused before any
    /// frame is read (`aeron_publication_image.c:655`).
    #[test]
    fn an_unaligned_term_offset_is_refused() {
        let mut bytes = Vec::new();
        push_frame(&mut bytes, crate::protocol::frame_type::DATA, 64, 8);

        assert_eq!(validate_packet(65536, 8, &bytes), None);
    }

    /// A heartbeat is **exactly** one data header with a zero frame length
    /// (`aeron_publication_image.h:239`). A longer packet whose first frame
    /// length is zero is a packet with trailing bytes, and refused — reading it
    /// as a heartbeat would take liveness, and an end of stream, from a packet
    /// nothing vouches for.
    #[test]
    fn a_zero_length_frame_is_a_heartbeat_only_at_the_header_length() {
        assert_eq!(validate_packet(65536, 0, &[0u8; 32]), Some(0));
        assert_eq!(validate_packet(65536, 0, &[0u8; 64]), None);
    }

    /// A reader's position counter, added to the image — and its **id**
    /// returned, because that is what the image tracks: a position held under
    /// the wrong counter is a reader that is not there.
    fn add_reader(fixture: &mut Fixture) -> i32 {
        let regions = fixture.holder.open();

        let counter_id = fixture
            .counters
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let _ = fixture.counters.set_value(&regions, counter_id, 0);

        let position = TetherablePosition {
            counter_id,
            subscription_registration_id: 9,
            time_of_last_update_ns: 0,
            state: TetherState::Active,
            is_tether: true,
            is_rejoin: false,
        };

        fixture.image.add_subscriber(position);

        counter_id
    }

    #[test]
    fn the_active_transport_count_is_the_connections_still_sending() {
        let mut fixture = Fixture::new();
        let _reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();
        let system = System::new(&fixture.counters, &regions);

        // The image is built with one connection, and the reference dates it
        // at the moment it was added (`aeron_publication_image_add_destination`,
        // `:1131-1142`), so the count starts at one rather than zero and falls
        // to zero when that connection goes quiet.
        let timeout = fixture.image.liveness_timeout_ns;
        assert_eq!(1, fixture.image.active_transport_count(0));
        assert_eq!(
            1,
            fixture.image.active_transport_count(timeout - 1),
            "live up to the timeout"
        );
        assert_eq!(
            0,
            fixture.image.active_transport_count(timeout),
            "and gone at it — the comparison is strict — which is the whole \
             of the 0-or-1 a group shows"
        );

        // A frame refreshes it.
        let sent = 1_000;
        let bytes = packet(INITIAL_TERM_ID, 0, b"a frame");
        fixture.image.insert_packet(
            INITIAL_TERM_ID,
            0,
            &bytes,
            "127.0.0.1:5555".parse().expect("an address"),
            DestinationId::FIRST,
            &system,
            &fixture.counters,
            &regions,
            sent,
        );

        assert_eq!(1, fixture.image.active_transport_count(sent + timeout - 1));
        assert_eq!(0, fixture.image.active_transport_count(sent + timeout));

        // A second source writing to the **same destination** is not a second
        // connection: the image answers that socket either way, and it is the
        // destination the connection is keyed by, as the reference keys it
        // (`aeron_publication_image.c:564-571`).
        let offset = bytes.len() as i32;
        let another = packet(INITIAL_TERM_ID, offset, b"another frame");
        fixture.image.insert_packet(
            INITIAL_TERM_ID,
            offset,
            &another,
            "127.0.0.1:5556".parse().expect("an address"),
            DestinationId::FIRST,
            &system,
            &fixture.counters,
            &regions,
            sent,
        );

        assert_eq!(
            1,
            fixture.image.connections.len(),
            "one destination is one transport, whoever writes to it"
        );
        assert_eq!(1, fixture.image.active_transport_count(sent + 1));

        // A second **destination** is a second connection, which is the case
        // the reference's own test asserts the number on
        // (`aeron_publication_image_test.cpp:485`, `:497`, `:503` — its two
        // packets go to `dest_1` and `dest_2`, not to two addresses).
        let third_offset = offset + another.len() as i32;
        fixture.image.insert_packet(
            INITIAL_TERM_ID,
            third_offset,
            &packet(INITIAL_TERM_ID, third_offset, b"a third frame"),
            "127.0.0.1:5556".parse().expect("an address"),
            DestinationId::for_test(1),
            &system,
            &fixture.counters,
            &regions,
            sent,
        );

        assert_eq!(2, fixture.image.connections.len(), "two destinations");
        assert_eq!(2, fixture.image.active_transport_count(sent + 1));
    }

    #[test]
    fn the_active_transport_count_is_written_where_a_client_reads_it() {
        let mut fixture = Fixture::new();
        let timeout = fixture.image.liveness_timeout_ns;

        let read = |fixture: &Fixture| {
            let metadata = fixture.image.log.metadata().expect("a metadata block");
            metadata
                .load_i32_relaxed(descriptor::ACTIVE_TRANSPORT_COUNT_OFFSET)
                .expect("in range")
        };

        // The image is created with the field zeroed (`LogMetadataInit`), so
        // the write is the only thing that moves it.
        assert_eq!(0, read(&fixture), "zero until a status message goes out");

        fixture.image.publish_active_transport_count(0);
        assert_eq!(
            1,
            read(&fixture),
            "the one connection the image was built with"
        );

        fixture.image.publish_active_transport_count(timeout + 1);
        assert_eq!(0, read(&fixture), "and back to zero when it goes quiet");
    }

    #[test]
    fn a_packet_reaches_the_term_and_moves_the_high_water_mark() {
        let mut fixture = Fixture::new();
        let _reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();
        let system = System::new(&fixture.counters, &regions);

        let payload = b"the reference's bytes";
        let bytes = packet(INITIAL_TERM_ID, 0, payload);

        let accepted = fixture.image.insert_packet(
            INITIAL_TERM_ID,
            0,
            &bytes,
            "127.0.0.1:5555".parse().expect("an address"),
            DestinationId::FIRST,
            &system,
            &fixture.counters,
            &regions,
            1_000,
        );

        assert_eq!(bytes.len(), accepted);
        assert_eq!(
            Some(i64::try_from(bytes.len()).expect("small")),
            fixture
                .counters
                .value(&regions, fixture.image.counters.rcv_hwm)
        );

        // And the bytes are in the term, at their offset, with the frame's own
        // header: what a reader maps and reads.
        let term = fixture.image.log.term(0).expect("a term");
        let frame = Frame::new(&term, 0);
        let mut read_back = [0u8; 21];
        frame.copy_payload(&mut read_back).expect("in range");
        assert_eq!(payload, &read_back);

        // A second copy of the same packet neither moves the high-water mark
        // nor rewrites the term: the slot already holds a frame, and
        // `insert_packet`'s rule is that a filled slot is left alone. That is
        // what makes a retransmission idempotent.
        let before = fixture
            .counters
            .value(&regions, fixture.image.counters.rcv_hwm);

        assert!(
            fixture.image.insert_packet(
                INITIAL_TERM_ID,
                0,
                &bytes,
                "127.0.0.1:5555".parse().expect("an address"),
                DestinationId::FIRST,
                &system,
                &fixture.counters,
                &regions,
                1_100,
            ) > 0
        );

        assert_eq!(
            before,
            fixture
                .counters
                .value(&regions, fixture.image.counters.rcv_hwm),
            "the high-water mark does not move for a duplicate"
        );
    }

    #[test]
    fn a_hole_stops_the_reader_until_it_is_filled() {
        let mut fixture = Fixture::new();
        let _reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();
        let system = System::new(&fixture.counters, &regions);
        let source = "127.0.0.1:5555".parse().expect("an address");

        // Two frames a hole apart. Each is 160 bytes aligned, so the frame
        // that was missed would have occupied 160..320 and the next one starts
        // where it would have ended — at 320.
        let first = packet(INITIAL_TERM_ID, 0, &[1u8; 100]);
        let third = packet(INITIAL_TERM_ID, 320, &[3u8; 100]);

        assert!(
            fixture.image.insert_packet(
                INITIAL_TERM_ID,
                0,
                &first,
                source,
                DestinationId::FIRST,
                &system,
                &fixture.counters,
                &regions,
                1_000
            ) > 0
        );
        assert!(
            fixture.image.insert_packet(
                INITIAL_TERM_ID,
                320,
                &third,
                source,
                DestinationId::FIRST,
                &system,
                &fixture.counters,
                &regions,
                1_000
            ) > 0
        );

        // The reader may go as far as the hole and no further, and the hole is
        // asked for once it has waited its delay.
        let gap = fixture
            .image
            .track_rebuild(&fixture.counters, &regions, 1_000)
            .or_else(|| {
                fixture
                    .image
                    .track_rebuild(&fixture.counters, &regions, 1_000 + 1_000_000)
            });

        let gap = gap.expect("a hole is asked for");
        assert_eq!(INITIAL_TERM_ID, gap.term_id);
        assert_eq!(160, gap.term_offset, "where the frame was missed");
        assert_eq!(160, gap.length, "and how much of it is missing");

        assert_eq!(
            Some(160),
            fixture
                .counters
                .value(&regions, fixture.image.counters.rcv_pos),
            "a reader stops at the hole"
        );

        // The retransmission arrives, and the reader moves past the hole — all
        // the way to the end of what has been received.
        let second = packet(INITIAL_TERM_ID, 160, &[2u8; 100]);
        assert!(
            fixture.image.insert_packet(
                INITIAL_TERM_ID,
                160,
                &second,
                source,
                DestinationId::FIRST,
                &system,
                &fixture.counters,
                &regions,
                2_000
            ) > 0
        );

        let _ = fixture
            .image
            .track_rebuild(&fixture.counters, &regions, 2_100);

        assert_eq!(
            Some(480),
            fixture
                .counters
                .value(&regions, fixture.image.counters.rcv_pos),
            "both frames and the retransmitted one"
        );
    }

    #[test]
    fn a_tethered_reader_is_never_put_aside_and_an_untethered_one_is() {
        // The publisher's escape valve: a reader that has asked *not* to be
        // tethered, and falls behind, is put down — and then either woken or
        // closed.
        let mut fixture = Fixture::new();
        let regions = fixture.holder.open();

        // Two readers: one tethered and keeping up, one not tethered and
        // behind. The "behind" test is relative to the *fastest* reader —
        // `(max_sub_pos - window) + window / 4` — which is why a single reader
        // can never be put aside: it is its own fastest reader.
        let tethered = fixture
            .counters
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let untethered = fixture
            .counters
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");

        for (counter_id, is_tether) in [(tethered, true), (untethered, false)] {
            let position = if is_tether { 100_000 } else { 0 };
            let _ = fixture.counters.set_value(&regions, counter_id, position);
            fixture.image.add_subscriber(TetherablePosition {
                counter_id,
                subscription_registration_id: i64::from(counter_id) + 100,
                time_of_last_update_ns: 0,
                state: TetherState::Active,
                is_tether,
                is_rejoin: true,
            });
        }

        // The tethered reader is 100,000 bytes ahead, which is more than a
        // window: the untethered one is behind and moves, the tethered one
        // does not.
        let events = fixture.image.check_untethered_subscriptions(
            &mut fixture.counters,
            &regions,
            fixture.image.untethered_window_limit_timeout_ns + 1,
        );

        assert_eq!(
            vec![UntetheredEvent::Unavailable {
                subscription_registration_id: i64::from(untethered) + 100,
                counter_id: untethered,
            }],
            events,
            "only the untethered reader is put aside"
        );

        assert_eq!(
            TetherState::Active,
            fixture
                .image
                .subscribers
                .find_by_counter(tethered)
                .expect("the tethered reader is still there")
                .state
        );

        // It rests next, being a rejoin: no counter is freed.
        let events = fixture.image.check_untethered_subscriptions(
            &mut fixture.counters,
            &regions,
            fixture.image.untethered_window_limit_timeout_ns
                + fixture.image.untethered_linger_timeout_ns
                + 2,
        );

        assert!(events.is_empty(), "a rejoin rests rather than closing");
        assert_eq!(
            TetherState::Resting,
            fixture
                .image
                .subscribers
                .find_by_counter(untethered)
                .expect("still attached")
                .state
        );

        // And the resting timeout wakes it: the counter is seeded at the join
        // position and the client is told the image is there again.
        let events = fixture.image.check_untethered_subscriptions(
            &mut fixture.counters,
            &regions,
            fixture.image.untethered_window_limit_timeout_ns
                + fixture.image.untethered_linger_timeout_ns
                + fixture.image.untethered_resting_timeout_ns
                + 3,
        );

        assert!(
            matches!(events.as_slice(), [UntetheredEvent::Available { .. }]),
            "a resting reader is woken: {events:?}"
        );
        assert_eq!(
            TetherState::Active,
            fixture
                .image
                .subscribers
                .find_by_counter(untethered)
                .expect("still attached")
                .state
        );
    }

    #[test]
    fn an_untethered_reader_that_is_not_rejoining_is_closed() {
        let mut fixture = Fixture::new();
        let regions = fixture.holder.open();

        let counter_id = fixture
            .counters
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let _ = fixture.counters.set_value(&regions, counter_id, 0);

        // A tethered reader ahead of it, which is what makes it "behind".
        let ahead = fixture
            .counters
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let _ = fixture.counters.set_value(&regions, ahead, 100_000);

        for (id, is_tether, is_rejoin) in [(counter_id, false, false), (ahead, true, false)] {
            fixture.image.add_subscriber(TetherablePosition {
                counter_id: id,
                subscription_registration_id: i64::from(id) + 100,
                time_of_last_update_ns: 0,
                state: TetherState::Active,
                is_tether,
                is_rejoin,
            });
        }

        let _ = fixture.image.check_untethered_subscriptions(
            &mut fixture.counters,
            &regions,
            fixture.image.untethered_window_limit_timeout_ns + 1,
        );

        let events = fixture.image.check_untethered_subscriptions(
            &mut fixture.counters,
            &regions,
            fixture.image.untethered_window_limit_timeout_ns
                + fixture.image.untethered_linger_timeout_ns
                + 2,
        );

        assert_eq!(
            vec![UntetheredEvent::Closed { counter_id }],
            events,
            "a reader that is not rejoining is done"
        );
        // The position stays where it is, with an id that is no longer a
        // counter — the reference's `counter_id = AERON_NULL_COUNTER_ID`
        // (`:1225-1240`). Not leaving the set is the point: the teardown that
        // gives every reader's counter back walks the whole set, so an id left
        // sitting there would be given back twice.
        let closed = fixture
            .image
            .subscribers
            .find_by_subscription(i64::from(counter_id) + 100);

        assert_eq!(1, closed.len(), "the position is still there");
        assert_eq!(TetherState::Closed, closed[0].state);
        assert_eq!(deepmsg_cnc::layout::NULL_COUNTER_ID, closed[0].counter_id);
        assert!(
            fixture
                .image
                .subscribers
                .find_by_counter(counter_id)
                .is_none(),
            "and its counter is no longer one the set points at"
        );
    }

    /// A time event between an image's creation and its first subscription is
    /// the one thing that must not retire it.
    ///
    /// The image is handed to the receiver thread and linked to its
    /// subscription by two separate commands, so a pass can end between them.
    /// The image created in that window has no subscribers because nobody has
    /// been given the chance to add one — not because everyone left — and a
    /// drain here answers a sender that has done nothing wrong with an
    /// end-of-stream (`aeron_driver_conductor.c:6763` links before `:6782`
    /// hands over, which is the ordering this stands in for).
    #[test]
    fn an_image_nobody_has_linked_yet_is_not_drained() {
        let mut fixture = Fixture::new();

        // No time has passed, so the liveness timeout cannot be the clause that
        // decides this: only the one about subscribers can.
        {
            let regions = fixture.holder.open();
            assert!(!fixture.image.on_time_event(&fixture.counters, &regions, 0));
        }
        assert_eq!(
            ImageState::Active,
            fixture.image.state,
            "an image created but not yet linked waits"
        );

        // Once a subscription has been linked, the same clause is the right one
        // again: a reader that goes away does drain the image.
        let reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();

        assert!(!fixture.image.on_time_event(&fixture.counters, &regions, 0));
        assert!(fixture.image.remove_subscriber(reader));
        assert!(
            fixture.image.on_time_event(&fixture.counters, &regions, 0),
            "the last reader leaving is what the clause is for"
        );
        assert_eq!(ImageState::Draining, fixture.image.state);
    }

    /// A revoked image does not wait for anything: `ACTIVE → DRAINING` the
    /// moment the flag is seen (`:1307-1311`), `DRAINING → LINGER` on the same
    /// flag (`:1330-1349`), and out of `LINGER` without waiting out a liveness
    /// window that exists for a sender that might come back — a revoked one
    /// will not.
    #[test]
    fn a_revoked_image_walks_to_done_without_waiting() {
        let mut fixture = Fixture::new();
        let _reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();

        fixture.image.is_revoked = true;

        assert!(fixture.image.on_time_event(&fixture.counters, &regions, 0));
        assert_eq!(ImageState::Draining, fixture.image.state);
        assert!(
            fixture.image.is_sending_eos_sm,
            "and it owes its readers an end-of-stream status message"
        );

        assert!(fixture.image.on_time_event(&fixture.counters, &regions, 0));
        assert_eq!(ImageState::Linger, fixture.image.state);

        assert!(fixture.image.on_time_event(&fixture.counters, &regions, 0));
        assert_eq!(ImageState::Done, fixture.image.state);
    }

    /// A heartbeat proposes the position it arrived at, and nothing on top.
    ///
    /// A frame header's worth added to it would be a high-water mark for eight
    /// bytes no sender is going to send, which the loss detector reads as a gap
    /// — so an idle stream NAKs forever for a packet that does not exist
    /// (`aeron_publication_image.c:772`, `:821`).
    #[test]
    fn a_heartbeat_proposes_its_own_position() {
        let mut fixture = Fixture::new();
        let _reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();
        let system = System::new(&fixture.counters, &regions);

        let term_offset = 640;
        let frame = crate::protocol::DataFrame {
            term_offset,
            session_id: SESSION_ID,
            stream_id: STREAM_ID,
            term_id: INITIAL_TERM_ID,
            reserved_value: 0,
        };
        let mut bytes = [0u8; 32];
        assert!(
            frame
                .write_with_flags(
                    &mut bytes,
                    crate::protocol::header_flags::BEGIN | crate::protocol::header_flags::END
                )
                .is_some()
        );
        let header = FrameHeader {
            frame_length: 0,
            version: crate::protocol::VERSION,
            flags: crate::protocol::header_flags::BEGIN | crate::protocol::header_flags::END,
            frame_type: crate::protocol::frame_type::DATA,
        };
        assert!(header.write(&mut bytes).is_some());

        assert_eq!(
            0,
            fixture.image.insert_packet(
                INITIAL_TERM_ID,
                term_offset,
                &bytes,
                "127.0.0.1:5555".parse().expect("an address"),
                DestinationId::FIRST,
                &system,
                &fixture.counters,
                &regions,
                1_000
            ),
            "a heartbeat carries no bytes to insert"
        );

        assert_eq!(
            i64::from(term_offset),
            fixture.image.hwm_position(&fixture.counters, &regions),
            "the heartbeat's own position, with nothing added on top of it"
        );
    }

    /// The window an image offers is the **channel's**, cut to half a term —
    /// and the metadata block records it **uncut**. Two facts from one call,
    /// and they are two different values on purpose: the strategy cuts what it
    /// advertises (`aeron_receiver_window_length`,
    /// `aeron_congestion_control.c:155-157`) while the block
    /// `aeron_logbuffer_metadata_init` is handed is `params.initial_window_length`
    /// straight (`aeron_publication_image.c:250-262`), which is the channel's
    /// own or the driver's default (`aeron_driver_uri.c:466`, `:502`).
    ///
    /// A build that wrote the cut value into the block would be handing a
    /// reader of an image's metadata a window the image never advertised.
    #[test]
    fn the_window_is_the_channels_and_the_metadata_keeps_it_uncut() {
        let fixture = Fixture::new();

        assert_eq!(
            TERM_LENGTH / 2,
            fixture.image.initial_window_length(),
            "the channel names a window of {CHANNEL_WINDOW} and the term is {TERM_LENGTH}, so \
             half a term is what may be offered"
        );
        let metadata = fixture.image.log.metadata().expect("metadata");

        assert_eq!(
            Some(CHANNEL_WINDOW),
            metadata.load_i32_relaxed(descriptor::RECEIVER_WINDOW_LENGTH_OFFSET),
            "uncut, as the reference writes it"
        );
    }

    /// A CUBIC image is a CUBIC window. The image advertises what its strategy
    /// says (`aeron_publication_image.c:377-380` — it keeps no window of its
    /// own), and with CUBIC that is the congestion window it starts at, not the
    /// channel's `rcv-wnd=` that the static strategy would have offered.
    ///
    /// Ten MTUs, because the channel's window of 128 KiB is cut to half a 64 KiB
    /// term, which is 23 congestion windows — more than `INITIAL_CWND`, so the
    /// start is ten.
    #[test]
    fn a_cubic_image_advertises_the_congestion_window() {
        let fixture = Fixture::cubic();

        assert_eq!(10 * 1408, fixture.image.initial_window_length());
        assert_eq!(
            10 * 1408,
            fixture.image.window_length(),
            "and that is what its next status message carries"
        );
        assert_eq!(
            2,
            fixture.image.congestion_control().counter_ids().len(),
            "with the two counters CUBIC takes"
        );
        assert!(
            fixture.image.initial_window_length() < CHANNEL_WINDOW,
            "which is a different window from the channel's, or this test says nothing"
        );
    }

    /// A measurement that comes back is a round trip: what has passed since the
    /// timestamp the far end echoed, less the delta it reported for its own
    /// handling (`aeron_publication_image.c:849-857`) — and the image hands both
    /// numbers to its strategy, which is where CUBIC keeps the estimate.
    #[test]
    fn an_rttm_that_comes_back_is_the_round_trip_it_measures() {
        let mut fixture = Fixture::cubic();
        let regions = fixture.holder.open();
        let ids = fixture.image.congestion_control().counter_ids().to_vec();

        // The far end echoed a timestamp from 500 µs ago, and spent 20 µs of
        // the trip on its own handling.
        let frame = crate::protocol::RttmFrame {
            session_id: SESSION_ID,
            stream_id: STREAM_ID,
            echo_timestamp: 1_000_000 - 500_000,
            reception_delta: 20_000,
            receiver_id: 1,
        };

        fixture
            .image
            .on_rttm(&frame, &fixture.counters, &regions, 1_000_000);

        assert_eq!(
            480_000,
            fixture
                .counters
                .value(&regions, ids[0])
                .expect("the rtt counter"),
            "500 µs less the 20 µs the far end spent"
        );
    }

    #[test]
    fn an_end_of_stream_heartbeat_ends_the_image() {
        let mut fixture = Fixture::new();
        let _reader = add_reader(&mut fixture);
        let regions = fixture.holder.open();
        let system = System::new(&fixture.counters, &regions);

        // A heartbeat: a whole data header whose frame length is zero, with
        // the end-of-stream flag.
        let frame = crate::protocol::DataFrame {
            term_offset: 0,
            session_id: SESSION_ID,
            stream_id: STREAM_ID,
            term_id: INITIAL_TERM_ID,
            reserved_value: 0,
        };
        let mut bytes = [0u8; 32];
        assert!(
            frame
                .write_with_flags(
                    &mut bytes,
                    crate::protocol::header_flags::BEGIN
                        | crate::protocol::header_flags::END
                        | crate::protocol::header_flags::EOS
                )
                .is_some()
        );
        let header = FrameHeader {
            frame_length: 0,
            version: crate::protocol::VERSION,
            flags: crate::protocol::header_flags::BEGIN
                | crate::protocol::header_flags::END
                | crate::protocol::header_flags::EOS,
            frame_type: crate::protocol::frame_type::DATA,
        };
        assert!(header.write(&mut bytes).is_some());

        assert_eq!(
            0,
            fixture.image.insert_packet(
                INITIAL_TERM_ID,
                0,
                &bytes,
                "127.0.0.1:5555".parse().expect("an address"),
                DestinationId::FIRST,
                &system,
                &fixture.counters,
                &regions,
                1_000
            ),
            "a heartbeat carries no bytes to insert"
        );

        assert!(fixture.image.is_end_of_stream);
        assert!(!fixture.image.is_revoked);
        assert_eq!(
            Some(0),
            fixture.image.end_of_stream_position(),
            "the heartbeat's own position"
        );
    }
}
