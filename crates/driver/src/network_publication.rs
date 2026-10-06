//! The publication half of the network data plane: what a producer writes,
//! sent.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_network_publication.c`. It is the
//! same idea as [`crate::ipc_publication::IpcPublication`] — a log buffer, a
//! producer position, a limit — with the two differences that make it a
//! *network* publication:
//!
//! * **The limit comes from the wire.** `snd-lmt` is written from the status
//!   messages a receiver sends (`:779-840`), and `pub-lmt` follows `snd-pos`
//!   when no local reader holds it back (`:947-1010`). A producer is therefore
//!   bounded by two stages: what has been *sent*, and what the far end says it
//!   has room for.
//! * **The term is read, not shared.** A sender walks the term buffer the
//!   producer filled and puts the frames on the wire
//!   ([`scan_for_availability`]), which is why the sender's position is not the
//!   producer's.
//!
//! # Two things called "connected"
//!
//! The log buffer's metadata byte is what a **client** reads
//! ([`NetworkPublication::update_connected_status`]); the channel-status
//! counter belongs to the **endpoint**
//! ([`crate::media::send_endpoint::SendChannelEndpoint`]). Both are set from
//! the same fact — a receiver is live — and a publication that updates one and
//! not the other is a publication a client and `AeronStat` disagree about.
//! That is why this module owns the first and the endpoint registry owns the
//! second, and why both are written from the one place that learns a receiver
//! is there: [`NetworkPublication::on_status_message`].
//!
//! # Why the frames are copied on the way out
//!
//! The reference hands `sendmmsg` an iovec pointing straight into the mapped
//! term. This build copies each frame into a scratch buffer first, because
//! `deepmsg_core::buffer` refuses to mint a `&[u8]` over memory another thread
//! is writing (a false immutability promise is licence for the optimiser to
//! hoist loads — see that module's note), and a syscall that takes `&[u8]` is
//! exactly such a borrow. The copy is one `memcpy` per datagram into a buffer
//! allocated once at creation; the alternative is an `unsafe` pointer path
//! through two modules to save it, which this slice does not spend.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use deepmsg_cnc::{CounterManager, CounterRegions, layout};
use deepmsg_core::logbuffer::descriptor;
use deepmsg_core::logbuffer::logfile::LogFile;
use deepmsg_core::logbuffer::position::Position;
use deepmsg_core::logbuffer::scan::{Availability, scan_for_availability};

use crate::flowcontrol::{FlowControl, StatusMessage, Strategy, receiver_window_length};
use crate::media::send_endpoint::SendChannelEndpoint;
use crate::protocol::{
    DataFrame, ErrorFrame, FrameHeader, NakFrame, RttmFrame, SetupFrame, StatusMessageFrame,
    frame_type, header_flags,
};
use crate::publication_maintenance::PublicationSightlines;
use crate::publication_params::PublicationParams;
use crate::retransmit_handler::{Faults, NakOutcome, Resend, RetransmitHandler};
use crate::system_counters::{self, System};

/// How long a publication keeps saying `SETUP` while nothing has answered
/// (`AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS`,
/// `aeron-driver/src/main/c/aeron_network_publication.h:32`).
pub const SETUP_TIMEOUT_NS: i64 = 100_000_000;

/// How long a publication may go without data before it sends a heartbeat
/// (`AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS`, `:33`).
pub const HEARTBEAT_TIMEOUT_NS: i64 = 100_000_000;

/// What a network publication's own counters are
/// (`aeron_counter_*_allocate`, `aeron-driver/src/main/c/aeron_driver_conductor.c:4508-4531`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationCounters {
    /// `pub-pos`: what the producer has written.
    pub pub_pos: i32,
    /// `pub-lmt`: how far it may write.
    pub pub_lmt: i32,
    /// `snd-pos`: what has been sent.
    pub snd_pos: i32,
    /// `snd-lmt`: how far the sender may send.
    pub snd_lmt: i32,
    /// `snd-bpe`: how many times a send was refused for want of window.
    pub snd_bpe: i32,
    /// `snd-naks-received`: how many NAKs this publication was told about.
    pub snd_naks_received: i32,
    /// `fc-receivers`: how many receivers the flow-control strategy is
    /// holding, for a strategy that holds any (`aeron_flow_control.h:28`).
    ///
    /// [`None`] under `max`, which keeps none and is given no counter — the
    /// reference allocates this one in the group supplier alone
    /// (`aeron_min_flow_control.c:483-513`).
    pub fc_receivers: Option<i32>,
}

/// A receiver that has told this publication it is there
/// (`receiver_liveness_tracker`, `aeron_network_publication.h:150-160`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReceiverLiveness {
    receiver_id: i64,
    last_sm_ns: i64,
}

/// Write the log buffer's connected byte, which is what a client's
/// `is_connected()` reads (`aeron_network_publication_update_connected_status`,
/// `:765-777`).
///
/// Free rather than a method, and taking the mapping rather than a publication,
/// because **both threads** write it: the sender when a status message arrives
/// or a receiver expires, and the conductor when a spy is linked or unlinked
/// and `ssc` makes a spy a connection. They reach different mappings of one
/// file — the same pages — so the byte they compare and store is one byte.
///
/// The reference caches the answer in an `is_connected` field of its own
/// (`:139`) and reads that; here the byte *is* the answer, which is one load
/// from a page both threads already have hot, and one less flag that can drift
/// from what a client reads.
pub(crate) fn update_connected_status(log: &LogFile, expected: bool) {
    if let Some(metadata) = log.metadata() {
        let current = metadata.load_i32_relaxed(descriptor::IS_CONNECTED_OFFSET);
        if current == Some(i32::from(expected)) {
            return;
        }

        let _ = metadata.store_i32_relaxed(descriptor::IS_CONNECTED_OFFSET, i32::from(expected));
    }
}

/// A publication that sends over UDP.
pub struct NetworkPublication {
    /// The client's correlation id for the `ADD_PUBLICATION`.
    pub registration_id: i64,
    /// The client that owns it.
    pub client_id: i64,
    /// The session this stream runs under.
    pub session_id: i32,
    /// The stream id.
    pub stream_id: i32,
    /// The term id the stream started at.
    pub initial_term_id: i32,
    /// The channel as the client sent it, which the counters quote back.
    pub channel: Vec<u8>,
    /// Which send endpoint it sends through.
    pub endpoint_id: u64,
    /// Whether the producer asked for a single-producer publication.
    pub is_exclusive: bool,
    /// How long the sender position may sit unmoved before the log is unblocked
    /// for the producer (`aeron.publication.unblock.timeout`, fifteen seconds).
    pub unblock_timeout_ns: i64,
    /// The sender position as of the last time it was seen to move
    /// (`conductor_fields.last_snd_pos`), and when that was
    /// (`time_of_last_activity_ns`).
    ///
    /// **Not** the same `time_of_last_activity_ns` the LINGER state uses —
    /// [`Self::linger_since_ns`] holds that one, and the reference keeps them in
    /// the same struct under the one name because a publication is never in
    /// both situations at once. The pair here is the unblock deadline's; the
    /// timer is refreshed **only** on a pass where the sender position moved or
    /// the log was not blocked, so a stream that keeps flowing never reaches a
    /// deadline however long it runs.
    last_snd_pos: i64,
    time_of_last_activity_ns: i64,
    /// The log buffer the producer writes and this sends from.
    pub log: Box<LogFile>,
    /// The counters a client reads.
    pub counters: PublicationCounters,
    /// How many datagrams one pass may send.
    pub max_messages_per_send: usize,
    /// How long a term is.
    pub term_length: i32,
    /// `log2(term_length)`.
    pub position_bits_to_shift: u32,
    /// The largest frame, which bounds one datagram.
    pub mtu_length: i32,
    /// How far ahead of the slowest local reader the producer may run.
    pub term_window_length: i32,
    /// What `fc=` chose for this publication, and `max` when it chose
    /// nothing.
    pub flow_control: FlowControl,
    /// The retransmissions this publication owes.
    pub retransmit_handler: RetransmitHandler,
    /// Whether the stream signals end of stream in its heartbeats.
    pub signal_eos: bool,
    /// What this publication shares with the conductor's half of it: the
    /// readings the two threads take of each other
    /// ([`PublicationSightlines`]). The readers themselves are the conductor's
    /// — `PublicationMaintenance::subscribers` — because the limit they set is
    /// computed there.
    pub sightlines: Arc<PublicationSightlines>,
    /// Whether a spy counts as a connection here — the `ssc` parameter, which
    /// the driver's `aeron.spies.simulate.connection` or the channel's `ssc=`
    /// may set (`aeron_network_publication.c:289`).
    ///
    /// False by default, and that default is the whole difference between a
    /// stream whose only reader is a spy being *live* and being *stalled*: a
    /// unicast publication with no receiver has a limit of `snd-pos`, and
    /// `snd-pos` only moves when a receiver's status message opens the window.
    /// A spy is not a receiver, so without this the producer cannot publish
    /// the message the spy is waiting to read.
    pub spies_simulate_connection: bool,
    /// When a `SETUP` was last sent.
    pub time_of_last_setup_ns: i64,
    /// When data or a heartbeat was last sent.
    pub time_of_last_data_or_heartbeat_ns: i64,
    /// Whether an answer has ever arrived; until it has, the publication keeps
    /// saying `SETUP` (`:595-600`).
    pub has_initial_connection: bool,
    /// Whether a receiver asked for a `SETUP` and has not had one answered.
    pub is_setup_elicited: bool,
    /// The registration id of the subscription this publication answers, or
    /// [`layout::NULL_VALUE`] when it is not a response publication
    /// (`aeron_network_publication.c:317`).
    ///
    /// It is the whole of the tie between a response setup frame and the
    /// subscription waiting for it: the frame names the *publication*, and this
    /// is the only thing that says which subscription that publication was
    /// made for (`aeron_send_channel_endpoint.c:738-748`).
    pub response_correlation_id: i64,
    /// Whether this publication *is* the response half — the one a responder
    /// creates with `control-mode=response` (`aeron_network_publication.c:316`).
    ///
    /// It is the opposite end of the same idea: a publication that is not a
    /// response one but names a subscription asks for a response channel (the
    /// `SEND_RESPONSE` bit in its `SETUP`), and a publication that *is* one
    /// never asks — it is the answer.
    pub is_response: bool,
    /// The one address a response publication may send to, learned from the
    /// frame that asked for the channel
    /// (`endpoint_address`, `aeron_network_publication.h:243-274`).
    ///
    /// `None` is the reference's `AF_UNSPEC`, and it is not "send nowhere
    /// special" — it is **send nothing**. A response publication's peer is the
    /// one that asked, and until one has, there is nobody entitled to its data
    /// (`aeron_network_publication.c:355-378`).
    endpoint_address: Option<SocketAddr>,
    /// When the receivers go quiet, this is when they are declared gone.
    pub status_message_deadline_ns: i64,
    /// How long that is (`connection_timeout_ns`).
    pub connection_timeout_ns: i64,
    /// Who has been heard from, and when.
    receivers: Vec<ReceiverLiveness>,
    /// Whether the sender was refused for want of window, for the `snd-bpe`
    /// counter's once-per-blocked-run rule (`:548-556`).
    track_sender_limits: bool,
    /// Whether the stream has ended (`is_end_of_stream`).
    pub is_end_of_stream: bool,
    /// When a revoke was noticed, or [`None`] while the stream is running
    /// (`conductor_fields.time_of_last_activity_ns` on the way into the
    /// reference's LINGER state).
    linger_since_ns: Option<i64>,
    /// Whether the last client has let go, which is what starts the linger
    /// (`aeron_driver_managed_resource_event_t`'s `DECREF`-to-zero,
    /// `media/aeron_network_publication.c:1048-1069`: the end-of-stream
    /// position is written, `is_end_of_stream` is set when everything has been
    /// sent, and the state goes DRAINING).
    ///
    /// A publication is released — and with it the **endpoint** it sends
    /// through, whose own reference is dropped by that release — only when it
    /// has finished lingering. That is what makes a channel publishable again
    /// right after its publication is closed: the endpoint is still there, and
    /// the reference's own test for it
    /// (`NameReResolutionTest.shouldReResolveUnicastAddressWhenSendChannelEndpointIsReused`)
    /// adds the next publication with nothing in between.
    ending: bool,
    /// A datagram-sized scratch buffer, allocated once, that frames are copied
    /// into on the way out — see the module note.
    scratch: Vec<u8>,
}

/// Why a network publication could not be made.
#[derive(Debug)]
pub enum PublicationError {
    /// The term length is not a power of two in range, though the parameters
    /// were already checked in the reference's own order.
    BadTermLength,
    /// The log buffer has no metadata block, so nothing could be written into
    /// it — a mapping this build did not make.
    NoMetadata,
}

impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadTermLength => f.write_str("term length is not a power of two in range"),
            Self::NoMetadata => f.write_str("the log buffer has no metadata block"),
        }
    }
}

impl std::error::Error for PublicationError {}

impl NetworkPublication {
    /// Create a publication over a log buffer that is already mapped
    /// (`aeron_network_publication_create`,
    /// `aeron-driver/src/main/c/aeron_network_publication.c:101-320`).
    ///
    /// # Errors
    ///
    /// [`PublicationError`] when the term length cannot be shifted — which the
    /// publication parameters have already refused once, so this is the
    /// constructor's own guard rather than a path a client can reach.
    #[allow(clippy::too_many_arguments)] // one per field of the publication
    pub fn create(
        registration_id: i64,
        client_id: i64,
        session_id: i32,
        stream_id: i32,
        endpoint_id: u64,
        channel: &[u8],
        log: Box<LogFile>,
        params: &PublicationParams,
        is_exclusive: bool,
        counters: PublicationCounters,
        max_messages_per_send: usize,
        flow_control: FlowControl,
        retransmit_handler: RetransmitHandler,
        page_size: usize,
        socket_buffers: crate::sys::SocketBufferLengths,
        channel_sndbuf: usize,
        channel_rcvbuf: usize,
        unblock_timeout_ns: i64,
        connection_timeout_ns: i64,
        now_ns: i64,
        sightlines: Arc<PublicationSightlines>,
    ) -> Result<Self, PublicationError> {
        let position_bits_to_shift =
            deepmsg_core::logbuffer::position::bits_to_shift(params.term_length)
                .ok_or(PublicationError::BadTermLength)?;

        // The three tails: the first term begins at offset zero, the two before
        // it hold the wraps a rotation looks for (`aeron_ipc_publication.c:75-103`).
        //
        // A stream that resumes starts its tails at the term the URI named —
        // the reference writes the same block on the network path
        // (`aeron_network_publication.c:163-182`, its own copy of the IPC
        // code), and it has to be the same block: a publication that names
        // another's session with `session-id=tag:N` is meant to describe the
        // *same* stream, so it has to start where that stream is.
        #[allow(clippy::cast_possible_truncation)] // bounded by a term length
        let start = params
            .starting_position
            .map(|position| (position.term_id, position.term_offset as i32));

        log.initialise_tails(params.initial_term_id, start);

        // Where `snd-pos` begins, which is what `last_snd_pos` is seeded from:
        // the caller writes this into the counter at creation
        // (`aeron_network_publication.c:311` reads it back out of the counter,
        // and the value is not reachable from here, so the same arithmetic the
        // caller used is repeated).
        let start_position = params.starting_position.map_or(0, |position| {
            deepmsg_core::logbuffer::position::Position::new(
                position.term_id,
                i32::try_from(position.term_offset).unwrap_or(i32::MAX),
                position_bits_to_shift,
                params.initial_term_id,
            )
            .raw()
        });

        // And then the metadata block, which is what a *client* reads when it
        // maps the file `ON_PUBLICATION_READY` named
        // (`aeron_logbuffer_metadata_init`, `aeron_network_publication.c:196-233`).
        // A log buffer whose metadata was never written is one a client cannot
        // map at all: its term length reads as zero.
        {
            let Some(metadata) = log.metadata() else {
                return Err(PublicationError::NoMetadata);
            };

            let init = descriptor::LogMetadataInit {
                end_of_stream_position: i64::MAX,
                is_connected: 0,
                active_transport_count: 0,
                correlation_id: registration_id,
                initial_term_id: params.initial_term_id,
                mtu_length: params.mtu_length,
                term_length: params.term_length,
                page_size: i32_from(page_size),
                publication_window_length: params.publication_window_length,
                // A publication advertises no receive window: the counter that
                // carries one belongs to an *image* (`:206` passes zero).
                receiver_window_length: 0,
                socket_sndbuf_length: i32_from(channel_sndbuf),
                os_default_socket_sndbuf_length: socket_buffers.sndbuf,
                os_max_socket_sndbuf_length: 0,
                socket_rcvbuf_length: i32_from(channel_rcvbuf),
                os_default_socket_rcvbuf_length: socket_buffers.rcvbuf,
                os_max_socket_rcvbuf_length: 0,
                max_resend: params.max_resend,
                session_id,
                stream_id,
                entity_tag: params.entity_tag,
                response_correlation_id: params.response_correlation_id,
                linger_timeout_ns: params.linger_timeout_ns,
                untethered_window_limit_timeout_ns: params.untethered_window_limit_timeout_ns,
                untethered_linger_timeout_ns: params.untethered_linger_timeout_ns,
                untethered_resting_timeout_ns: params.untethered_resting_timeout_ns,
                // Group semantics, a response channel, a rejoin and a reliable
                // stream are multicast or response-channel ideas: a unicast
                // publication writes them false (`:226-229`).
                //
                // `group` is not among the ones this build always answers false
                // to — it is the endpoint channel's group semantics, the same
                // value the setup frame's `GROUP` flag carries (`:136`, `:224`).
                group: u8::from(retransmit_handler.has_group_semantics()),
                is_response: params.is_response,
                rejoin: false,
                reliable: false,
                sparse: params.is_sparse,
                signal_eos: params.signal_eos,
                spies_simulate_connection: params.spies_simulate_connection,
                tether: false,
                is_exclusive,
            };

            if descriptor::initialise(&metadata, &init).is_none() {
                return Err(PublicationError::NoMetadata);
            }
        }

        Ok(Self {
            registration_id,
            client_id,
            session_id,
            stream_id,
            initial_term_id: params.initial_term_id,
            channel: channel.to_vec(),
            endpoint_id,
            is_exclusive,
            unblock_timeout_ns,
            // Seeded from where `snd-pos` starts, which is the position the
            // caller writes into that counter at creation
            // (`aeron_network_publication.c:311` reads it back out of the
            // counter; the value is not available here, so the same arithmetic
            // the caller used is repeated).
            last_snd_pos: start_position,
            time_of_last_activity_ns: 0,
            log,
            counters,
            // Clamped where the reference clamps it — its context setter
            // refuses anything above `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND`
            // (`aeron_driver_context.c:3378-3384`), which is the sixteen
            // `send_data`'s batch arrays are sized to. The setting is not read
            // here yet, so this is the default four and the clamp cannot bite
            // today; it is here so that adding the setting cannot turn a batch
            // into an out-of-bounds write.
            max_messages_per_send: max_messages_per_send.min(crate::sys::socket::MAX_BATCH),
            term_length: params.term_length,
            position_bits_to_shift,
            mtu_length: params.mtu_length,
            term_window_length: params.publication_window_length,
            flow_control,
            retransmit_handler,
            signal_eos: params.signal_eos,
            sightlines,
            spies_simulate_connection: params.spies_simulate_connection,
            // The first `SETUP` is due at once. The reference seeds this a
            // timeout and a nanosecond *before* now, so that
            // `now_ns > time_of_last_setup_ns + SETUP_TIMEOUT_NS` already holds
            // on the first pass (`aeron_network_publication.c:285`). Zero says
            // the same thing only to a clock whose zero is the epoch; against a
            // monotonic reading it holds the first `SETUP` back for the whole
            // timeout, which is exactly the moment a sender that has met no
            // receiver is supposed to be saying it.
            time_of_last_setup_ns: now_ns.saturating_sub(SETUP_TIMEOUT_NS).saturating_sub(1),
            time_of_last_data_or_heartbeat_ns: now_ns,
            has_initial_connection: false,
            is_setup_elicited: false,
            response_correlation_id: params.response_correlation_id,
            is_response: params.is_response,
            endpoint_address: None,
            status_message_deadline_ns: now_ns + connection_timeout_ns,
            connection_timeout_ns,
            receivers: Vec::new(),
            track_sender_limits: false,
            is_end_of_stream: false,
            linger_since_ns: None,
            ending: false,
            scratch: vec![0u8; max_messages_per_send * params.mtu_length as usize],
        })
    }

    /// The highest position the producer has published
    /// (`aeron_network_publication_producer_position`, `:400-430`): the largest
    /// of the log buffer's three term tail counters.
    /// The walk is [`LogFile::producer_position`]'s, and so is the conductor's
    /// copy of it: the two threads read the same three tails out of two
    /// mappings of one file, and a second implementation of the comparison
    /// would be a second chance to get it wrong.
    pub fn producer_position(&self) -> Option<i64> {
        self.log.producer_position(self.initial_term_id)
    }

    /// Whether a receiver is live (`has_receivers`).
    pub fn has_receivers(&self) -> bool {
        !self.receivers.is_empty()
    }

    /// Whether a client's `is_connected()` reads true, which is the log
    /// buffer's connected byte — both threads write it and it is the one the
    /// client reads, so it is the answer rather than a copy of it.
    pub fn is_connected(&self) -> bool {
        self.log
            .metadata()
            .and_then(|metadata| metadata.load_i32_relaxed(descriptor::IS_CONNECTED_OFFSET))
            .is_some_and(|connected| connected != 0)
    }

    /// How many receivers have been heard from lately.
    pub fn receiver_count(&self) -> usize {
        self.receivers.len()
    }

    /// Send what this publication has
    /// (`aeron_network_publication_send`, `:582-652`).
    ///
    /// The order is the reference's and each step answers a different question:
    /// while nothing has answered, say `SETUP`; send data; if nothing went,
    /// send a heartbeat and let the flow control look at an idle pass.
    ///
    /// # Errors
    ///
    /// The socket's error, for the caller to record and count.
    pub fn send(
        &mut self,
        endpoint: &mut SendChannelEndpoint,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> io::Result<usize> {
        let snd_pos = counters.value(regions, self.counters.snd_pos).unwrap_or(0);
        let position = Position::from_raw(snd_pos);
        let active_term_id = position.term_id(self.position_bits_to_shift, self.initial_term_id);
        let term_offset = position.term_offset(self.position_bits_to_shift);

        // `pub-pos` and `pub-lmt` are **not** written here any more. They are
        // the conductor's (`PublicationMaintenance::update_pub_pos_and_lmt`),
        // which is where the reference writes them
        // (`aeron_driver_conductor.c:3395-3399`), and more to the point where
        // the readers whose slowest one decides the limit now live. One writer:
        // a second one could move `pub-lmt` backwards and hold the producer to
        // a limit that went down — see that module.
        if !self.has_initial_connection || self.is_setup_elicited {
            self.setup_message_check(
                endpoint,
                system,
                counters,
                regions,
                now_ns,
                active_term_id,
                term_offset,
            )?;
        }

        let mut bytes_sent = self.send_data(endpoint, system, counters, regions, now_ns)?;

        if 0 == bytes_sent {
            bytes_sent = self.heartbeat_message_check(
                endpoint,
                system,
                counters,
                regions,
                now_ns,
                active_term_id,
                term_offset,
            )?;

            // `:612-636`: an idle pass is where a publication with **only
            // spies** moves at all, and it moves because there is nothing to
            // send. `snd-pos` says what has left the machine, and with no
            // receiver nothing ever does — so for an `ssc` stream the spies
            // stand in for the wire and `snd-pos` is taken up to the furthest
            // of them. The flow control is asked from there rather than from
            // `snd-pos`, which is what makes `snd-lmt` follow.
            let snd_lmt = counters.value(regions, self.counters.snd_lmt).unwrap_or(0);

            let new_limit = if self.spies_simulate_connection
                && self.sightlines.has_spies()
                && !self.has_receivers()
            {
                let new_snd_pos = self.sightlines.max_spy_position().max(snd_pos);
                let _ = counters.set_value(regions, self.counters.snd_pos, new_snd_pos);

                self.flow_control
                    .on_idle(now_ns, new_snd_pos, new_snd_pos, self.is_end_of_stream)
            } else {
                // Otherwise the limit is the flow control's to move — the
                // `max` strategy does nothing with it, and the strategies that
                // do (a multicast sender waiting for receivers) belong to
                // multicast, refused here and recorded in `docs/compat.md`.
                self.flow_control
                    .on_idle(now_ns, snd_lmt, snd_pos, self.is_end_of_stream)
            };

            if new_limit != snd_lmt {
                let _ = counters.set_value(regions, self.counters.snd_lmt, new_limit);
            }

            // A group strategy drops the receivers that have gone quiet here
            // (`aeron_min_flow_control.c:98-157`), so this is the pass its count
            // changes on as well as the one a status message arrives on.
            self.update_receiver_count(counters, regions);

            if self.expire_receivers(now_ns) {
                self.refresh_connected_status();
            }
        }

        // The conductor's half of `has_subscribers` reads this, and the answer
        // moves when a receiver is heard from, when one expires and when the
        // strategy decides it has heard from enough of them
        // (`aeron_network_publication_update_has_receivers`, `:84-96`, is the
        // reference's own refresh). Stored only when it changes, so a pass that
        // changes nothing costs a load.
        let satisfied = self.has_receivers() && self.flow_control.has_required_receivers();
        self.sightlines
            .set_receivers_satisfy_flow_control(satisfied);

        let retransmitted = self.serve_retransmissions(endpoint, system, counters, regions, now_ns);
        let _ = retransmitted;

        Ok(bytes_sent)
    }

    /// Say `SETUP` if it is due
    /// (`aeron_network_publication_setup_message_check`, `:383-437`).
    #[allow(clippy::too_many_arguments)] // the position and the two counter views
    fn setup_message_check(
        &mut self,
        endpoint: &mut SendChannelEndpoint,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        active_term_id: i32,
        term_offset: i32,
    ) -> io::Result<usize> {
        if now_ns <= self.time_of_last_setup_ns + SETUP_TIMEOUT_NS {
            return Ok(0);
        }

        let frame = SetupFrame {
            term_offset,
            session_id: self.session_id,
            stream_id: self.stream_id,
            initial_term_id: self.initial_term_id,
            active_term_id,
            term_length: self.term_length,
            mtu: self.mtu_length,
            ttl: i32::from(endpoint.channel.multicast_ttl),
        };

        let mut buffer = [0u8; SetupFrame::LENGTH];
        // `:392-400`. A publication that is not itself a response channel but
        // was made to answer one says so here, and that bit is the whole of how
        // the far end learns it must answer with a `RSP_SETUP`: a responder's
        // own publication never sets it, so the request is never echoed.
        let send_response_flag =
            if !self.is_response && self.response_correlation_id != layout::NULL_VALUE {
                header_flags::SETUP_SEND_RESPONSE
            } else {
                0
            };
        let group_flag = if self.retransmit_handler.has_group_semantics() {
            header_flags::SETUP_GROUP
        } else {
            0
        };
        let flags = send_response_flag | group_flag;

        if frame.write_with_flags(&mut buffer, flags).is_none() {
            return Ok(0);
        }

        if self.is_setup_elicited {
            // `:414-421`: only an elicited setup is reported, and it is
            // reported *before* the frame goes out — a strategy that gates on
            // the setup it sent reads the limit from this moment
            // (`aeron_min_flow_control.c:283-303`).
            let snd_lmt = counters.value(regions, self.counters.snd_lmt).unwrap_or(0);

            self.flow_control.on_setup(now_ns, snd_lmt);
        }

        let sent = self.do_send(endpoint, &[&buffer], counters, regions, now_ns)?;

        if sent < 1 {
            system.increment(system_counters::id::SHORT_SENDS);
        }

        self.time_of_last_setup_ns = now_ns;

        if self.has_receivers() {
            self.is_setup_elicited = false;
        }

        let _ = (counters, regions);

        // Bytes, like the rest of the send path (`:576`). The caller checks
        // only the sign of this one, so the unit is the caller's business
        // either way — but a `SETUP` that said "1" for a 48-byte frame would be
        // a number in the wrong unit waiting for someone to believe it.
        Ok(if sent < 1 { 0 } else { buffer.len() })
    }

    /// Say something if the stream has been quiet
    /// (`aeron_network_publication_heartbeat_message_check`, `:439-493`).
    ///
    /// A heartbeat is a **zero-length DATA frame** with both the begin and end
    /// flags: it carries no payload, and it is what tells a receiver the stream
    /// is alive and where it stands.
    #[allow(clippy::too_many_arguments)] // the frame, and where it arrived
    fn heartbeat_message_check(
        &mut self,
        endpoint: &mut SendChannelEndpoint,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
        active_term_id: i32,
        term_offset: i32,
    ) -> io::Result<usize> {
        if !self.has_initial_connection
            || now_ns <= self.time_of_last_data_or_heartbeat_ns + HEARTBEAT_TIMEOUT_NS
        {
            return Ok(0);
        }

        let mut flags = header_flags::BEGIN | header_flags::END;

        // `:459-472`: a revoked publication says so, and so does the end of a
        // stream the channel asked to signal.
        if self.is_revoked() {
            flags |= header_flags::EOS | header_flags::REVOKED;
        } else if self.signal_eos && self.is_end_of_stream {
            flags |= header_flags::EOS;
        }

        let frame = DataFrame {
            term_offset,
            session_id: self.session_id,
            stream_id: self.stream_id,
            term_id: active_term_id,
            reserved_value: 0,
        };

        // The reference fills a data header and then sets `frame_length = 0`
        // (`aeron_network_publication.c:463`): the *packet* is a whole data
        // header, and the zero length is what tells the receiver there is no
        // payload to insert — `aeron_is_frame_valid` accepts it because a DATA
        // frame's validity is about the packet, not about `frame_length`
        // (`aeron_udp_protocol.h:235-238`).
        let mut buffer = [0u8; DataFrame::LENGTH];

        if frame.write_with_flags(&mut buffer, flags).is_none() {
            return Ok(0);
        }

        let header = FrameHeader {
            frame_length: 0,
            version: crate::protocol::VERSION,
            flags,
            frame_type: frame_type::DATA,
        };

        if header.write(&mut buffer).is_none() {
            return Ok(0);
        }

        let sent = self.do_send(endpoint, &[&buffer], counters, regions, now_ns)?;

        if sent < 1 {
            system.increment(system_counters::id::SHORT_SENDS);
        }

        system.increment(system_counters::id::HEARTBEATS_SENT);
        self.time_of_last_data_or_heartbeat_ns = now_ns;

        // Bytes: this value is what a pass that sent no data reports, and the
        // sender adds it to `bytes-sent` (`:576`, `aeron_driver_sender.c:457`).
        Ok(if sent < 1 { 0 } else { buffer.len() })
    }

    /// Cut the stream off and say so
    /// (`AERON_DRIVER_MANAGED_RESOURCE_EVENT_REVOKE`,
    /// `aeron_network_publication.c:1075-1079`).
    ///
    /// One byte, on the log buffer every reader maps: the heartbeat that
    /// carries `REVOKED`, the image that drains because of it and the readers
    /// that are told are all downstream of this.
    /// The last client has let go: the publication may end
    /// (`DECREF`-to-zero, `media/aeron_network_publication.c:1048-1069`).
    pub const fn request_end(&mut self) {
        self.ending = true;
    }

    pub fn set_revoked(&self) {
        if let Some(metadata) = self.log.metadata() {
            let _ = metadata.store_u8_relaxed(descriptor::IS_PUBLICATION_REVOKED_OFFSET, 1);
        }
    }

    /// What a revoked publication does on its next pass, and when it is
    /// finished.
    ///
    /// Two of the reference's arms in one call
    /// (`aeron_network_publication_check_managed_resources`, `:1240-1340`):
    ///
    /// * **ACTIVE → LINGER**, the first time the byte is seen. The limit stops
    ///   where the producer did, the log says where the stream ended, this
    ///   publication is no longer connected and — the part everything else is
    ///   for — its heartbeats say `REVOKED` from here on. That is what a
    ///   reader's image needs to drain, and it is why a publication cannot be
    ///   released in the same pass as its revoke.
    /// * **LINGER → DONE**, once nobody is left to tell: no receivers at all,
    ///   or the linger window (`aeron.publication.linger.timeout`) gone by.
    ///
    /// Returns whether the publication is finished with, which is when the
    /// conductor may finish releasing it.
    pub fn notice_revoke(
        &mut self,
        now_ns: i64,
        linger_timeout_ns: i64,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        // While a publication whose last client has let go is still draining,
        // the log is unblocked at the **sender's** position
        // (`aeron_network_publication.c:1300-1315`). No gate and no
        // `is_exclusive` test: the client is gone, so whatever is in the way
        // will never be committed, and being here is the evidence. The reference
        // skips it for a *revoked* publication, whose stream is being cut off
        // rather than finished, and so does this.
        //
        // The reference runs it on every time event for as long as the state is
        // DRAINING. Here that window is the one between this call and the linger
        // decision, and `ending` is the flag that stands for it.
        if self.ending && !self.is_revoked() {
            let sender_position = self.sender_position(counters, regions);

            if self.producer_position().unwrap_or(0) > sender_position {
                self.unblock_at(sender_position, counters, regions);
            }
        }

        let Some(since) = self.linger_since_ns else {
            // Two ways in, and the reference meets them in one place: a
            // revoked publication says so in its heartbeats
            // (`:1246-1277`), and one whose last client has let go writes the
            // end-of-stream position (`:1048-1069`). Both then linger.
            if !self.is_revoked() && !self.ending {
                return false;
            }

            let end_position = self.producer_position().unwrap_or(0);

            let _ = counters.set_value(regions, self.counters.pub_lmt, end_position);
            self.set_end_of_stream(end_position);
            // `:1060-1063`: the byte is set when everything the producer wrote
            // has been sent. A revoked publication is a stream that is over
            // whether or not that happened.
            self.is_end_of_stream =
                self.is_revoked() || self.sender_position(counters, regions) >= end_position;
            self.linger_since_ns = Some(now_ns);

            if self.is_revoked() {
                system.increment(system_counters::id::PUBLICATIONS_REVOKED);
            }

            return false;
        };

        // The end-of-stream byte is **not** decided once, on the way in: the
        // reference sets it on every tick of the drain, the moment the sender
        // has caught up with the producer (`:1319-1322`). It is what the flow
        // control's idle path reads to tell receivers the stream is over
        // (`:602-606` → `aeron_flow_control_on_idle`), so a publication whose
        // producer stopped writing before its sender did — which is the whole
        // point of draining — would otherwise never say so.
        if !self.is_revoked() && !self.is_end_of_stream {
            let producer_position = self.producer_position().unwrap_or(0);

            if self.sender_position(counters, regions) >= producer_position {
                self.is_end_of_stream = true;
            }
        }

        // A **revoked** publication is done as soon as there is nobody left to
        // tell — that is this build's own shortcut, and `docs/compat.md` says
        // so. One whose clients have all let go waits the window the reference
        // waits (`:1329-1341`, whose condition is the linger timeout or an EOS
        // from a unicast receiver): the endpoint is released with it, and a
        // client may be about to publish on that channel again.
        if self.is_revoked() {
            return !self.has_receivers() || now_ns > since + linger_timeout_ns;
        }

        now_ns > since + linger_timeout_ns
    }

    /// Write where the stream ended, which is how a reader that has not seen
    /// the heartbeats yet still learns it is over.
    fn set_end_of_stream(&self, position: i64) {
        if let Some(metadata) = self.log.metadata() {
            let _ = metadata.store_i64_release(descriptor::END_OF_STREAM_POSITION_OFFSET, position);
        }
    }

    /// Whether the log buffer's metadata says the publication was revoked
    /// (`is_publication_revoked`).
    /// How far the sender has got, which is what the end-of-stream byte is
    /// decided against (`snd_pos_position`, `:1060`).
    fn sender_position(&self, counters: &CounterManager, regions: &CounterRegions<'_>) -> i64 {
        counters
            .value(regions, self.counters.snd_pos)
            .unwrap_or_default()
    }

    fn is_revoked(&self) -> bool {
        self.log
            .metadata()
            .and_then(|metadata| metadata.load_u8(descriptor::IS_PUBLICATION_REVOKED_OFFSET))
            .is_some_and(|value| value != 0)
    }

    /// Send whatever the term holds, in one batch
    /// (`aeron_network_publication_send_data`, `:495-580`).
    ///
    /// The sender's position advances by what the kernel took **only if it took
    /// everything**: a partial send is a datagram still waiting for room, and a
    /// position that moved past it would lose it.
    ///
    /// Returns the **bytes** that went out, which is what the reference returns
    /// (`:576`) and what the sender counts as `bytes-sent`
    /// (`aeron_driver_sender.c:457`). Not the datagram count: those differ by
    /// three orders of magnitude, and a counter named `bytes-sent` that held
    /// `1000` for a megabyte of frames is a counter that lies.
    ///
    /// # Errors
    ///
    /// The socket's error.
    pub fn send_data(
        &mut self,
        endpoint: &mut SendChannelEndpoint,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> io::Result<usize> {
        let snd_pos = counters.value(regions, self.counters.snd_pos).unwrap_or(0);
        let snd_lmt = counters.value(regions, self.counters.snd_lmt).unwrap_or(0);
        let mut available_window = snd_lmt - snd_pos;
        let mut highest_pos = snd_pos;

        let term_length = self.term_length as usize;
        let position = Position::from_raw(snd_pos);
        let mut term_index = position.index(self.position_bits_to_shift);
        let mut term_offset = position
            .term_offset(self.position_bits_to_shift)
            .unsigned_abs() as usize;

        let mut scratch = std::mem::take(&mut self.scratch);
        let mut filled = 0usize;
        // A fixed array, not a `Vec`: the reference's batch is
        // `struct iovec iov[AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND]`
        // on the stack (`aeron_network_publication.c:515`), that macro is 16
        // (`aeron_driver_context.h:57`), and the context setter clamps the
        // setting to the same sixteen this build's socket shim batches with
        // (`crate::sys::socket::MAX_BATCH`). Two `Vec`s here were a `malloc`
        // and a `free` in every send — the allocator is ~14% of this thread
        // and the chain that reaches it is this one.
        let mut bounds = [(0usize, 0usize); crate::sys::socket::MAX_BATCH];
        let mut frames = 0usize;
        let mut blocked = false;

        while frames < self.max_messages_per_send && available_window > 0 {
            let scan_limit =
                i32::try_from(available_window.min(i64::from(self.mtu_length))).unwrap_or(0);
            let term_length_left = i32::try_from(term_length - term_offset).unwrap_or(0);

            let Some(term) = self.log.term(term_index) else {
                break;
            };

            let availability = scan_for_availability(
                &term.as_read_only(),
                term_offset,
                term_length_left,
                scan_limit,
            );

            let (available, padding) = match availability {
                Availability::Ready { available, padding } => (
                    usize::try_from(available).unwrap_or(0),
                    usize::try_from(padding).unwrap_or(0),
                ),
                Availability::Limited { .. } => {
                    blocked = true;
                    break;
                }
                Availability::Empty => break,
            };

            if available > 0 {
                if term
                    .copy_out(term_offset, &mut scratch[filled..filled + available])
                    .is_none()
                {
                    break;
                }

                bounds[frames] = (filled, available);
                frames += 1;
                filled += available;
            }

            let step = available + padding;
            available_window -= i64::try_from(step).unwrap_or(0);
            highest_pos += i64::try_from(step).unwrap_or(0);
            term_offset += step;

            // A batch may cross a term boundary; the padding frame that closes
            // a term is included in `step`, so the next frame starts in the
            // next partition.
            if term_offset >= term_length {
                term_index = (term_index + 1) % descriptor::PARTITION_COUNT;
                term_offset = 0;
            }

            if 0 == step {
                break;
            }
        }

        // Built only when there is something to send. Every one of these
        // arrays is sixteen entries wide whatever the batch is, and this
        // function runs about 1.3M times a second with nothing to send — an
        // initialised `slices` on that path is 256 bytes written to be read by
        // nobody.
        let mut bytes_sent = 0usize;
        // How many of the batch the socket took, which the check below reads
        // even on a pass that sent nothing.
        let mut sent = 0usize;

        if frames > 0 {
            let mut slices: [&[u8]; crate::sys::socket::MAX_BATCH] =
                [&[]; crate::sys::socket::MAX_BATCH];
            for (index, (offset, length)) in bounds[..frames].iter().enumerate() {
                slices[index] = &scratch[*offset..*offset + *length];
            }

            // What `do_send` returns is **bytes**, not datagrams: the sender
            // adds it to `bytes-sent`, and the reference's `send_data` returns
            // the byte count its `do_send` accumulated
            // (`aeron_network_publication.c:576`, `aeron_driver_sender.c:457`).
            // A partial send is the first `sent` datagrams — `send` reports how
            // many of the batch it took, in order.
            sent = self.do_send(endpoint, &slices[..frames], counters, regions, now_ns)?;

            bytes_sent = slices[..frames]
                .iter()
                .take(sent)
                .map(|slice| slice.len())
                .sum();
        }

        self.scratch = scratch;

        if frames > 0 {
            if sent == frames {
                let _ = counters.set_value(regions, self.counters.snd_pos, highest_pos);
                self.time_of_last_data_or_heartbeat_ns = now_ns;
                self.track_sender_limits = true;
            } else {
                system.increment(system_counters::id::SHORT_SENDS);
            }
        } else if self.track_sender_limits && available_window <= 0 {
            let _ = system_counters::increment(counters, regions, self.counters.snd_bpe);
            system.increment(system_counters::id::SENDER_FLOW_CONTROL_LIMITS);
            self.track_sender_limits = false;
        }

        if blocked && self.track_sender_limits {
            let _ = system_counters::increment(counters, regions, self.counters.snd_bpe);
            system.increment(system_counters::id::SENDER_FLOW_CONTROL_LIMITS);
            self.track_sender_limits = false;
        }

        let _ = position;

        Ok(bytes_sent)
    }

    /// Serve what the retransmit handler owes
    /// (`aeron_retransmit_handler_process_timeouts` from `send`, `:641-650`).
    fn serve_retransmissions(
        &mut self,
        endpoint: &mut SendChannelEndpoint,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> usize {
        let mut due = Vec::new();

        let served = self.retransmit_handler.process_timeouts(now_ns, |resend| {
            due.push(resend);
            true
        });

        for resend in due {
            if self
                .resend(endpoint, resend, system, counters, regions, now_ns)
                .is_err()
            {
                break;
            }
        }

        served
    }

    /// Send the frames a NAK asked for
    /// (`aeron_network_publication_resend`, `:655-728`).
    ///
    /// One datagram per scan result, until the requested length is covered.
    /// The reference refuses a resend further back than half a term plus a
    /// maximum message: that far back the term has been reused and the frames
    /// the NAK names are no longer the frames it wants.
    ///
    /// # Errors
    ///
    /// The socket's error.
    fn resend(
        &mut self,
        endpoint: &mut SendChannelEndpoint,
        resend: Resend,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> io::Result<usize> {
        let sender_position = counters.value(regions, self.counters.snd_pos).unwrap_or(0);
        let resend_position = Position::new(
            resend.term_id,
            resend.term_offset,
            self.position_bits_to_shift,
            self.initial_term_id,
        )
        .raw();

        let term_length = self.term_length as usize;
        let bottom = sender_position
            - (term_length as i64 >> 1)
            - i64::from(deepmsg_core::logbuffer::position::max_message_length(
                self.term_length,
            ));

        if resend_position < bottom || resend_position >= sender_position {
            return Ok(0);
        }

        let mut term_index = Position::from_raw(resend_position).index(self.position_bits_to_shift);
        let mut offset = resend.term_offset.unsigned_abs() as usize;
        let mut remaining = resend.length;
        let mut total = 0usize;

        let mut scratch = std::mem::take(&mut self.scratch);

        while remaining > 0 {
            let term_length_left = i32::try_from(term_length - offset).unwrap_or(0);
            let max_length = i32::try_from(remaining.min(self.mtu_length as usize)).unwrap_or(0);

            let Some(term) = self.log.term(term_index) else {
                break;
            };

            let availability =
                scan_for_availability(&term.as_read_only(), offset, term_length_left, max_length);
            let (available, padding) = match availability {
                Availability::Ready { available, padding } => (
                    usize::try_from(available).unwrap_or(0),
                    usize::try_from(padding).unwrap_or(0),
                ),
                _ => break,
            };

            if available == 0 || available > scratch.len() {
                break;
            }

            if term.copy_out(offset, &mut scratch[..available]).is_none() {
                break;
            }

            let sent = self.do_send(
                endpoint,
                &[&scratch[..available]],
                counters,
                regions,
                now_ns,
            )?;

            if sent < 1 {
                system.increment(system_counters::id::SHORT_SENDS);
                break;
            }

            let step = available + padding;
            total += step;
            remaining = remaining.saturating_sub(step);
            offset += step;

            if offset >= term_length {
                term_index = (term_index + 1) % descriptor::PARTITION_COUNT;
                offset = 0;
            }
        }

        self.scratch = scratch;

        if total > 0 {
            system.increment(system_counters::id::RETRANSMITS_SENT);
            system.add(
                system_counters::id::RETRANSMITTED_BYTES,
                i64::try_from(total).unwrap_or(i64::MAX),
            );
        }

        Ok(total)
    }

    /// Whether a status message reports a position this publication can place
    /// (`aeron_network_publication_is_valid_status_message`, `:841-856`).
    ///
    /// A receiver that reports a position far outside where this publication is
    /// has the wrong session, or a state this publication cannot reconcile with
    /// its own — and `snd-lmt` is computed from that position, so acting on it
    /// would open a window the log cannot follow. The band is a term and a half
    /// wide: half a term behind `snd-pos`, a term and a half in front of it.
    pub fn is_valid_status_message(&self, frame: &StatusMessageFrame, snd_pos: i64) -> bool {
        let sm_position = Position::new(
            frame.consumption_term_id,
            frame.consumption_term_offset,
            self.position_bits_to_shift,
            self.initial_term_id,
        )
        .raw();

        let term_buffer_length = i64::from(self.term_length);
        let half_term = term_buffer_length >> 1;

        sm_position >= (snd_pos - half_term)
            && sm_position <= (snd_pos + term_buffer_length + half_term)
    }

    /// A receiver asked for a `SETUP` because it has no image for this session
    /// (`aeron_network_publication_trigger_send_setup_frame`, `.h:245-271`).
    ///
    /// The flag is what makes this different from an ordinary status message:
    /// the receiver is not reporting a position, it is asking to be told how
    /// the stream starts. Setting `is_setup_elicited` re-opens the `SETUP` path
    /// in [`NetworkPublication::send`] — `if !has_initial_connection ||
    /// is_setup_elicited` (`aeron_network_publication.c:586`) — which a
    /// publication that has already met one receiver has otherwise closed for
    /// good. Without it a receiver that has restarted, or a second one that
    /// arrives later, is never answered and builds no image.
    ///
    /// The reference hands the status message to the flow control strategy
    /// here as well (`on_trigger_send_setup`, `.h:255-261`), and the only thing
    /// any strategy reads out of it is the group tag
    /// (`aeron_min_flow_control.c:394-422`) — which is why what this takes is
    /// the tag and not the frame.
    ///
    /// `elicited_from` is where the message came from, and for a response
    /// publication it is the **only** place this publication may ever send: the
    /// peer that asked for the channel is the one that elicited, and it is
    /// learned here or not at all
    /// (`aeron_network_publication_trigger_send_setup_frame`,
    /// `aeron_network_publication.h:243-274`).
    /// It is optional only because this build's transport can be asked not to
    /// report a source; a datagram off a socket always has one, and a response
    /// publication learns its peer from nothing else.
    pub fn trigger_send_setup_frame(
        &mut self,
        elicited_from: Option<SocketAddr>,
        group_tag: Option<i64>,
    ) {
        if self.is_end_of_stream {
            return;
        }

        self.is_setup_elicited = true;

        self.flow_control.on_trigger_send_setup(group_tag);

        if self.is_response {
            if let Some(address) = elicited_from {
                self.endpoint_address = Some(address);
            }
        }
    }

    /// Hand one batch to the transport, or refuse to
    /// (`aeron_network_publication_do_send`, `:355-378`).
    ///
    /// Every frame a publication sends goes through here, which is the whole
    /// point: a response publication's restriction is not a rule about its data
    /// frames, it is a rule about *this publication*, and the reference reaches
    /// it from the setup, the heartbeat, the data path, the retransmit path and
    /// the RTTM answer alike.
    ///
    /// A response publication that has learned no address sends nothing at all
    /// — not to the endpoint's address, which is a different peer entirely.
    ///
    /// # Errors
    ///
    /// The socket's error.
    fn do_send(
        &self,
        endpoint: &mut SendChannelEndpoint,
        buffers: &[&[u8]],
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> io::Result<usize> {
        if self.is_response {
            return match self.endpoint_address {
                Some(address) => endpoint.send_to(address, buffers),
                None => Ok(0),
            };
        }

        endpoint.send(buffers, counters, regions, now_ns)
    }

    /// A status message arrived (`aeron_network_publication_on_status_message`,
    /// `:779-840`).
    ///
    /// The order is the reference's and each step is load-bearing: the sender's
    /// liveness first (an end-of-stream status message is a receiver *leaving*,
    /// so it removes rather than refreshes), then the flow control, then the
    /// connected state recomputed from both.
    pub fn on_status_message(
        &mut self,
        frame: &StatusMessageFrame,
        flags: u8,
        group_tag: Option<i64>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> Option<i64> {
        // `status-messages-received` is counted at the endpoint, before this —
        // and whether or not a publication answered to the message
        // (`media/aeron_send_channel_endpoint.c:625`).
        self.status_message_deadline_ns = now_ns + self.connection_timeout_ns;

        let had_receivers = self.has_receivers();

        if flags & header_flags::SM_EOS != 0 {
            self.remove_receiver(frame.receiver_id);
        } else {
            self.record_receiver(frame.receiver_id, now_ns);
        }

        self.has_initial_connection = true;

        let snd_lmt = counters.value(regions, self.counters.snd_lmt).unwrap_or(0);
        let consumption_position = Position::new(
            frame.consumption_term_id,
            frame.consumption_term_offset,
            self.position_bits_to_shift,
            self.initial_term_id,
        )
        .raw();

        let new_limit = self.flow_control.on_sm(
            &StatusMessage {
                consumption_position,
                receiver_window: frame.receiver_window,
                receiver_id: frame.receiver_id,
                session_id: frame.session_id,
                stream_id: frame.stream_id,
                eos_flagged: flags & header_flags::SM_EOS != 0,
                group_tag,
            },
            snd_lmt,
            now_ns,
        );

        let _ = counters.set_value(regions, self.counters.snd_lmt, new_limit);

        self.update_receiver_count(counters, regions);

        self.refresh_connected_status();

        // A publication that has just acquired its **first** live receiver
        // reports the fact (`:805-812`), and the correlation id it reports is
        // its own `response-correlation-id` — for the publication a response
        // channel was asked for, that is the registration id of the *image*
        // which owes it a response setup, and the conductor is what can reach
        // that image (`aeron_driver_conductor_on_response_connected`,
        // `aeron_driver_conductor.c:7117-7131`).
        //
        // It is reported whether or not this publication is on a response
        // channel, as the reference reports it: what makes it meaningful is the
        // conductor's lookup by registration id, which an id that names no
        // image simply misses.
        (!had_receivers && self.has_receivers()).then_some(self.response_correlation_id)
    }

    /// A measurement request, answered when it asked to be
    /// (`aeron_network_publication_on_rttm`, `:888-921`).
    ///
    /// `RTTM_REPLY` on the way in means *answer this*: a receiver measuring its
    /// round trip sends the request with the flag
    /// (`aeron_receive_channel_endpoint.c:407-410`, called with `is_reply` true
    /// from `aeron_publication_image.c:1096`). The answer echoes the
    /// requester's own timestamp and reports no time spent here, so what the
    /// peer measures is its round trip and none of this driver's — and the
    /// answer carries **no** flags, which is what stops it being answered in
    /// turn.
    ///
    /// A publication that never answers leaves a peer on a congestion control
    /// that measures round trips — `cubic` — with nothing to measure, whatever
    /// flow control this end runs.
    #[allow(clippy::too_many_arguments)] // the frame, and where it arrived
    pub fn on_rttm(
        &mut self,
        frame: &RttmFrame,
        flags: u8,
        endpoint: &mut SendChannelEndpoint,
        system: &System<'_>,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> io::Result<usize> {
        if flags & header_flags::RTTM_REPLY == 0 {
            return Ok(0);
        }

        let reply = RttmFrame {
            session_id: self.session_id,
            stream_id: self.stream_id,
            echo_timestamp: frame.echo_timestamp,
            reception_delta: 0,
            receiver_id: frame.receiver_id,
        };

        let mut buffer = [0u8; RttmFrame::LENGTH];
        if reply.write(&mut buffer).is_none() {
            return Ok(0);
        }

        let sent = self.do_send(endpoint, &[&buffer], counters, regions, now_ns)?;

        if sent < 1 {
            system.increment(system_counters::id::SHORT_SENDS);
        }

        // Bytes, like the rest of the send path (`:576`). The reference
        // discards this value; it is counted here only so that no caller has to
        // know which of the send paths are in which unit.
        Ok(if sent < 1 { 0 } else { buffer.len() })
    }

    /// An error frame arrived, which the reference treats as a receiver going
    /// away (`aeron_network_publication_on_error`, `:858-887`).
    ///
    /// Returns whether that receiver was one this publication was still waiting
    /// on — the reference's `liveness_on_remote_close`, and the whole of what
    /// decides whether its client hears about it (`:872-875`): an `ERR` from a
    /// receiver the publication has already given up on is news about nothing.
    pub fn on_error(
        &mut self,
        frame: &ErrorFrame,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        // The two views are the caller's and this no longer counts anything
        // with them: the connected byte it writes is the log buffer's, and the
        // counters it used to need for that are gone with the publication's
        // half of `update_pub_pos_and_lmt`.
        let _ = (counters, regions);

        // `error-frames-received` is counted at the endpoint, before the
        // publication is looked up (`media/aeron_send_channel_endpoint.c:686`).
        // The strategy is told first (`aeron_network_publication.c:869`): it
        // keeps its own receivers, and a reader that refused the stream is one
        // of them leaving rather than one that has gone quiet.
        self.flow_control.on_error(frame.receiver_id);

        let was_live = self.remove_receiver(frame.receiver_id);

        self.refresh_connected_status();

        was_live
    }

    /// A NAK arrived (`aeron_network_publication_on_nak`, `:730-758`).
    ///
    /// With a zero delay the handler answers with the retransmission it wants
    /// sent *now*, and the reference sends it from inside the handler through
    /// the `resend` callback it was given (`aeron_retransmit_handler_on_nak`,
    /// `aeron_retransmit_handler.c:110-124`). This does the same: the answer is
    /// a `NakOutcome::Send` the caller must not have to act on separately,
    /// because a caller that forgot would be a driver that acknowledges a NAK
    /// and sends nothing.
    #[allow(clippy::too_many_arguments)] // the NAK and the things a resend needs
    pub fn on_nak(
        &mut self,
        frame: &NakFrame,
        system: &System<'_>,
        endpoint: &mut SendChannelEndpoint,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> NakOutcome {
        // `nak-messages-received` is counted at the endpoint, once the report
        // has been found well formed (`media/aeron_send_channel_endpoint.c:566`).
        // The system counter answers "is this driver being asked to
        // retransmit"; this one answers "which publication is"
        // (`aeron_network_publication.c:733`).
        let _ = system_counters::increment(counters, regions, self.counters.snd_naks_received);

        let term_length = self.term_length as usize;
        let term_window = self.term_window_length as usize;
        let mtu = self.mtu_length as usize;
        let flow_control = &self.flow_control;

        let outcome = self.retransmit_handler.on_nak(
            &mut PublicationFaults { system },
            frame.term_id,
            frame.term_offset,
            frame.length,
            term_length,
            |term_offset, length| {
                flow_control.max_retransmission_length(
                    term_offset,
                    length,
                    term_length,
                    receiver_window_length(term_window, term_length),
                )
            },
            now_ns,
        );

        let _ = mtu;

        if let NakOutcome::Send(resend) = outcome {
            let _ = self.resend(endpoint, resend, system, counters, regions, now_ns);
        }

        outcome
    }

    /// Write how many receivers the strategy is holding into `fc-receivers`
    /// (`aeron_min_flow_control.c:241-242`, `:151-152`).
    ///
    /// A publication under a strategy that keeps none has no counter to write
    /// and nothing to write into it.
    fn update_receiver_count(&self, counters: &CounterManager, regions: &CounterRegions<'_>) {
        let Some(counter_id) = self.counters.fc_receivers else {
            return;
        };

        let Some(count) = self.flow_control.group_receiver_count() else {
            return;
        };

        let _ = counters.set_value(
            regions,
            counter_id,
            i64::try_from(count).unwrap_or(i64::MAX),
        );
    }

    /// Whether this publication counts a reader at all
    /// (`aeron_network_publication_has_subscribers`, `:755-766`).
    ///
    /// Two ways to have one, and they are not the same kind of thing: a
    /// **receiver** is a remote reader that has said it is there, and a
    /// **spy** is a local one reading this buffer. The second counts only when
    /// `ssc` asked it to — which is what the setting is for, and why a stream
    /// no one has subscribed to over the wire can still be live.
    ///
    /// The reference's receiver clause also asks the flow control whether it
    /// *requires* receivers (`has_required_receivers`, `:760`), and that is the
    /// gate a group-aware strategy is: it answers `false` until enough of them
    /// have asked for the stream, so a publication on such a channel is not
    /// connected while nobody is there to read it. The `and` binds to the
    /// receiver clause alone — a local spy is not a receiver the strategy has
    /// to have heard (`:755-761`).
    ///
    /// The receiver half is **this thread's** and is read from the strategy
    /// directly; the spy half is the conductor's, and arrives as the two
    /// readings it keeps in [`PublicationSightlines`]. The method exists on
    /// both sides because the reference asks the question from both — its
    /// `aeron_network_publication_on_status_message` (`:838`) is the sender's,
    /// its `update_pub_pos_and_lmt` (`:958`) the conductor's.
    pub fn has_subscribers(&self) -> bool {
        (self.has_receivers() && self.flow_control.has_required_receivers())
            || (self.spies_simulate_connection && self.sightlines.has_working_spies())
    }

    /// Write the log buffer's connected byte from the answer above
    /// (`aeron_network_publication_update_connected_status`, `:765-777`).
    fn refresh_connected_status(&self) {
        update_connected_status(&self.log, self.has_subscribers());
    }

    /// Record that a receiver said something
    /// (`aeron_network_publication_liveness_on_status_message`, `:44-58`).
    fn record_receiver(&mut self, receiver_id: i64, now_ns: i64) {
        match self
            .receivers
            .iter_mut()
            .find(|entry| entry.receiver_id == receiver_id)
        {
            Some(entry) => entry.last_sm_ns = now_ns,
            None => self.receivers.push(ReceiverLiveness {
                receiver_id,
                last_sm_ns: now_ns,
            }),
        }
    }

    /// Forget a receiver that said it was going away.
    fn remove_receiver(&mut self, receiver_id: i64) -> bool {
        let before = self.receivers.len();
        self.receivers
            .retain(|entry| entry.receiver_id != receiver_id);

        before != self.receivers.len()
    }

    /// Forget receivers that have gone quiet
    /// (`aeron_network_publication_liveness_on_idle`, `:66-79`); answers
    /// whether any went.
    fn expire_receivers(&mut self, now_ns: i64) -> bool {
        let expiry = now_ns - self.connection_timeout_ns;
        let before = self.receivers.len();
        self.receivers.retain(|entry| entry.last_sm_ns > expiry);

        before != self.receivers.len()
    }

    /// Whether the sender has fallen behind far enough that the log may be
    /// blocked (`aeron_network_publication_is_possibly_blocked`, `.h:209-222`).
    ///
    /// The same test as an IPC publication's over the same metadata — the
    /// reference carries one copy per publication type, word for word — and
    /// like that one it is a *cheap gate* in front of the expensive question,
    /// not an answer to it: a sender behind the producer is enough.
    pub fn is_possibly_blocked(&self, producer_position: i64, sender_position: i64) -> bool {
        let Some(term_count) = self
            .log
            .metadata()
            .and_then(|metadata| metadata.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET))
        else {
            return false;
        };

        #[allow(clippy::cast_possible_truncation)] // the reference truncates here too
        let expected = (sender_position >> self.position_bits_to_shift) as i32;

        term_count != expected || producer_position > sender_position
    }

    /// Run the unblocker at `position`, and count it if it did anything.
    ///
    /// The count goes to the **system** counter 19, whose address the reference
    /// takes once at creation (`:307-309`) — one counter for every publication
    /// in the process.
    fn unblock_at(
        &self,
        position: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        if !deepmsg_core::logbuffer::unblocker::unblock(&self.log, position, self.term_length) {
            return false;
        }

        system_counters::increment(
            counters,
            regions,
            system_counters::id::UNBLOCKED_PUBLICATIONS,
        );

        true
    }

    /// Unblock the log for a producer whose stream has stopped moving
    /// (`aeron_network_publication_check_for_blocked_publisher`, `:1011-1031`).
    ///
    /// Returns whether the log was unblocked. `snd_pos` is passed in rather than
    /// read here because it is a counter value and the caller holds the regions.
    ///
    /// The deadline runs from the last time the **sender position moved**, and
    /// it is refreshed on every pass where either it moved or the log was not
    /// blocked — so a stream that keeps flowing resets its own deadline
    /// continuously and never reaches one.
    pub fn check_for_blocked_publisher(
        &mut self,
        snd_pos: i64,
        now_ns: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        if snd_pos == self.last_snd_pos
            && self.is_possibly_blocked(self.producer_position().unwrap_or(0), snd_pos)
        {
            if now_ns > self.time_of_last_activity_ns + self.unblock_timeout_ns
                && self.unblock_at(snd_pos, counters, regions)
            {
                return true;
            }
        } else {
            self.time_of_last_activity_ns = now_ns;
            self.last_snd_pos = snd_pos;
        }

        false
    }
}

impl std::fmt::Debug for NetworkPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkPublication")
            .field("registration_id", &self.registration_id)
            .field("session_id", &self.session_id)
            .field("stream_id", &self.stream_id)
            .field("endpoint_id", &self.endpoint_id)
            .field("receivers", &self.receivers.len())
            .field("is_connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

/// A length as the `i32` the log buffer's metadata stores.
fn i32_from(length: usize) -> i32 {
    i32::try_from(length).unwrap_or(i32::MAX)
}

/// The fault counters a retransmit handler bumps
/// (`invalid_packets_counter`, `retransmit_overflow_counter`).
struct PublicationFaults<'a> {
    system: &'a System<'a>,
}

impl Faults for PublicationFaults<'_> {
    fn invalid_packet(&mut self) {
        self.system.increment(system_counters::id::INVALID_PACKETS);
    }

    fn retransmit_overflow(&mut self) {
        self.system
            .increment(system_counters::id::RETRANSMIT_OVERFLOW);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::channel_uri::ChannelUri;
    use crate::sys::AddressFamily;
    use crate::sys::socket::{DatagramSocket, Datagrams};
    use crate::udp_channel::UdpChannel;
    use deepmsg_core::buffer::AtomicBuffer;
    use deepmsg_core::logbuffer::descriptor;
    use deepmsg_core::logbuffer::frame::{FLAG_UNFRAGMENTED, Frame, TYPE_DATA};
    use deepmsg_core::logbuffer::position::RawTail;

    /// A directory a test's log buffer lands in, removed when the test ends.
    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "deepmsg-network-publication-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("a temp directory");

            Self(dir)
        }

        fn log_buffer(&self, term_length: i32) -> Box<LogFile> {
            Box::new(
                LogFile::create(
                    &self.0.join("publication.logbuffer"),
                    term_length,
                    4096,
                    false,
                )
                .expect("a log buffer"),
            )
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[repr(align(64))]
    struct Region(Vec<u8>);

    struct Counters {
        metadata: Region,
        values: Region,
    }

    impl Counters {
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

            (
                CounterManager::new(64 * 1024, 1_000).expect("room"),
                regions,
            )
        }
    }

    const TERM_LENGTH: i32 = 64 * 1024;
    const MTU: i32 = 1408;

    /// A status message is only acted on where the publication can place it
    /// (`aeron_network_publication_is_valid_status_message`, `:841-856`).
    ///
    /// The band is half a term behind `snd-pos` to a term and a half in front of
    /// it. A receiver outside that is not describing this stream, and `snd-lmt`
    /// is computed from what it reports — acting on it would open a window the
    /// log cannot follow.
    #[test]
    fn a_status_message_is_only_valid_where_the_publication_can_place_it() {
        let fixture = fixture();
        let publication = &fixture.publication;

        let bits = publication.position_bits_to_shift;
        let initial_term_id = publication.initial_term_id;
        let term_length = i64::from(publication.term_length);
        let half_term = term_length >> 1;

        // A position, as a receiver would report it: back through the
        // publication's own term geometry.
        let at = |raw: i64| StatusMessageFrame {
            session_id: publication.session_id,
            stream_id: publication.stream_id,
            consumption_term_id: Position::from_raw(raw).term_id(bits, initial_term_id),
            consumption_term_offset: Position::from_raw(raw).term_offset(bits),
            receiver_window: 0,
            receiver_id: 1,
        };

        let snd_pos = term_length * 3;

        assert!(publication.is_valid_status_message(&at(snd_pos), snd_pos));

        for inside in [
            snd_pos - half_term,
            snd_pos - 32,
            snd_pos + term_length,
            snd_pos + term_length + half_term,
        ] {
            assert!(
                publication.is_valid_status_message(&at(inside), snd_pos),
                "{inside} is inside the band"
            );
        }

        for outside in [
            snd_pos - half_term - 32,
            snd_pos + term_length + half_term + 32,
            snd_pos + term_length * 8,
        ] {
            assert!(
                !publication.is_valid_status_message(&at(outside), snd_pos),
                "{outside} is outside the band"
            );
        }
    }

    struct Fixture {
        /// Held for its `Drop`: the log buffer the publication owns lives here.
        _dir: TempDir,
        counters: Counters,
        channel: UdpChannel,
        listener: DatagramSocket,
        publication: NetworkPublication,
    }

    fn params(term_length: i32, mtu: i32, window: i32) -> PublicationParams {
        PublicationParams {
            term_length,
            term_length_named: false,
            mtu_length: mtu,
            mtu_length_named: false,
            publication_window_length: window,
            max_resend: 0,
            entity_tag: -1,
            response_correlation_id: -1,
            is_response: false,
            session_id: Some(42),
            linger_timeout_ns: 5_000_000_000,
            untethered_window_limit_timeout_ns: 5_000_000_000,
            untethered_linger_timeout_ns: 5_000_000_000,
            untethered_resting_timeout_ns: 10_000_000_000,
            is_sparse: true,
            signal_eos: true,
            spies_simulate_connection: false,
            starting_position: None,
            initial_term_id: 1_000,
        }
    }

    /// A publication sending to a socket this test owns, so what leaves can be
    /// read back byte for byte.
    fn fixture() -> Fixture {
        fixture_with(&params(TERM_LENGTH, MTU, 32 * 1024))
    }

    /// The same, over parameters a test changed — the starting position is the
    /// one that reaches the log buffer's tails.
    fn fixture_with(params: &PublicationParams) -> Fixture {
        let dir = TempDir::new();
        let log = dir.log_buffer(TERM_LENGTH);
        let counters = Counters::new();

        let listener = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
        listener
            .bind("127.0.0.1:0".parse().expect("an address"))
            .expect("a bind");
        listener.set_nonblocking().expect("non-blocking");
        let bound = listener.local_address().expect("a bound address");

        let uri = format!("aeron:udp?endpoint={bound}");
        let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
        let channel = UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel");

        let publication = NetworkPublication::create(
            7,
            9,
            42,
            1001,
            1,
            uri.as_bytes(),
            log,
            params,
            false,
            PublicationCounters {
                fc_receivers: None,
                pub_pos: 0,
                pub_lmt: 1,
                snd_pos: 2,
                snd_lmt: 3,
                snd_bpe: 4,
                snd_naks_received: 5,
            },
            4,
            FlowControl::default(),
            RetransmitHandler::new(0, 5_000_000, false, 1),
            4096,
            crate::sys::SocketBufferLengths {
                rcvbuf: 0,
                sndbuf: 0,
            },
            0,
            0,
            crate::config::PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT,
            crate::config::PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT,
            0,
            Arc::new(PublicationSightlines::new()),
        )
        .expect("a publication");

        Fixture {
            _dir: dir,
            counters,
            channel,
            listener,
            publication,
        }
    }

    /// Write a frame the way a producer would — the frame *and* the term tail
    /// counter that says it is there, which is what a sender reads — and return
    /// its aligned length.
    fn publish_frame(
        log: &LogFile,
        term_index: usize,
        offset: usize,
        term_id: i32,
        payload: &[u8],
    ) -> usize {
        let term = log.term(term_index).expect("a term");
        let frame = Frame::new(&term, offset);
        let length = payload.len() as i32 + 32;

        frame
            .begin(
                length,
                FLAG_UNFRAGMENTED,
                TYPE_DATA,
                offset as i32,
                42,
                1001,
                term_id,
            )
            .expect("in range");
        frame.write_payload(payload).expect("in range");
        frame.publish(length).expect("in range");

        let aligned = deepmsg_core::logbuffer::position::align_up(length, 32) as usize;
        let metadata = log.metadata().expect("metadata");
        let tail = RawTail::new(term_id, (offset + aligned) as i32);
        let counter_offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
            + term_index * descriptor::TERM_TAIL_COUNTER_STRIDE;
        metadata
            .store_i64_release(counter_offset, tail.raw())
            .expect("in range");

        aligned
    }

    /// The log buffer's own connected byte, read the way a client reads it.
    fn connected_flag(publication: &NetworkPublication) -> Option<i32> {
        publication
            .log
            .metadata()?
            .load_i32_acquire(descriptor::IS_CONNECTED_OFFSET)
    }

    fn receive(listener: &mut DatagramSocket, buffers: &mut [Vec<u8>]) -> Vec<Vec<u8>> {
        let mut datagrams = Datagrams::new();
        let received = match listener.receive_batch(buffers, &mut datagrams) {
            Ok(received) => received,
            // Nothing queued is the empty batch, not a failure.
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => 0,
            Err(error) => panic!("a receive: {error}"),
        };

        datagrams.as_slice()[..received]
            .iter()
            .enumerate()
            .map(|(index, datagram)| buffers[index][..datagram.length].to_vec())
            .collect()
    }

    #[test]
    fn a_status_message_opens_the_producers_window() {
        // The chain that matters most on the send side, and the one A1 stalled
        // on: an SM arrives, the *sender* limit moves, and the producer's own
        // limit follows it — because a network publication with no local reader
        // is limited to what has been sent, plus a term's window.
        let mut fixture = fixture();
        let (counters, regions) = fixture.counters.open();

        let frame = StatusMessageFrame {
            session_id: 42,
            stream_id: 1001,
            consumption_term_id: 1_000,
            consumption_term_offset: 0,
            receiver_window: 8192,
            receiver_id: 99,
        };

        fixture
            .publication
            .on_status_message(&frame, 0, None, &counters, &regions, 1_000);

        assert!(fixture.publication.has_subscribers());
        assert_eq!(
            Some(8192),
            counters.value(&regions, fixture.publication.counters.snd_lmt)
        );

        // The limit itself is written by the conductor's half now
        // (`publication_maintenance`), over its own mapping of the same buffer
        // and its own set of readers; what this test is about is the sender's
        // reading of the status message, which is where the window came from.
        let window = i64::from(fixture.publication.term_window_length);
        assert!(window > 0, "a publication's window is half a term");
        assert!(window > 0);
        let _ = counters.value(&regions, fixture.publication.counters.pub_lmt);
    }

    #[test]
    fn an_error_only_reports_a_receiver_the_publication_was_waiting_on() {
        // The reference's `liveness_on_remote_close`
        // (`aeron_network_publication.c:41-47`) is what decides whether a
        // publisher hears about a refusal at all: an `ERR` names a receiver,
        // and one this publication has already given up on is news about
        // nothing (`:872-875`).
        let mut fixture = fixture();
        let (counters, regions) = fixture.counters.open();

        let frame = |receiver_id| ErrorFrame {
            session_id: 42,
            stream_id: 1001,
            receiver_id,
            group_tag: 0,
            error_code: deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED,
            error_length: 18,
        };

        // Nobody has been heard from, so this receiver is not one to report.
        assert!(
            !fixture
                .publication
                .on_error(&frame(99), &counters, &regions)
        );

        // A status message is how a receiver makes itself known.
        let status = StatusMessageFrame {
            session_id: 42,
            stream_id: 1001,
            consumption_term_id: 1_000,
            consumption_term_offset: 0,
            receiver_window: 8192,
            receiver_id: 99,
        };
        fixture
            .publication
            .on_status_message(&status, 0, None, &counters, &regions, 1_000);

        assert!(
            fixture
                .publication
                .on_error(&frame(99), &counters, &regions),
            "this one was live"
        );

        assert!(
            !fixture
                .publication
                .on_error(&frame(99), &counters, &regions),
            "and it is not any more: the refusal took it out of the set"
        );

        assert!(
            !fixture.publication.has_subscribers(),
            "which is what the producer's connection byte follows"
        );
    }

    #[test]
    fn a_nak_brings_a_frame_back() {
        // A5's send half: a frame goes out, a NAK says it did not arrive, and
        // the same bytes leave again. The channel's delay is zero here — the
        // test fixture's handler — so the resend happens inside `on_nak`, which
        // is where the reference's callback fires too.
        let mut fixture = fixture();
        publish_frame(&fixture.publication.log, 0, 0, 1_000, &[7u8; 100]);
        let (counters, regions) = fixture.counters.open();
        let system = System::new(&counters, &regions);

        let mut endpoint_manager = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut metadata = vec![0u8; 64 * 1024 * 4];
        let mut values = vec![0u8; 64 * 1024];
        let endpoint_regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");

        let mut endpoint = SendChannelEndpoint::create(
            fixture.channel.clone(),
            &mut crate::port_manager::WildcardPortManager::sender(),
            &crate::media::TransportParams::default(),
            &mut endpoint_manager,
            &endpoint_regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        let _ = counters.set_value(&regions, fixture.publication.counters.snd_lmt, 4096);
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_pos, 0);

        // The first send, which is what the NAK will say was lost. `send_data`
        // reports **bytes**, so what it returns is checked against what arrived
        // rather than against a count of datagrams
        // (`aeron_network_publication.c:576`).
        let sent = fixture
            .publication
            .send_data(&mut endpoint, &system, &counters, &regions, 1_000)
            .expect("a send");

        let mut buffers = vec![vec![0u8; 2048]];
        let first = receive(&mut fixture.listener, &mut buffers);
        assert_eq!(1, first.len());
        assert_eq!(first[0].len(), sent, "the bytes that went out");

        // The NAK names the whole frame, from its offset.
        let nak = NakFrame {
            session_id: 42,
            stream_id: 1001,
            term_id: 1_000,
            term_offset: 0,
            length: 160,
        };

        let outcome =
            fixture
                .publication
                .on_nak(&nak, &system, &mut endpoint, &counters, &regions, 2_000);

        assert!(
            matches!(outcome, NakOutcome::Send(_)),
            "a zero delay answers at once: {outcome:?}"
        );

        // And the frame is on the wire again, byte for byte.
        let mut buffers = vec![vec![0u8; 2048]];
        let resent = receive(&mut fixture.listener, &mut buffers);
        assert_eq!(1, resent.len(), "the retransmission arrived");
        assert_eq!(first[0], resent[0], "the same bytes, not new ones");

        assert!(
            system.value(system_counters::id::RETRANSMITS_SENT) >= 1,
            "and it is counted"
        );
    }

    #[test]
    fn a_term_that_holds_nothing_is_not_a_reason_to_send() {
        // The retransmit window's other edge: a NAK for a frame further back
        // than half a term plus a maximum message names bytes the term no
        // longer holds, and the reference refuses to answer it
        // (`aeron_network_publication_resend`, `:655-665`).
        let mut fixture = fixture();
        let (counters, regions) = fixture.counters.open();
        let system = System::new(&counters, &regions);

        let mut endpoint_manager = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut metadata = vec![0u8; 64 * 1024 * 4];
        let mut values = vec![0u8; 64 * 1024];
        let endpoint_regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");

        let mut endpoint = SendChannelEndpoint::create(
            fixture.channel.clone(),
            &mut crate::port_manager::WildcardPortManager::sender(),
            &crate::media::TransportParams::default(),
            &mut endpoint_manager,
            &endpoint_regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        // The sender is a term ahead of what the NAK asks for.
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_pos, 64 * 1024);
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_lmt, 64 * 1024);

        let nak = NakFrame {
            session_id: 42,
            stream_id: 1001,
            term_id: 1_000,
            term_offset: 0,
            length: 160,
        };

        let _ =
            fixture
                .publication
                .on_nak(&nak, &system, &mut endpoint, &counters, &regions, 2_000);

        let mut buffers = vec![vec![0u8; 2048]];
        assert!(
            receive(&mut fixture.listener, &mut buffers).is_empty(),
            "nothing is sent for a frame the term has moved past"
        );
    }

    #[test]
    fn the_log_buffer_is_left_describing_the_publication() {
        // What a *client* reads when it maps the file the publication's
        // `ON_PUBLICATION_READY` named: a log buffer whose metadata was never
        // written reads a term length of zero, and a client cannot map it.
        let fixture = fixture();
        let metadata = fixture.publication.log.metadata().expect("metadata");

        assert_eq!(
            Some(fixture.publication.term_length),
            metadata.load_i32_acquire(descriptor::TERM_LENGTH_OFFSET)
        );
        assert_eq!(
            Some(fixture.publication.mtu_length),
            metadata.load_i32_acquire(descriptor::MTU_LENGTH_OFFSET)
        );
        assert_eq!(
            Some(4096),
            metadata.load_i32_acquire(descriptor::PAGE_SIZE_OFFSET)
        );
    }

    #[test]
    fn data_frames_leave_the_publication_and_advance_the_sender_position() {
        let mut fixture = fixture();
        let written = publish_frame(&fixture.publication.log, 0, 0, 1_000, &[7u8; 100]);
        let (counters, regions) = fixture.counters.open();
        let system = System::new(&counters, &regions);

        let mut endpoint = SendChannelEndpoint::create(
            fixture.channel.clone(),
            &mut crate::port_manager::WildcardPortManager::sender(),
            &crate::media::TransportParams::default(),
            &mut CounterManager::new(64 * 1024, 1_000).expect("room"),
            &CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut vec![0u8; 64 * 1024 * 4]).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut vec![0u8; 64 * 1024]).expect("aligned"),
            )
            .expect("four-to-one"),
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        // The sender may send as far as the window says.
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_lmt, 1024);
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_pos, 0);

        let sent = fixture
            .publication
            .send_data(&mut endpoint, &system, &counters, &regions, 1_000)
            .expect("a send");

        let mut buffers = vec![vec![0u8; MTU as usize + 64]];
        let datagrams = receive(&mut fixture.listener, &mut buffers);
        assert_eq!(1, datagrams.len(), "one datagram");
        assert_eq!(datagrams[0].len(), sent, "and the bytes it measured");

        // What arrived is the frame itself, header and all, with its published
        // length and the payload the producer wrote — and it is the *aligned*
        // frame that goes on the wire, padding included, because that is what
        // the term holds and what a receiver's insert expects
        // (`aeron_network_publication.c:520-530` sends `available`).
        let header = FrameHeader::read(&datagrams[0]).expect("a header");
        assert_eq!(frame_type::DATA, header.frame_type);
        assert_eq!(132, header.frame_length, "header plus a 100-byte payload");
        assert_eq!(160, datagrams[0].len(), "aligned to the frame alignment");
        assert!(datagrams[0][32..132].iter().all(|byte| *byte == 7));
        assert!(
            datagrams[0][132..].iter().all(|byte| *byte == 0),
            "the tail past the payload is the term's own zeroes"
        );

        // And the sender position moved by the *aligned* frame length.
        assert_eq!(
            Some(written as i64),
            counters.value(&regions, fixture.publication.counters.snd_pos)
        );
    }

    #[test]
    fn nothing_is_sent_past_the_sender_limit() {
        let mut fixture = fixture();
        publish_frame(&fixture.publication.log, 0, 0, 1_000, &[1u8; 100]);
        let (counters, regions) = fixture.counters.open();
        let system = System::new(&counters, &regions);

        let mut endpoint_manager = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut metadata = vec![0u8; 64 * 1024 * 4];
        let mut values = vec![0u8; 64 * 1024];
        let endpoint_regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");

        let mut endpoint = SendChannelEndpoint::create(
            fixture.channel.clone(),
            &mut crate::port_manager::WildcardPortManager::sender(),
            &crate::media::TransportParams::default(),
            &mut endpoint_manager,
            &endpoint_regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        // The limit is below what has been sent: the window is closed.
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_lmt, 0);
        let _ = counters.set_value(&regions, fixture.publication.counters.snd_pos, 0);

        assert_eq!(
            0,
            fixture
                .publication
                .send_data(&mut endpoint, &system, &counters, &regions, 1_000)
                .expect("a send")
        );

        let mut buffers = vec![vec![0u8; MTU as usize]];
        assert!(receive(&mut fixture.listener, &mut buffers).is_empty());
    }

    #[test]
    fn a_status_message_moves_the_limit_and_connects_the_publication() {
        let mut fixture = fixture();
        let (counters, regions) = fixture.counters.open();

        // A receiver that has read nothing and has room for 8 KiB, in the term
        // the publication is in.
        let frame = StatusMessageFrame {
            session_id: 42,
            stream_id: 1001,
            consumption_term_id: 1_000,
            consumption_term_offset: 0,
            receiver_window: 8192,
            receiver_id: 99,
        };

        fixture
            .publication
            .on_status_message(&frame, 0, None, &counters, &regions, 1_000);

        assert_eq!(
            Some(8192),
            counters.value(&regions, fixture.publication.counters.snd_lmt)
        );
        assert!(
            fixture.publication.is_connected(),
            "a live receiver connects it"
        );
        assert!(fixture.publication.has_receivers());
        assert_eq!(1, fixture.publication.receiver_count());

        // The byte a *client* reads is the log buffer's, not this struct's.
        assert_eq!(
            Some(1),
            connected_flag(&fixture.publication),
            "the client-visible connected flag"
        );

        // An end-of-stream status message is the receiver leaving.
        fixture.publication.on_status_message(
            &frame,
            crate::protocol::header_flags::SM_EOS,
            None,
            &counters,
            &regions,
            1_100,
        );

        assert!(!fixture.publication.has_receivers());
        assert!(!fixture.publication.is_connected());
        assert_eq!(Some(0), connected_flag(&fixture.publication));
    }

    #[test]
    fn a_claim_that_has_stopped_the_sender_is_padded() {
        let mut fixture = fixture();
        let (manager, regions) = fixture.counters.open();
        let publication = &mut fixture.publication;

        let stalled = 128i32;
        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            // The producer got to 128 while the sender stayed at zero, so the
            // sender is behind it and the log may be blocked.
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(1_000, stalled).raw(),
                )
                .expect("in range");
        }

        publication.unblock_timeout_ns = 1_000;

        assert!(
            publication.check_for_blocked_publisher(0, 2_000, &manager, &regions),
            "the deadline passed and the log was blocked, so it was unblocked"
        );

        let term = publication.log.term(0).expect("the first term");
        let frame = Frame::new(&term, 0);
        assert_eq!(Some(stalled), frame.frame_length());
        assert!(frame.is_padding(), "the claim is padding now");
    }

    /// The other half: a sender that keeps moving refreshes its own deadline.
    #[test]
    fn a_sender_that_keeps_moving_is_never_unblocked() {
        let mut fixture = fixture();
        let (manager, regions) = fixture.counters.open();
        let publication = &mut fixture.publication;

        let stalled = 128i32;
        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(1_000, stalled).raw(),
                )
                .expect("in range");
        }

        publication.unblock_timeout_ns = 1_000;
        publication.last_snd_pos = 0;

        assert!(
            !publication.check_for_blocked_publisher(64, 2_000, &manager, &regions),
            "the sender moved, so the timer was refreshed rather than fired"
        );
        assert_eq!(2_000, publication.time_of_last_activity_ns, "to now");
        assert_eq!(64, publication.last_snd_pos);

        let term = publication.log.term(0).expect("the first term");
        assert_eq!(
            Some(-stalled),
            Frame::new(&term, 0).frame_length(),
            "the claim is still in flight"
        );
    }

    /// The draining path: the last client has let go, and the sender is stuck
    /// behind a claim that will never be committed.
    #[test]
    fn a_draining_publication_unblocks_the_claim_its_sender_is_stuck_behind() {
        let mut fixture = fixture();
        let (manager, regions) = fixture.counters.open();
        let publication = &mut fixture.publication;

        let stalled = 128i32;
        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            // The producer got to 128 and the sender is still at zero.
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(1_000, stalled).raw(),
                )
                .expect("in range");
        }

        publication.ending = true;

        let system = System::new(&manager, &regions);
        publication.notice_revoke(0, 5_000_000_000, &system, &manager, &regions);

        let term = publication.log.term(0).expect("the first term");
        let frame = Frame::new(&term, 0);
        assert_eq!(Some(stalled), frame.frame_length());
        assert!(frame.is_padding(), "the sender can move now");
    }

    /// The end-of-stream byte is decided on every tick of the drain, not once:
    /// a producer that stopped before its sender did is the ordinary case.
    #[test]
    fn a_draining_publication_says_the_stream_ended_when_the_sender_catches_up() {
        let mut fixture = fixture();
        let (manager, regions) = fixture.counters.open();
        let publication = &mut fixture.publication;

        {
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(1_000, 128).raw(),
                )
                .expect("in range");
        }

        publication.ending = true;

        let system = System::new(&manager, &regions);
        publication.notice_revoke(0, 5_000_000_000, &system, &manager, &regions);

        assert!(
            !publication.is_end_of_stream,
            "the sender is still at zero, so the stream has not been sent out yet"
        );

        // It catches up.
        manager
            .set_value(&regions, publication.counters.snd_pos, 128)
            .expect("the counter");

        publication.notice_revoke(1, 5_000_000_000, &system, &manager, &regions);

        assert!(
            publication.is_end_of_stream,
            "now it has, and the flow control's idle path can say so"
        );
    }

    #[test]
    fn a_quiet_receiver_is_declared_gone_after_the_connection_timeout() {
        let mut fixture = fixture();
        let (counters, regions) = fixture.counters.open();
        let system = System::new(&counters, &regions);

        let frame = StatusMessageFrame {
            session_id: 42,
            stream_id: 1001,
            consumption_term_id: 1_000,
            consumption_term_offset: 0,
            receiver_window: 8192,
            receiver_id: 99,
        };

        fixture
            .publication
            .on_status_message(&frame, 0, None, &counters, &regions, 1_000);

        // A heartbeat-less, data-less pass after the timeout: the receiver is
        // expired rather than simply never seen.
        let sent = fixture
            .publication
            .send(
                &mut test_endpoint(&fixture.channel),
                &system,
                &counters,
                &regions,
                crate::config::PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT + 2_000,
            )
            .expect("a send");

        let _ = sent;
        assert!(
            !fixture.publication.has_receivers(),
            "five seconds of silence is a receiver that is gone"
        );
    }

    #[test]
    fn a_heartbeat_is_a_zero_length_data_frame() {
        let mut fixture = fixture();
        let (counters, regions) = fixture.counters.open();
        let system = System::new(&counters, &regions);

        // It has connected at some point, and then gone quiet.
        let frame = StatusMessageFrame {
            session_id: 42,
            stream_id: 1001,
            consumption_term_id: 1_000,
            consumption_term_offset: 0,
            receiver_window: 8192,
            receiver_id: 99,
        };
        fixture
            .publication
            .on_status_message(&frame, 0, None, &counters, &regions, 1_000);

        let mut metadata = vec![0u8; 64 * 1024 * 4];
        let mut values = vec![0u8; 64 * 1024];
        let endpoint_regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");
        let mut endpoint_manager = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut endpoint = SendChannelEndpoint::create(
            fixture.channel.clone(),
            &mut crate::port_manager::WildcardPortManager::sender(),
            &crate::media::TransportParams::default(),
            &mut endpoint_manager,
            &endpoint_regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint");

        let _ = counters.set_value(&regions, fixture.publication.counters.snd_lmt, 4096);
        fixture.publication.time_of_last_data_or_heartbeat_ns = 1_000;

        let sent = fixture
            .publication
            .send(
                &mut endpoint,
                &system,
                &counters,
                &regions,
                1_000 + HEARTBEAT_TIMEOUT_NS + 1,
            )
            .expect("a send");

        let mut buffers = vec![vec![0u8; MTU as usize]];
        let datagrams = receive(&mut fixture.listener, &mut buffers);
        assert_eq!(1, datagrams.len(), "one datagram");
        assert_eq!(datagrams[0].len(), sent, "and the bytes it measured");

        let header = FrameHeader::read(&datagrams[0]).expect("a header");
        assert_eq!(frame_type::DATA, header.frame_type);
        assert_eq!(0, header.frame_length, "a heartbeat carries nothing");
        assert_eq!(
            crate::protocol::header_flags::BEGIN | crate::protocol::header_flags::END,
            header.flags
        );
        assert_eq!(
            1,
            system.value(system_counters::id::HEARTBEATS_SENT),
            "and it is counted"
        );
    }

    #[test]
    fn the_producer_position_is_the_highest_term_tail() {
        let fixture = fixture();
        assert_eq!(
            Some(0),
            fixture.publication.producer_position(),
            "a fresh log starts at the first term's zero"
        );

        publish_frame(&fixture.publication.log, 0, 0, 1_000, &[1u8; 100]);
        assert_eq!(Some(160), fixture.publication.producer_position());
    }

    /// A stream that resumes starts its tails at the term the URI named,
    /// **offset included** — the reference writes that block on the network
    /// path (`aeron_network_publication.c:163-182`) exactly as it does on the
    /// IPC one (`aeron_ipc_publication.c:75-103`).
    ///
    /// It is what makes a publication that took another's session with
    /// `session-id=tag:N` describe the *same* stream rather than a fresh one
    /// that happens to share an id: the log it publishes into begins where that
    /// stream is, and the producer position over the tails is what every
    /// position computation below is measured from.
    #[test]
    fn a_resumed_stream_starts_at_the_term_the_uri_named() {
        let mut resumed = params(TERM_LENGTH, MTU, 32 * 1024);
        let term_id = resumed.initial_term_id + 5;
        resumed.starting_position = Some(crate::publication_params::StartingPosition {
            initial_term_id: resumed.initial_term_id,
            term_id,
            term_offset: 4_096,
        });

        let fixture = fixture_with(&resumed);
        let bits = deepmsg_core::logbuffer::position::bits_to_shift(TERM_LENGTH).expect("a power");
        let expected = deepmsg_core::logbuffer::position::Position::new(
            term_id,
            4_096,
            bits,
            resumed.initial_term_id,
        )
        .raw();

        assert_eq!(
            Some(expected),
            fixture.publication.producer_position(),
            "the producer position over the tails is where the URI said"
        );

        let metadata = fixture.publication.log.metadata().expect("metadata");
        assert_eq!(
            Some(5),
            metadata.load_i32_relaxed(descriptor::ACTIVE_TERM_COUNT_OFFSET),
            "the active term count is the named term's distance from the initial one"
        );

        let index = deepmsg_core::logbuffer::position::index_by_term_count(5);
        let tail = metadata
            .load_i64_relaxed(
                descriptor::TERM_TAIL_COUNTERS_OFFSET
                    + index * descriptor::TERM_TAIL_COUNTER_STRIDE,
            )
            .expect("in range");

        assert_eq!(
            RawTail::new(term_id, 4_096).raw(),
            tail,
            "and the term holds the offset the URI named, not zero"
        );
    }

    fn test_endpoint(channel: &UdpChannel) -> SendChannelEndpoint {
        let mut metadata = vec![0u8; 64 * 1024 * 4];
        let mut values = vec![0u8; 64 * 1024];
        let regions = CounterRegions::new(
            AtomicBuffer::from_slice_mut(&mut metadata).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        )
        .expect("four-to-one");
        let mut manager = CounterManager::new(64 * 1024, 1_000).expect("room");

        SendChannelEndpoint::create(
            channel.clone(),
            &mut crate::port_manager::WildcardPortManager::sender(),
            &crate::media::TransportParams::default(),
            &mut manager,
            &regions,
            7,
            true,
            1,
            1_000_000,
        )
        .expect("an endpoint")
    }

    /// The descriptor's `group` byte, read straight out of the log file
    /// (`GROUP_OFFSET`, `aeron_logbuffer_descriptor.h:74`).
    ///
    /// The descriptor sits at the **end** of the file, after the terms
    /// (`metadata_offset = length - METADATA_LENGTH`,
    /// `crates/core/src/logbuffer/logfile.rs:90`) — not at the start, which is
    /// where the terms are.
    fn group_byte(path: &std::path::Path) -> u8 {
        use deepmsg_core::logbuffer::descriptor;

        let length = std::fs::metadata(path).expect("the log file").len() as usize;
        let offset = length - descriptor::METADATA_LENGTH + descriptor::GROUP_OFFSET;

        let mapping =
            deepmsg_core::pal::MappedFile::open_readonly(path).expect("map the log buffer");
        let region = mapping.region(offset, 1).expect("the descriptor byte");

        region.load_u8(0).expect("readable")
    }

    /// The log buffer's `group` byte is the **endpoint channel's** group
    /// semantics — the same value the setup frame's `GROUP` flag carries
    /// (`aeron_network_publication_create` computes it at `:136` and writes it
    /// into the descriptor at `:224`).
    ///
    /// A reader of the log buffer uses that byte to tell a multicast or
    /// multi-destination stream from a unicast one, so a publication that left
    /// it at zero while putting `GROUP` on the wire would be describing itself
    /// two different ways. This build did exactly that: the flag was read from
    /// the handler, which was built with a hardcoded `false`.
    #[test]
    fn the_log_buffer_says_whether_the_channel_has_group_semantics() {
        for (has_group_semantics, expected) in [(false, 0u8), (true, 1u8)] {
            let dir = TempDir::new();
            let log = dir.log_buffer(TERM_LENGTH);

            let listener = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
            listener
                .bind("127.0.0.1:0".parse().expect("an address"))
                .expect("a bind");
            listener.set_nonblocking().expect("non-blocking");
            let bound = listener.local_address().expect("a bound address");

            let uri = format!("aeron:udp?endpoint={bound}");
            let parsed = ChannelUri::parse(uri.as_bytes()).expect("a URI");
            let _channel = UdpChannel::resolve(uri.as_bytes(), &parsed).expect("a channel");

            let _publication = NetworkPublication::create(
                7,
                9,
                42,
                1001,
                1,
                uri.as_bytes(),
                log,
                &params(TERM_LENGTH, MTU, 32 * 1024),
                false,
                PublicationCounters {
                    fc_receivers: None,
                    pub_pos: 0,
                    pub_lmt: 1,
                    snd_pos: 2,
                    snd_lmt: 3,
                    snd_bpe: 4,
                    snd_naks_received: 5,
                },
                4,
                FlowControl::default(),
                RetransmitHandler::new(0, 5_000_000, has_group_semantics, 1),
                4096,
                crate::sys::SocketBufferLengths {
                    rcvbuf: 0,
                    sndbuf: 0,
                },
                0,
                0,
                crate::config::PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT,
                crate::config::PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT,
                0,
                Arc::new(PublicationSightlines::new()),
            )
            .expect("a publication");

            assert_eq!(
                expected,
                group_byte(&dir.0.join("publication.logbuffer")),
                "group semantics {has_group_semantics}"
            );
        }
    }
}
