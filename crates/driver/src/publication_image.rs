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

use deepmsg_cnc::{CounterManager, CounterRegions};
use deepmsg_core::logbuffer::descriptor;
use deepmsg_core::logbuffer::logfile::LogFile;
use deepmsg_core::logbuffer::position::{Position, RawTail};

use crate::flowcontrol::receiver_window_length;
use crate::protocol::{DataFrame, FrameHeader, HEADER_LENGTH, header_flags};
use crate::subscribable::{Subscribable, TetherablePosition};
use crate::system_counters::{self, System};

/// How long a status message may go unsent before one is sent anyway
/// (`aeron.status.message.timeout`, 200 ms).
pub const STATUS_MESSAGE_TIMEOUT_NS: i64 = 200_000_000;

/// How long an image may go without a packet before it starts draining
/// (`AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT`,
/// `aeron-driver/src/main/c/aeron_driver_context.c:204` — ten seconds).
pub const IMAGE_LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;

/// How many status-message periods a drained image waits before lingering
/// (`AERON_IMAGE_SM_EOS_MULTIPLE`,
/// `aeron-driver/src/main/c/aeron_publication_image.h:35`).
pub const IMAGE_SM_EOS_MULTIPLE: i64 = 5;

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
}

/// An image: a stream rebuilt from datagrams.
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
    /// Where status messages and NAKs go: the source of the packets this image
    /// was built from, or the channel's control address when it named one
    /// (`aeron_publication_image_connection_set_control_address`, `:30-37`).
    pub control_address: Option<SocketAddr>,
    /// When a packet was last seen, which is what decides draining.
    pub time_of_last_packet_ns: i64,
    /// Whether the sender has said the stream is over.
    pub is_end_of_stream: bool,
    /// Whether the status messages being sent carry the end-of-stream flag.
    pub is_sending_eos_sm: bool,
    /// Whether the sender revoked the stream (a `REVOKED` flag on an
    /// end-of-stream heartbeat).
    pub is_revoked: bool,
    /// Why this image was rejected, when it was — an image that cannot be
    /// built still has to tell its sender so (`:974-996`).
    pub invalidation_reason: Option<String>,
    /// The position the next status message reports.
    next_sm_position: i64,
    /// The window the next status message advertises.
    next_sm_receiver_window_length: i32,
    /// What the last status message said, which is what bounds an over-run.
    last_sm_position: i64,
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
    /// The window this endpoint offers (`receiver.window.length`).
    initial_window_length: i32,
    /// The largest window it will ever offer (`max_window_length`).
    max_receiver_window_length: i32,
    /// How long this image may go quiet before it drains.
    liveness_timeout_ns: i64,
    /// Where it is in its life.
    pub state: ImageState,
    /// When the state last changed, for the linger timeout.
    pub time_of_last_state_change_ns: i64,
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
        channel: &[u8],
        log: Box<LogFile>,
        setup: &crate::protocol::SetupFrame,
        _source: SocketAddr,
        control_address: SocketAddr,
        counters: ImageCounters,
        initial_window_length: i32,
        sm_timeout_ns: i64,
        page_size: usize,
        now_ns: i64,
    ) -> Self {
        let bits =
            deepmsg_core::logbuffer::position::bits_to_shift(setup.term_length).unwrap_or(16);
        let initial_position = Position::new(
            setup.active_term_id,
            setup.term_offset,
            bits,
            setup.initial_term_id,
        )
        .raw();

        // The window the receiver offers: its configured one, cut to half a
        // term because a receiver needs the other half to keep reading
        // (`aeron_receiver_window_length`).
        let window = receiver_window_length(
            initial_window_length.unsigned_abs() as usize,
            setup.term_length.unsigned_abs() as usize,
        );
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let window = window as i32;

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
                    receiver_window_length: window,
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
                    group: 0,
                    is_response: false,
                    rejoin: false,
                    reliable: true,
                    sparse: false,
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
            control_address: Some(control_address),
            time_of_last_packet_ns: now_ns,
            is_end_of_stream: false,
            is_sending_eos_sm: false,
            is_revoked: false,
            invalidation_reason: None,
            next_sm_position: initial_position,
            next_sm_receiver_window_length: window,
            last_sm_position: initial_position,
            last_overrun_threshold: initial_position + i64::from(setup.term_length / 2),
            last_sm_change_number: 0,
            sm_change_number: 0,
            // The first status message is due at once: the sender is still
            // saying `SETUP` until one arrives, and this is the answer it is
            // waiting for.
            next_sm_deadline_ns: now_ns - 1,
            sm_timeout_ns,
            initial_window_length: window,
            max_receiver_window_length: window,
            liveness_timeout_ns: IMAGE_LIVENESS_TIMEOUT_NS,
            state: ImageState::Active,
            time_of_last_state_change_ns: now_ns,
        }
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
    /// message answers to when the channel named no control address.
    ///
    /// Returns how many bytes were accepted, which is the packet's length for
    /// anything that was written and zero for anything that was not. Three
    /// kinds of packet are *accepted without being written*, and all three are
    /// the protocol working rather than failing:
    ///
    /// * a **heartbeat** — a zero-length DATA frame — which carries liveness
    ///   and the end-of-stream flag and nothing else;
    /// * a packet older than what is already there, which is the cooling tail
    ///   of a retransmission;
    /// * a packet already in the term, which `insert_packet`'s
    ///   "write only into an empty slot" rule refuses.
    #[allow(clippy::too_many_arguments)] // the packet, its place, and the counters
    pub fn insert_packet(
        &mut self,
        term_id: i32,
        term_offset: i32,
        packet: &[u8],
        source: SocketAddr,
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
        let Some(payload_length) =
            validate_packet(term_length, term_offset, packet, self.mtu_length)
        else {
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
                self.track_connection(source, now_ns);
                self.on_heartbeat(packet, packet_position, counters, regions, system);
            } else {
                system.increment(system_counters::id::FLOW_CONTROL_UNDER_RUNS);
            }

            return 0;
        }

        if packet_position >= self.last_sm_position - i64::from(self.max_receiver_window_length) {
            // Inside what the last status message asked for: a packet behind
            // the window bottom is one the receiver can no longer ask for, and
            // writing it would be writing history over a reused term.
            self.track_connection(source, now_ns);
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
        } else {
            system.increment(system_counters::id::FLOW_CONTROL_UNDER_RUNS);
        }

        packet.len()
    }

    /// What an end-of-stream heartbeat does (`:790-830`).
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

        let proposed_position = packet_position + i64::from(HEADER_LENGTH as i32);
        let _ = counters.set_value(
            regions,
            self.counters.rcv_hwm,
            self.hwm_position(counters, regions).max(proposed_position),
        );

        if header.flags & header_flags::EOS == 0 || self.is_end_of_stream {
            return;
        }

        // The end of the stream: where it ended, and whether it was revoked.
        let eos_position = self.find_eos_position(counters, regions);
        self.is_end_of_stream = true;

        if header.flags & header_flags::REVOKED != 0 {
            self.is_revoked = true;
            system.increment(system_counters::id::PUBLICATION_IMAGES_REVOKED);
        }

        if let Some(metadata) = self.log.metadata() {
            let _ =
                metadata.store_i64_release(descriptor::END_OF_STREAM_POSITION_OFFSET, eos_position);
        }
    }

    /// Where the stream ended, from the last data frame
    /// (`aeron_publication_find_eos_position`).
    fn find_eos_position(&self, counters: &CounterManager, regions: &CounterRegions<'_>) -> i64 {
        self.hwm_position(counters, regions)
    }

    /// Remember where this connection answers
    /// (`aeron_publication_image_track_connection`, `:555-600`): the source of
    /// the packets, which is the implicit-unicast control address.
    fn track_connection(&mut self, source: SocketAddr, now_ns: i64) {
        if self.control_address.is_none() {
            self.control_address = Some(source);
        }

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
    ) -> usize {
        let hwm_position = self.hwm_position(counters, regions);

        let Some(min_sub_pos) = self.subscribers.min_active_position(counters, regions) else {
            // Nobody is reading: there is no position to advance and no window
            // to offer. Status messages still go out on their timeout, which is
            // what keeps the sender's view of this endpoint alive.
            return 0;
        };

        let max_sub_pos = self
            .subscribers
            .max_active_position(counters, regions)
            .unwrap_or(min_sub_pos);

        let rcv_pos = counters.value(regions, self.counters.rcv_pos).unwrap_or(0);
        let rebuild_position = rcv_pos.max(max_sub_pos);

        // Where the contiguous data ends: the highest position whose term
        // holds published frames from `rebuild_position` onwards.
        let rebuilt = self.contiguous_position(rebuild_position, hwm_position);

        system_counters::propose_max(counters, regions, self.counters.rcv_pos, rebuilt);

        // A status message is due when the reader has moved a quarter of a
        // window since the last one (`:545-553`).
        let window_length = self.next_sm_receiver_window_length;
        let threshold = window_length / 4;

        if min_sub_pos > self.next_sm_position + i64::from(threshold)
            || window_length != self.next_sm_receiver_window_length
        {
            self.schedule_status_message(min_sub_pos, window_length, counters, regions, now_ns);
            1
        } else {
            0
        }
    }

    /// The highest position from `from` that a reader may be moved to: the end
    /// of the contiguous run of published frames.
    fn contiguous_position(&self, from: i64, hwm: i64) -> i64 {
        let term_length = self.term_length.unsigned_abs() as usize;
        let mut position = from;

        while position < hwm {
            let index = Position::from_raw(position).index(self.position_bits_to_shift);
            let Some(term) = self.log.term(index) else {
                break;
            };

            let offset = Position::from_raw(position).term_offset(self.position_bits_to_shift);
            let offset = usize::try_from(offset.unsigned_abs()).unwrap_or(0);
            let left = term_length - offset.min(term_length);

            match deepmsg_core::logbuffer::scan::scan_for_availability(
                &term.as_read_only(),
                offset,
                i32::try_from(left).unwrap_or(i32::MIN),
                i32::try_from(left).unwrap_or(i32::MIN),
            ) {
                deepmsg_core::logbuffer::scan::Availability::Ready { available, padding } => {
                    let step = i64::from(available + padding);

                    if step <= 0 {
                        break;
                    }

                    position += step;
                }
                _ => break,
            }
        }

        position.min(hwm)
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
        let has_timed_out = self.next_sm_deadline_ns < now_ns;

        if self.invalidation_reason.is_some() {
            if has_timed_out {
                self.next_sm_deadline_ns = now_ns + self.sm_timeout_ns;
            }

            return Ok(0);
        }

        let Some(control_address) = self.control_address else {
            return Ok(0);
        };

        if self.sm_change_number == self.last_sm_change_number && !has_timed_out {
            return Ok(0);
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

        let sent = endpoint.send_sm(
            control_address,
            self.stream_id,
            self.session_id,
            term_id,
            term_offset,
            self.next_sm_receiver_window_length,
            flags,
        )?;

        if sent > 0 {
            system.increment(system_counters::id::STATUS_MESSAGES_SENT);
        }

        let _ = (counters, regions);

        self.last_sm_position = self.next_sm_position;
        self.last_overrun_threshold = self.next_sm_position + i64::from(self.term_length / 2);
        self.last_sm_change_number = self.sm_change_number;
        self.next_sm_deadline_ns = now_ns + self.sm_timeout_ns;

        Ok(usize::from(sent > 0))
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
                let quiet = now_ns > self.time_of_last_packet_ns + self.liveness_timeout_ns;
                let drained = self.is_end_of_stream && self.is_drained(counters, regions);

                if !self.has_subscribers() || quiet || drained {
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

                    self.state = ImageState::Linger;
                    self.time_of_last_state_change_ns = now_ns;
                    return true;
                }
            }
            ImageState::Linger => {
                let expired = now_ns > self.time_of_last_state_change_ns + self.liveness_timeout_ns;

                if !self.has_subscribers() || expired {
                    self.state = ImageState::Done;
                    return true;
                }
            }
            ImageState::Done => {}
        }

        false
    }

    /// Reject the image, with the reason (`aeron_publication_image_invalidate`,
    /// `:1383-1389`): a sender whose image could not be built is told with an
    /// `ERR` frame rather than left sending into silence.
    pub fn invalidate(&mut self, reason: &str) {
        self.invalidation_reason = Some(reason.to_owned());
    }

    /// The reason this image was rejected, if it was.
    pub fn invalidation_reason(&self) -> Option<&str> {
        self.invalidation_reason.as_deref()
    }

    /// The window this image offers, which a reader's congestion control would
    /// read back.
    pub const fn window_length(&self) -> i32 {
        self.next_sm_receiver_window_length
    }

    /// The initial window, for a caller that wants to report the configured
    /// value rather than the current one.
    pub const fn initial_window_length(&self) -> i32 {
        self.initial_window_length
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

/// `aeron_publication_image_validate_packet` (`:645-735`), minus the
/// timestamping an ATS channel would add.
///
/// Returns the packet's payload bytes: zero for a heartbeat, the packet's
/// length when every frame in it is contiguous and complete, and `None` when
/// the packet is not one an image may take.
fn validate_packet(
    term_length: i32,
    term_offset: i32,
    packet: &[u8],
    mtu_length: i32,
) -> Option<i32> {
    if term_offset < 0 || term_offset >= term_length {
        return None;
    }

    // A heartbeat is a whole data header with a frame length of zero.
    if is_heartbeat(packet, mtu_length) {
        return Some(0);
    }

    let mut offset = 0usize;
    let mut next_offset = i64::from(term_offset);
    let mut last_type = -1i16;

    while offset + HEADER_LENGTH <= packet.len() {
        let frame = FrameHeader::read(&packet[offset..])?;

        if frame.frame_length <= 0 {
            break;
        }

        last_type = frame.frame_type;

        // Only DATA and PAD belong in a term.
        if frame.frame_type != crate::protocol::frame_type::DATA
            && frame.frame_type != crate::protocol::frame_type::PAD
        {
            break;
        }

        let header = DataFrame::read(&packet[offset..])?;
        if i64::from(header.term_offset) != next_offset {
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

        if offset > packet.len().saturating_sub(HEADER_LENGTH) {
            break;
        }
    }

    if offset != packet.len()
        && (offset < packet.len() || last_type != crate::protocol::frame_type::PAD)
    {
        return None;
    }

    i32::try_from(packet.len()).ok()
}

/// Whether a packet is a heartbeat: a data header whose frame length says the
/// frame carries nothing (`aeron_publication_image_is_heartbeat`, `:600-610`).
fn is_heartbeat(packet: &[u8], mtu_length: i32) -> bool {
    let Some(header) = FrameHeader::read(packet) else {
        return false;
    };

    let expected = i32::try_from(DataFrame::LENGTH).unwrap_or(32);
    let _ = mtu_length;

    packet.len() >= expected as usize && header.frame_length == 0
}

/// The raw tails an image's log holds, for a caller that wants to see them.
pub fn tail_of(log: &LogFile, index: usize) -> Option<RawTail> {
    let metadata = log.metadata()?;
    let offset =
        descriptor::TERM_TAIL_COUNTERS_OFFSET + index * descriptor::TERM_TAIL_COUNTER_STRIDE;

    metadata.load_i64_acquire(offset).map(RawTail::from_raw)
}
