//! One client's publication over `aeron:ipc`.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_ipc_publication.{c,h}`. An IPC
//! publication is the log buffer the producer writes into plus the set of
//! positions reading it — the subscribers' `sub-pos` counters. There is no
//! image object and no transport: for `aeron:ipc` the subscriber maps *this*
//! publication's file, which is why the driver's whole job here is to create
//! it, keep the publisher limit honest, and tell subscribers where to look.
//!
//! # The driver writes two things into the log
//!
//! `pub-pos` — how far the producer has written — and `pub-lmt` — how far it
//! *may* write. Both are recomputed every conductor pass from the log's own
//! tail and the subscribers' positions ([`IpcPublication::update_pub_pos_and_lmt`]),
//! and `pub-lmt` is the entire backpressure mechanism: a producer that outruns
//! its readers stops being allowed to write.
//!
//! # Cleanup is not garbage collection
//!
//! Terms are reused, so a term must be zeroed before it becomes the next one —
//! otherwise a reader walking committed frames runs into the *previous*
//! rotation's lengths. [`IpcPublication::clean_buffer`] does it a chunk at a
//! time, following the slowest reader, and it zeroes the frame-length word
//! **last** and with a release: a reader that sees a zero length stops, and one
//! that saw the old length keeps reading bytes that are still there.

use deepmsg_cnc::{CounterManager, CounterRegions};
use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};
use deepmsg_core::logbuffer::descriptor;
use deepmsg_core::logbuffer::logfile::LogFile;
use deepmsg_core::logbuffer::position::{self, Position, RawTail};
use deepmsg_core::logbuffer::unblocker;

use crate::publication_params::PublicationParams;
use crate::subscribable::{
    Subscribable, SubscribableHooks, TetherState, TetherablePosition, UntetheredEvent,
};
use crate::sys::SocketBufferLengths;

/// Where a publication is in its life
/// (`aeron_ipc_publication.h:26-33`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Reading and writing.
    Active,
    /// Its last client is gone: still readable until every subscriber has read
    /// what was written, then it lingers and is removed.
    Draining,
    /// Drained: waiting out the linger timeout before it is removed.
    Linger,
}

/// One client's publication.
pub struct IpcPublication {
    /// The client's correlation id for `ADD_PUBLICATION`; also what names the
    /// log file and what `ON_AVAILABLE_IMAGE` calls this publication.
    pub registration_id: i64,
    /// The client that owns it.
    pub client_id: i64,
    /// The session this stream runs under.
    pub session_id: i32,
    /// The stream id.
    pub stream_id: i32,
    /// Where the stream's terms started.
    pub initial_term_id: i32,
    /// How long each term is.
    pub term_length: i32,
    /// The largest frame, which is in the log buffer's metadata and is what a
    /// second publication on this channel has to agree about before it may
    /// share this one (`aeron_confirm_publication_match`,
    /// `aeron_driver_conductor.c:1109-1136`).
    pub mtu_length: i32,
    /// `log2(term_length)`.
    pub bits_to_shift: u32,
    /// The term a resumed stream resumed in — the initial term id when the
    /// stream did not resume (`aeron_ipc_publication.c:168`).
    pub starting_term_id: i32,
    /// How far into it.
    pub starting_term_offset: i64,
    /// The channel as the client sent it, which is what the counters' keys and
    /// labels carry.
    pub channel: Vec<u8>,
    /// Whether this publication has exactly one producer
    /// (`log_meta_data->type`).
    pub is_exclusive: bool,
    /// Which request this channel answers, resolved at creation. It is one of
    /// the four things two `ADD_PUBLICATION`s have to agree about before the
    /// second may share the first (`aeron_driver_conductor.c:1770-1784`), and
    /// it is the value the **first** one was created with.
    pub response_correlation_id: i64,
    /// The log buffer itself.
    pub log: Box<LogFile>,
    /// `pub-pos`: written every pass from the log's tail.
    pub pub_pos_counter_id: i32,
    /// `pub-lmt`: the backpressure knob.
    pub pub_lmt_counter_id: i32,
    /// Who is reading, and how far.
    pub subscribers: Subscribable,
    /// Half a term by default: the slack the limit is computed from.
    pub term_window_length: i32,
    /// How long a drained publication lingers before it is removed.
    pub linger_timeout_ns: i64,
    /// How long a subscription may stall the limit before it stops counting.
    pub untethered_window_limit_timeout_ns: i64,
    /// The same for the lingering half of the tether cycle.
    pub untethered_linger_timeout_ns: i64,
    /// And the resting half.
    pub untethered_resting_timeout_ns: i64,
    /// How long a rejection lasts (`aeron_ipc_publication.c:177`, which takes
    /// it from `context->image_liveness_timeout_ns` — the same window an image
    /// waits before deciding a publication is gone, and the same setting that
    /// value comes from).
    pub liveness_timeout_ns: i64,
    /// Where the limit may next jump to (`aeron_ipc_publication.h:172`).
    trip_gain: i32,
    trip_limit: i64,
    /// How long the publisher's position may sit unmoved before its log is
    /// unblocked for it (`aeron.publication.unblock.timeout`, fifteen seconds).
    pub unblock_timeout_ns: i64,
    /// The highest subscriber position seen on the last pass.
    pub consumer_position: i64,
    /// How far the terms have been zeroed.
    clean_position: i64,
    /// The consumer position as of the last time it was seen to move
    /// (`conductor_fields.last_consumer_position`), and when that was
    /// (`time_of_last_consumer_position_change_ns`).
    ///
    /// The pair is what tells a stalled reader from a slow one, and it is why
    /// the unblocker needs no clock of its own: the timer is refreshed **only**
    /// when the position moves, so a publication whose readers keep up never
    /// reaches its deadline no matter how long it runs.
    last_consumer_position: i64,
    time_of_last_consumer_position_change_ns: i64,
    /// How many clients hold a link to this publication.
    refcount: i32,
    /// When the state last changed (`managed_resource.time_of_last_state_change_ns`).
    time_of_last_state_change_ns: i64,
    state: State,
    /// Whether this publication is refusing its readers
    /// (`aeron_ipc_publication.h:71`). Unlike every other field here it is not
    /// a state: a rejected publication stays `Active` throughout, and the flag
    /// is what keeps subscribers off it until the cool down runs out.
    in_cool_down: bool,
    /// When that ends (`aeron_ipc_publication.h:72`).
    cool_down_expire_time_ns: i64,
    has_reached_end_of_life: bool,
}

/// Why a second publication on the same channel may not share the first.
///
/// One variant per parameter `aeron_confirm_publication_match` compares, so
/// that the error a client is sent names the field rather than the channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShareMismatch {
    /// The URI named a session the existing publication does not have.
    SessionId {
        /// The publication's.
        existing: i32,
        /// What the URI asked for.
        requested: i32,
    },
    /// The URI named a different MTU.
    Mtu {
        /// The publication's.
        existing: i32,
        /// What the URI asked for.
        requested: i32,
    },
    /// The URI named a different term length.
    TermLength {
        /// The publication's.
        existing: i32,
        /// What the URI asked for.
        requested: i32,
    },
    /// The URI named a position in a different stream.
    InitialTermId {
        /// The publication's.
        existing: i32,
        /// What the URI asked for.
        requested: i32,
    },
    /// The URI named a different term to resume in.
    TermId {
        /// The publication's.
        existing: i32,
        /// What the URI asked for.
        requested: i32,
    },
    /// And a different offset inside it.
    TermOffset {
        /// The publication's.
        existing: i64,
        /// What the URI asked for.
        requested: i64,
    },
}

impl std::fmt::Display for ShareMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionId {
                existing,
                requested,
            } => write!(
                f,
                "existing publication has different session-id: existing={existing} requested={requested}"
            ),
            Self::Mtu {
                existing,
                requested,
            } => write!(
                f,
                "existing publication has different mtu: existing={existing} requested={requested}"
            ),
            Self::TermLength {
                existing,
                requested,
            } => write!(
                f,
                "existing publication has different term-length: existing={existing} requested={requested}"
            ),
            Self::InitialTermId {
                existing,
                requested,
            } => write!(
                f,
                "existing publication has different init-term-id: existing={existing} requested={requested}"
            ),
            Self::TermId {
                existing,
                requested,
            } => write!(
                f,
                "existing publication has different term-id: existing={existing} requested={requested}"
            ),
            Self::TermOffset {
                existing,
                requested,
            } => write!(
                f,
                "existing publication has different term-offset: existing={existing} requested={requested}"
            ),
        }
    }
}

/// What the driver knows about a publication before its log buffer is mapped.
///
/// The parts of an `ADD_PUBLICATION` that are not channel parameters: who asked,
/// what to call the result, and whether it may be shared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationIdentity {
    /// The id the log file is named after, and what every image built on it
    /// reports as its publication.
    pub registration_id: i64,
    /// The client that owns it — the counter's `owner_id`, and whose death
    /// takes the publication with it.
    pub client_id: i64,
    /// The session the stream runs under.
    pub session_id: i32,
    /// The stream id.
    pub stream_id: i32,
    /// The channel as the client wrote it, which the position counters quote
    /// back in their keys and labels.
    pub channel: Vec<u8>,
    /// Whether this publication is exclusive: one producer, no sharing, and a
    /// different `pub-pos` label and log type.
    pub is_exclusive: bool,
}

impl IpcPublication {
    /// Take ownership of a freshly mapped log buffer and make it a publication.
    ///
    /// The metadata block is initialised here, after the mapping exists and
    /// before anything can read it — the reference does the same, one function
    /// later than the create (`aeron_ipc_publication.c:107-142`) — and the term
    /// tails are written *before* the metadata, because that is the order the
    /// reference's caller uses and nothing in the tails needs the metadata.
    ///
    /// Every value written comes from `params`, which is the URI resolved
    /// against the driver's configuration (`crate::publication_params`), and
    /// from `socket_buffers`, which is what the kernel reports: two of the six
    /// socket-buffer fields are the machine's own receive and send buffer
    /// sizes, and the other four are zeroes on both sides of the comparison
    /// (`aeron_ipc_publication.c:118-124`).
    #[allow(clippy::too_many_arguments)] // one per source of a written field
    pub fn create(
        log: Box<LogFile>,
        identity: PublicationIdentity,
        params: &PublicationParams,
        page_size: i32,
        socket_buffers: SocketBufferLengths,
        pub_pos_counter_id: i32,
        pub_lmt_counter_id: i32,
        liveness_timeout_ns: i64,
        unblock_timeout_ns: i64,
    ) -> Result<Self, Box<LogFile>> {
        // The term length is the caller's, not the metadata's: this function is
        // what writes the metadata, so reading it here would read zero.
        //
        // Every refusal below hands the mapping back rather than dropping it:
        // a log buffer this function could not make a publication of is a
        // *file* the caller has to remove, and dropping the mapping would
        // leave it behind (`aeron_driver_conductor.c:3913-3916` frees the
        // counters, and the command's own free removes the file).
        let Some(bits_to_shift) = position::bits_to_shift(params.term_length) else {
            return Err(log);
        };

        // A stream that resumes starts its tails at the term the URI named;
        // one that does not starts at the initial term id.
        let start_term_id = params
            .starting_position
            .map_or(params.initial_term_id, |position| position.term_id);
        #[allow(clippy::cast_possible_truncation)] // bounded by a term length
        let start = params
            .starting_position
            .map(|position| (position.term_id, position.term_offset as i32));

        if !log.initialise_tails(params.initial_term_id, start) {
            return Err(log);
        }

        {
            let Some(metadata) = log.metadata() else {
                return Err(log);
            };
            let init = descriptor::LogMetadataInit {
                end_of_stream_position: i64::MAX,
                is_connected: 0,
                active_transport_count: 0,
                correlation_id: identity.registration_id,
                initial_term_id: params.initial_term_id,
                mtu_length: params.mtu_length,
                term_length: params.term_length,
                page_size,
                publication_window_length: params.publication_window_length,
                // The receiver window is an image's field: an IPC publication
                // has no flow control to advertise.
                receiver_window_length: 0,
                // Four of the six socket-buffer fields are zero, on both
                // sides: the two a channel could configure are literal zeros
                // on the reference's IPC path (`:119`, `:122`), and the two
                // `os_max` fields are zero because the reference never writes
                // them anywhere — its context is allocated zeroed and nothing
                // in the driver tree assigns them. Only the two defaults are
                // the kernel's answer (`:120`, `:123`).
                socket_sndbuf_length: 0,
                os_default_socket_sndbuf_length: socket_buffers.sndbuf,
                os_max_socket_sndbuf_length: 0,
                socket_rcvbuf_length: 0,
                os_default_socket_rcvbuf_length: socket_buffers.rcvbuf,
                os_max_socket_rcvbuf_length: 0,
                max_resend: params.max_resend,
                session_id: identity.session_id,
                stream_id: identity.stream_id,
                entity_tag: params.entity_tag,
                response_correlation_id: params.response_correlation_id,
                linger_timeout_ns: params.linger_timeout_ns,
                untethered_window_limit_timeout_ns: params.untethered_window_limit_timeout_ns,
                untethered_linger_timeout_ns: params.untethered_linger_timeout_ns,
                untethered_resting_timeout_ns: params.untethered_resting_timeout_ns,
                group: 0,
                is_response: false,
                rejoin: false,
                reliable: false,
                sparse: params.is_sparse,
                signal_eos: params.signal_eos,
                spies_simulate_connection: params.spies_simulate_connection,
                tether: false,
                is_exclusive: identity.is_exclusive,
            };

            if descriptor::initialise(&metadata, &init).is_none() {
                return Err(log);
            }
        }

        // Nothing has been written, so the producer, the consumer and cleanup
        // all start where the tails say the stream is — which for a resumed
        // stream is not zero: the reference takes all three from the producer
        // position over the raw tail, starting offset included
        // (`aeron_ipc_publication.c:182-184`).
        #[allow(clippy::cast_possible_truncation)] // bounded by a term length
        let starting_term_offset = params
            .starting_position
            .map_or(0, |position| position.term_offset as i32);
        let start_position = Position::new(
            start_term_id,
            starting_term_offset,
            bits_to_shift,
            params.initial_term_id,
        )
        .raw();

        Ok(Self {
            registration_id: identity.registration_id,
            client_id: identity.client_id,
            session_id: identity.session_id,
            stream_id: identity.stream_id,
            initial_term_id: params.initial_term_id,
            term_length: params.term_length,
            mtu_length: params.mtu_length,
            bits_to_shift,
            starting_term_id: start_term_id,
            starting_term_offset: i64::from(starting_term_offset),
            channel: identity.channel,
            is_exclusive: identity.is_exclusive,
            response_correlation_id: params.response_correlation_id,
            log,
            pub_pos_counter_id,
            pub_lmt_counter_id,
            subscribers: Subscribable::new(identity.registration_id),
            term_window_length: params.publication_window_length,
            linger_timeout_ns: params.linger_timeout_ns,
            untethered_window_limit_timeout_ns: params.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: params.untethered_linger_timeout_ns,
            untethered_resting_timeout_ns: params.untethered_resting_timeout_ns,
            liveness_timeout_ns,
            trip_gain: params.publication_window_length / 8,
            trip_limit: 0,
            unblock_timeout_ns,
            consumer_position: start_position,
            clean_position: start_position,
            // The reference seeds both from the consumer position it starts at
            // (`aeron_ipc_publication.c:183-184`).
            last_consumer_position: start_position,
            time_of_last_consumer_position_change_ns: 0,
            refcount: 0,
            time_of_last_state_change_ns: 0,
            state: State::Active,
            in_cool_down: false,
            cool_down_expire_time_ns: 0,
            has_reached_end_of_life: false,
        })
    }

    /// Whether a publication this driver already has can serve `params`, and
    /// why not when it cannot (`aeron_confirm_publication_match`,
    /// `aeron_driver_conductor.c:1109-1177`).
    ///
    /// Only what the URI **named** is compared: a second client that says
    /// nothing about the MTU is served the existing publication whatever its
    /// MTU is, and one that names a different MTU is refused rather than given
    /// a publication whose log buffer disagrees with it.
    pub fn can_be_shared_with(&self, params: &PublicationParams) -> Result<(), ShareMismatch> {
        if let Some(requested) = params.session_id {
            if requested != self.session_id {
                return Err(ShareMismatch::SessionId {
                    existing: self.session_id,
                    requested,
                });
            }
        }

        if params.mtu_length_named && params.mtu_length != self.mtu_length {
            return Err(ShareMismatch::Mtu {
                existing: self.mtu_length,
                requested: params.mtu_length,
            });
        }

        if params.term_length_named && params.term_length != self.term_length {
            return Err(ShareMismatch::TermLength {
                existing: self.term_length,
                requested: params.term_length,
            });
        }

        if let Some(position) = params.starting_position {
            if position.initial_term_id != self.initial_term_id {
                return Err(ShareMismatch::InitialTermId {
                    existing: self.initial_term_id,
                    requested: position.initial_term_id,
                });
            }
            if position.term_id != self.starting_term_id {
                return Err(ShareMismatch::TermId {
                    existing: self.starting_term_id,
                    requested: position.term_id,
                });
            }
            if position.term_offset != self.starting_term_offset {
                return Err(ShareMismatch::TermOffset {
                    existing: self.starting_term_offset,
                    requested: position.term_offset,
                });
            }
        }

        Ok(())
    }

    /// The log buffer's path, as the bytes a reply carries.
    ///
    /// Borrowed rather than copied: `ON_PUBLICATION_READY` is the path the
    /// driver formed, byte for byte, and it is sent inside the same pass that
    /// knows the publication.
    pub fn path_bytes(&self) -> &[u8] {
        self.log.path().as_os_str().as_encoded_bytes()
    }

    /// Where the stream ends, as the log records it. `i64::MAX` until it ends.
    ///
    /// This is what a subscriber's image reads to learn the stream is over —
    /// the driver does not send it a message for that, which is why a
    /// publication going away quietly still ends cleanly for its readers
    /// (`aeron_ipc_publication_handle_managed_resource_event`, the DECREF at
    /// zero).
    pub fn end_of_stream_position(&self) -> Option<i64> {
        self.log
            .metadata()?
            .load_i64_acquire(descriptor::END_OF_STREAM_POSITION_OFFSET)
    }

    /// Write it.
    pub fn set_end_of_stream(&self, position: i64) -> bool {
        self.log
            .metadata()
            .and_then(|metadata| {
                metadata.store_i64_release(descriptor::END_OF_STREAM_POSITION_OFFSET, position)
            })
            .is_some()
    }

    /// Whether the publication was revoked: the byte `REMOVE_PUBLICATION`'s
    /// revoke flag sets (`aeron_ipc_publication_handle_managed_resource_event`,
    /// the REVOKE case).
    /// The load is a plain one, and the store below a relaxed one, because the
    /// only reader of this byte is the conductor that writes it: a client reads
    /// `end_of_stream_position` and `is_connected`, never this. The reference's
    /// `AERON_GET_ACQUIRE`/`AERON_SET_RELEASE` are for the same reason a
    /// volatile read and write — the field is shared memory.
    pub fn is_revoked(&self) -> bool {
        self.log
            .metadata()
            .and_then(|metadata| metadata.load_u8(descriptor::IS_PUBLICATION_REVOKED_OFFSET))
            .unwrap_or(0)
            != 0
    }

    /// Set the revoked byte.
    pub fn set_revoked(&self) {
        if let Some(metadata) = self.log.metadata() {
            let _ = metadata.store_u8_relaxed(descriptor::IS_PUBLICATION_REVOKED_OFFSET, 1);
        }
    }

    /// One link fewer, and what the last one does
    /// (`aeron_ipc_publication_handle_managed_resource_event`).
    ///
    /// Three things, in the reference's order, and each is the last chance to
    /// do it:
    ///
    /// 1. the limit is pulled back to where the producer actually got to, so a
    ///    client still holding the counter is not told it may write past the
    ///    end of a stream;
    /// 2. the log's end-of-stream position is written **there**, which is how
    ///    every reader learns the stream is over;
    /// 3. and unless it was revoked — which cuts the stream off the same way
    ///    but with a message to the readers — the publication starts draining.
    ///
    /// Returns whether that was the last link.
    pub fn release(&mut self, counters: &mut CounterManager, regions: &CounterRegions<'_>) -> bool {
        if !self.decref() {
            return false;
        }

        let producer_position = self.publisher_position().unwrap_or(0);

        if counters
            .value(regions, self.pub_lmt_counter_id)
            .is_some_and(|limit| limit > producer_position)
        {
            counters.set_value(regions, self.pub_lmt_counter_id, producer_position);
        }

        self.set_end_of_stream(producer_position);

        if !self.is_revoked() {
            self.state = State::Draining;
        }

        true
    }

    /// Cut the stream off where the producer got to
    /// (`aeron_ipc_publication_on_time_event`'s revoked branch).
    ///
    /// The readers are told by the caller — one `ON_UNAVAILABLE_IMAGE` each —
    /// because that is a conductor's job, not a publication's.
    ///
    /// Returns whether it was still active: a publication that has already been
    /// revoked, or is already draining, is left alone.
    pub fn revoke(&mut self, counters: &mut CounterManager, regions: &CounterRegions<'_>) -> bool {
        if self.state != State::Active {
            return false;
        }

        let revoked_position = self.publisher_position().unwrap_or(0);

        counters.set_value(regions, self.pub_lmt_counter_id, revoked_position);
        self.set_end_of_stream(revoked_position);

        if let Some(metadata) = self.log.metadata() {
            let _ = metadata.store_i32_release(descriptor::IS_CONNECTED_OFFSET, 0);
        }

        self.state = State::Linger;

        true
    }

    /// Whether a rejection arriving now would cut the readers off, or only
    /// push the deadline out (`aeron_ipc_publication.c:252`, the
    /// `if (!publication->in_cool_down)` that guards all of the work).
    ///
    /// A rejected-but-not-yet-cooled publication is left exactly as it is: a
    /// second rejection inside the window is a client repeating itself, and the
    /// reference answers it by extending the silence rather than by unlinking
    /// readers that are already unlinked.
    pub const fn is_in_cool_down(&self) -> bool {
        self.in_cool_down
    }

    /// Refuse readers until `now_ns + liveness_timeout_ns`
    /// (`aeron_ipc_publication.c:273-276`).
    pub fn enter_cool_down(&mut self, now_ns: i64) {
        self.in_cool_down = true;
        self.cool_down_expire_time_ns = now_ns + self.liveness_timeout_ns;
    }

    /// End the cool down if it has run out, and say whether it did
    /// (`aeron_ipc_publication_check_cooldown_status`,
    /// `aeron_ipc_publication.c:468-479`).
    ///
    /// The caller links the subscriptions again — that is a conductor's act,
    /// and this publication cannot reach them. The comparison is the
    /// reference's strict `<`: a deadline of exactly now is not yet past.
    pub fn cool_down_has_expired(&mut self, now_ns: i64) -> bool {
        if !self.in_cool_down || self.cool_down_expire_time_ns >= now_ns {
            return false;
        }

        self.in_cool_down = false;
        self.cool_down_expire_time_ns = 0;

        true
    }

    /// Close the log's `is_connected` byte, the first thing a rejection does
    /// (`aeron_ipc_publication.c:254`).
    ///
    /// Written rather than recomputed from the readers, which is what
    /// [`Self::update_connected_status`] does and what the untethered machine
    /// needs: here the answer is already known — nobody is connected to a
    /// publication that has just refused them — and the reference stores it
    /// rather than deriving it.
    pub fn mark_disconnected(&self) {
        if let Some(metadata) = self.log.metadata() {
            let _ = metadata.store_i32_release(descriptor::IS_CONNECTED_OFFSET, 0);
        }
    }

    /// Give every reader's counter back and empty the set
    /// (`aeron_ipc_publication.c:258-271`).
    ///
    /// The counters belong to the publication's set and not to the subscription
    /// links that point at them, which is why the links are unlinked first and
    /// without freeing: the reference's `unlink_subscribable` drops the link's
    /// entries and this frees what they named.
    pub fn clear_subscribers(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) {
        for reader in self.subscribers.positions() {
            counters.free(regions, reader.counter_id, now_ms);
        }

        self.subscribers.clear();
    }

    /// Whether the readers have fallen behind far enough that the log may be
    /// blocked (`aeron_ipc_publication_is_possibly_blocked`,
    /// `aeron_ipc_publication.h:146-159`).
    ///
    /// Deliberately not an answer to "is a claim stalled in there": a consumer
    /// behind the producer, or a consumer whose expected term is not the active
    /// one, is enough. It gates the expensive question rather than answering
    /// it — [`unblocker::unblock`] is what walks the term.
    pub fn is_possibly_blocked(&self, producer_position: i64, consumer_position: i64) -> bool {
        let Some(term_count) = self
            .log
            .metadata()
            .and_then(|metadata| metadata.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET))
        else {
            return false;
        };

        #[allow(clippy::cast_possible_truncation)] // the reference truncates here too
        let expected = (consumer_position >> self.bits_to_shift) as i32;

        term_count != expected || producer_position > consumer_position
    }

    /// Unblock the log for a publisher that has gone quiet
    /// (`aeron_ipc_publication_check_for_blocked_publisher`, `:650-672`).
    ///
    /// Returns whether the log was unblocked.
    ///
    /// The deadline runs from the last time the **consumer position moved**,
    /// and it is refreshed on every pass where either the position moved or
    /// the log was not blocked — so a publication whose readers keep up resets
    /// its own deadline continuously and never reaches one.
    pub fn check_for_blocked_publisher(
        &mut self,
        producer_position: i64,
        now_ns: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        if self.consumer_position == self.last_consumer_position
            && self.is_possibly_blocked(producer_position, self.consumer_position)
        {
            if now_ns > self.time_of_last_consumer_position_change_ns + self.unblock_timeout_ns
                && self.unblock_at(self.consumer_position, counters, regions)
            {
                return true;
            }
        } else {
            self.time_of_last_consumer_position_change_ns = now_ns;
            self.last_consumer_position = self.consumer_position;
        }

        false
    }

    /// Run the unblocker at `position`, and count it if it did anything
    /// (`aeron_logbuffer_unblocker.c:19-66` through either of its two callers
    /// here).
    ///
    /// The count goes to the **system** counter 19: the reference takes that one
    /// address at creation (`aeron_ipc_publication.c:186-188`), so every
    /// publication in the process increments the same counter.
    fn unblock_at(
        &self,
        position: i64,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        if !unblocker::unblock(&self.log, position, self.term_length) {
            return false;
        }

        crate::system_counters::increment(
            counters,
            regions,
            crate::system_counters::id::UNBLOCKED_PUBLICATIONS,
        );

        true
    }

    /// The timeout tier's turn for this publication
    /// (`aeron_ipc_publication_on_time_event`, `:481-585`).
    ///
    /// The active arm's other duties — the untethered subscription sweep, the
    /// connected byte and the cool-down — run in [`IpcPublications::on_time_event`]
    /// immediately before this call, on the same tier, because they need the
    /// subscription links and a publication cannot reach them.
    ///
    /// Returns whether anything happened.
    pub fn on_time_event(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> bool {
        match self.state {
            State::Active => {
                let producer_position = self.publisher_position().unwrap_or(0);

                // An exclusive publication has one producer, which is
                // responsible for its own log — the reference skips the check
                // for it (`:543`).
                if self.is_exclusive {
                    return false;
                }

                self.check_for_blocked_publisher(producer_position, now_ns, counters, regions)
            }
            State::Draining => {
                let producer_position = self.publisher_position().unwrap_or(0);
                counters.set_value(regions, self.pub_pos_counter_id, producer_position);

                if !self.is_drained(counters, regions) {
                    // Not drained, so a reader is stuck — and the publication is
                    // draining, which means the last client has let go and will
                    // never commit whatever is in the way. **No timeout gate and
                    // no `is_exclusive` test** (`aeron_ipc_publication.c:579-585`):
                    // being here is the evidence. It is also why this arm
                    // retries on every time event rather than once — an unblock
                    // advances the log, so the next attempt is at a different
                    // position.
                    return self.unblock_at(self.consumer_position, counters, regions);
                }

                self.state = State::Linger;
                self.time_of_last_state_change_ns = now_ns;

                true
            }
            State::Linger => {
                // The reference's whole rule for this state: a publication with
                // nobody holding it is done. The linger timeout in the metadata
                // is for readers, not for this.
                if self.refcount <= 0 {
                    self.has_reached_end_of_life = true;
                }

                true
            }
        }
    }

    /// When the state last changed, for a report.
    pub const fn time_of_last_state_change_ns(&self) -> i64 {
        self.time_of_last_state_change_ns
    }

    /// One more client holds a link to this publication
    /// (`AERON_DRIVER_MANAGED_RESOURCE_INCREF`,
    /// `aeron-driver/src/main/c/aeron_driver_common.h:58`).
    pub fn incref(&mut self) {
        self.refcount += 1;
    }

    /// One fewer. `true` when that was the last link, which is when the
    /// reference closes the publication (`aeron_ipc_publication.c:196-214`).
    pub fn decref(&mut self) -> bool {
        self.refcount -= 1;
        0 == self.refcount
    }

    /// Where the publication is in its life.
    pub const fn state(&self) -> State {
        self.state
    }

    /// Whether it has been removed and is waiting to be collected.
    pub const fn has_reached_end_of_life(&self) -> bool {
        self.has_reached_end_of_life
    }

    /// How many clients hold it.
    pub const fn refcount(&self) -> i32 {
        self.refcount
    }

    /// How far the producer has written, read from the log's own tail
    /// (`aeron_ipc_publication.h:162-173`).
    pub fn publisher_position(&self) -> Option<i64> {
        let metadata = self.log.metadata()?;
        let term_count = metadata.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET)?;
        let index = position::index_by_term_count(term_count);
        let offset =
            descriptor::TERM_TAIL_COUNTERS_OFFSET + index * descriptor::TERM_TAIL_COUNTER_STRIDE;
        let tail = RawTail::from_raw(metadata.load_i64_acquire(offset)?);

        Some(
            Position::new(
                tail.term_id(),
                tail.term_offset(self.term_length),
                self.bits_to_shift,
                self.initial_term_id,
            )
            .raw(),
        )
    }

    /// Attach a subscriber that has already been given a counter.
    ///
    /// The counter is allocated and seeded by the caller — the conductor, which
    /// also owns the position the subscriber joins at — because the reference
    /// allocates it in `link_subscribable` and only then calls this
    /// (`aeron_driver_conductor.c:3547-3617`).
    pub fn add_subscriber(&mut self, position: TetherablePosition) -> bool {
        let Some(metadata) = self.log.metadata() else {
            return false;
        };

        self.subscribers
            .add_position(position, &mut ConnectionHook { metadata });

        true
    }

    /// Detach the subscriber reading through `counter_id`.
    pub fn remove_subscriber(&mut self, counter_id: i32) -> Option<TetherablePosition> {
        let metadata = self.log.metadata()?;

        self.subscribers
            .remove_position(counter_id, &mut ConnectionHook { metadata })
    }

    /// Write the log's `is_connected` byte from what the readers are doing
    /// (`aeron_ipc_publication.c:533-534`, where the reference recomputes it on
    /// every ACTIVE tier).
    ///
    /// Our build otherwise keeps that byte through the add and remove hooks,
    /// which is enough for a reader that *leaves* — but not for one that stops
    /// being a working reader while staying in the set. The untethered machine
    /// is what produces those: a resting reader is still there and still
    /// counted, and a producer reading this byte is asking whether anyone is
    /// reading at all.
    pub fn update_connected_status(&self) {
        let Some(metadata) = self.log.metadata() else {
            return;
        };

        let connected = i32::from(self.subscribers.has_working_positions());
        let _ = metadata.store_i32_release(descriptor::IS_CONNECTED_OFFSET, connected);
    }

    /// Move a subscriber's position to another tether state.
    pub fn set_subscriber_state(
        &mut self,
        counter_id: i32,
        state: crate::subscribable::TetherState,
        now_ns: i64,
    ) -> Option<crate::subscribable::StateChange> {
        self.subscribers.set_state(counter_id, state, now_ns)
    }

    /// Where a subscriber joining right now should start reading
    /// (`aeron_ipc_publication_join_position`, `aeron_ipc_publication.h:176-197`).
    ///
    /// The earliest position any current reader still needs, or the
    /// publication's own consumer position when there is none. Not the
    /// producer's position: a reader that joined at the tail could not be
    /// served a stream whose earliest unread byte is a term behind, and the
    /// limit holds the producer back to the oldest reader anyway — so joining
    /// at the oldest reader is joining where the stream actually is.
    pub fn join_position(&self, manager: &CounterManager, regions: &CounterRegions<'_>) -> i64 {
        self.subscribers
            .min_active_position(manager, regions)
            .map_or(self.consumer_position, |position| {
                position.min(self.consumer_position)
            })
    }

    /// The untethered subscriptions' state machine, for a publication's own
    /// readers (`aeron_ipc_publication_check_untethered_subscriptions`,
    /// `aeron-driver/src/main/c/aeron_ipc_publication.c:354-470`).
    ///
    /// The same three states as an image's readers, judged against the same
    /// shape of limit, and differing in exactly three things — which is the
    /// whole of what a reader comparing the two C functions has to hold in
    /// mind:
    ///
    /// * the limit is measured against [`Self::consumer_position`], the
    ///   **fastest** active reader (the reference's `:316`), where an image
    ///   takes the maximum of its own readers' positions;
    /// * the window is the publication's own [`Self::term_window_length`] —
    ///   the `pub-wnd` the channel named — where an image uses the receiver
    ///   window it advertises in its status messages;
    /// * a woken reader starts at [`Self::join_position`], the slowest active
    ///   reader and no later than `consumer_position`.
    ///
    /// A tethered reader is never put aside. Neither is one that is merely
    /// slow: the limit moves with the fastest reader, so a publication whose
    /// only reader has stopped has nothing for it to be behind — which is why
    /// the reference can offer this as an escape valve at all.
    ///
    /// Returns what the conductor has to say about each reader it moved, in
    /// the order the readers are held.
    pub fn check_untethered_subscriptions(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> Vec<UntetheredEvent> {
        let mut events = Vec::new();

        let window_length = i64::from(self.term_window_length);
        let untethered_window_limit =
            (self.consumer_position - window_length) + (window_length / 4);

        // Copied out for the same reason the image's copy is: a woken reader
        // reads the whole set again for its join position, and the set cannot
        // be read while it is being written. The reference walks the array it
        // is writing into, which is the same thing with the aliasing left
        // implicit.
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
                        let join_position = self.join_position(counters, regions);

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

    /// Write `pub-pos` and recompute `pub-lmt`
    /// (`aeron_ipc_publication.c:278-328`).
    ///
    /// Returns whether anything moved, which is the reference's `work_count`
    /// contribution to the conductor's pass.
    ///
    /// The rule, in the reference's order:
    ///
    /// - with readers, the limit is the slowest active reader's position plus
    ///   one term window — and it is only *written* when it passes `trip_limit`,
    ///   which is what stops the counter being rewritten every pass;
    /// - with no readers, the limit can only fall, to where the slowest reader
    ///   got to before it left. A publication with no subscribers therefore
    ///   reports back-pressure to its producer, which is exactly what "nobody
    ///   is reading" should mean.
    pub fn update_pub_pos_and_lmt(
        &mut self,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        if State::Active != self.state {
            return false;
        }

        let Some(producer_position) = self.publisher_position() else {
            return false;
        };

        if manager
            .set_value(regions, self.pub_pos_counter_id, producer_position)
            .is_none()
        {
            return false;
        }

        let consumer_position = self.consumer_position;

        if self.subscribers.has_working_positions() {
            // Both ends in one pass, as the reference takes them
            // (`aeron_ipc_publication.c:288-302` folds `min_sub_pos` and
            // `max_sub_pos` in the same loop). This runs every cycle for every
            // IPC publication, so the second walk the two single-ended helpers
            // would make is paid once per reader per cycle for nothing.
            let Some((min_sub_pos, max_sub_pos)) =
                self.subscribers.active_position_range(manager, regions)
            else {
                return false;
            };

            // The reference seeds the running maximum with the consumer position
            // before it takes a single reading (`aeron_ipc_publication.c:292`):
            // the position is monotonic, and a reader that comes back below it —
            // a rejoining subscription still holding its join position — must
            // not drag the limit arithmetic backwards with it.
            let max_sub_pos = max_sub_pos.max(consumer_position);

            let new_limit = min_sub_pos + i64::from(self.term_window_length);
            let mut worked = false;

            if new_limit > self.trip_limit {
                self.clean_buffer(min_sub_pos);
                manager.set_value(regions, self.pub_lmt_counter_id, new_limit);
                self.trip_limit = new_limit + i64::from(self.trip_gain);
                worked = true;
            }

            self.consumer_position = max_sub_pos;

            worked
        } else if manager
            .value(regions, self.pub_lmt_counter_id)
            .is_some_and(|limit| limit > consumer_position)
        {
            manager.set_value(regions, self.pub_lmt_counter_id, consumer_position);
            self.trip_limit = consumer_position;
            self.clean_buffer(consumer_position);

            true
        } else {
            false
        }
    }

    /// Zero the terms the readers have finished with, a chunk at a time
    /// (`aeron_ipc_publication.c:328-350`).
    ///
    /// Everything past the first eight bytes is zeroed first, and the
    /// frame-length word goes to zero last with a **release**. A reader that
    /// sees a zero length stops; a reader that already saw the old length finds
    /// the bytes it describes still untouched. Zeroing the length first would
    /// let a reader see a record whose body had been cleared underneath it.
    pub fn clean_buffer(&mut self, position: i64) {
        if position <= self.clean_position {
            return;
        }

        let term_length = self.term_length as i64;
        let dirty_index = self.term_index(self.clean_position);
        let clean_offset = (self.clean_position & (term_length - 1)) as usize;
        let bytes_left_in_term = term_length as usize - clean_offset;
        let bytes_to_clean = (position - self.clean_position) as usize;
        let length = bytes_to_clean.min(bytes_left_in_term);

        let Some(term) = self.log.term(dirty_index) else {
            return;
        };

        let body = length.saturating_sub(size_of::<i64>());
        if term.zero(clean_offset + size_of::<i64>(), body).is_none() {
            return;
        }

        if term.store_i64_release(clean_offset, 0).is_none() {
            return;
        }

        self.clean_position += length as i64;
    }

    /// Whether every active reader has read everything written
    /// (`aeron_ipc_publication.h:202-222`).
    ///
    /// A draining publication is readable until this is true, and the reference
    /// keeps accepting subscribers until then too
    /// (`is_accepting_subscriptions`).
    pub fn is_drained(&self, manager: &CounterManager, regions: &CounterRegions<'_>) -> bool {
        let Some(producer_position) = self.publisher_position() else {
            return true;
        };

        self.subscribers
            .positions()
            .iter()
            .filter(|position| position.state.is_active())
            .all(|position| {
                manager
                    .value(regions, position.counter_id)
                    .is_none_or(|sub_pos| sub_pos >= producer_position)
            })
    }

    /// Whether a subscriber arriving now would be attached
    /// (`aeron_ipc_publication.h:224-230`): an active publication, or a draining
    /// one that still has unread data — and never one that is refusing readers,
    /// whatever state it is in.
    pub fn is_accepting_subscriptions(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        if self.in_cool_down {
            return false;
        }

        match self.state {
            State::Active => true,
            State::Draining => !self.is_drained(manager, regions),
            State::Linger => false,
        }
    }

    /// The term a position falls in.
    fn term_index(&self, position: i64) -> usize {
        Position::from_raw(position).index(self.bits_to_shift)
    }

    /// Give the log buffer back, for the caller to hand to the agent that will
    /// unmap and remove it.
    pub fn into_log(self) -> Box<LogFile> {
        self.log
    }
}

impl std::fmt::Debug for IpcPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpcPublication")
            .field("registration_id", &self.registration_id)
            .field("session_id", &self.session_id)
            .field("stream_id", &self.stream_id)
            .field("log", &self.log)
            .field("subscribers", &self.subscribers.len())
            .field("state", &self.state)
            .finish()
    }
}

/// The publication's own view of the one byte a subscriber changes.
///
/// IPC's two hooks are the whole of what a publication does when a reader comes
/// or goes: the log's `is_connected` byte, which is what a producer reads to
/// tell "a slow reader" from "nobody is reading"
/// (`aeron_ipc_publication.h:130-144`).
struct ConnectionHook<'a> {
    metadata: AtomicBuffer<'a, ReadWrite>,
}

impl SubscribableHooks for ConnectionHook<'_> {
    fn position_added(&mut self, _position: &TetherablePosition) {
        let _ = self
            .metadata
            .store_i32_release(descriptor::IS_CONNECTED_OFFSET, 1);
    }

    fn position_removed(&mut self, _position: &TetherablePosition, working_before: usize) {
        // The reference sets it to zero only when this was the **last** working
        // position — which the hook can see because it runs before the removal.
        if 1 == working_before {
            let _ = self
                .metadata
                .store_i32_release(descriptor::IS_CONNECTED_OFFSET, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::logbuffer::frame;
    use deepmsg_core::logbuffer::logfile::LogFile;

    /// The smallest legal term, so a test's log buffer is 200 KiB.
    const TERM_LENGTH: i32 = descriptor::TERM_MIN_LENGTH;
    const PAGE_SIZE: i32 = 4096;
    const MTU: i32 = descriptor::MTU_LENGTH_DEFAULT;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("deepmsg-publication-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create the directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[repr(align(64))]
    struct Region(Vec<u8>);

    const VALUES_LENGTH: usize = 64 * 1024;

    /// The counter regions, kept apart from the publication so a test can hold
    /// both — a fixture that owned both would borrow itself mutably twice.
    struct Regions {
        metadata: Region,
        values: Region,
    }

    impl Regions {
        fn new() -> Self {
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
            let manager = CounterManager::new(VALUES_LENGTH, 1_000).expect("room");

            (manager, regions)
        }
    }

    /// The log's `is_connected` byte, read fresh — the metadata view borrows
    /// the publication, so a test that held one would have to give it up
    /// before touching the publication again.
    fn connected(publication: &IpcPublication) -> Option<i32> {
        publication
            .log
            .metadata()?
            .load_i32_acquire(descriptor::IS_CONNECTED_OFFSET)
    }

    /// One reader, as the conductor leaves it when it links a subscriber.
    fn reader(counter_id: i32, is_tether: bool, is_rejoin: bool) -> TetherablePosition {
        TetherablePosition {
            counter_id,
            subscription_registration_id: i64::from(counter_id) + 100,
            time_of_last_update_ns: 0,
            state: TetherState::Active,
            is_tether,
            is_rejoin,
        }
    }

    /// A publication with a stalled reader and one that keeps up, and the
    /// positions the machine's limit is measured from.
    ///
    /// `consumer_position` is set by hand rather than by producing: the
    /// machine reads the field the publication maintains (`:316`), and an
    /// untethered test that had to fill a window first would be testing the
    /// limit's *maintenance* instead of its use.
    fn stalled_and_reading(
        dir: &TempDir,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        is_rejoin: bool,
    ) -> (IpcPublication, i32, i32) {
        let mut publication = publication(dir, manager, regions);

        let stalled = manager
            .allocate(regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let reading = manager
            .allocate(regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let _ = manager.set_value(regions, stalled, 0);
        let _ = manager.set_value(regions, reading, 100_000);

        publication.consumer_position = 100_000;

        assert!(publication.add_subscriber(reader(stalled, false, is_rejoin)));
        assert!(publication.add_subscriber(reader(reading, true, is_rejoin)));

        (publication, stalled, reading)
    }

    #[test]
    fn a_reader_that_stops_reading_is_put_aside_and_woken_at_the_join_position() {
        let dir = TempDir::new();
        let mut region_holder = Regions::new();
        let (mut manager, regions) = region_holder.open();
        let (mut publication, stalled, _) = stalled_and_reading(&dir, &mut manager, &regions, true);

        // Behind by more than three quarters of the window, and quiet for
        // longer than the window limit.
        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns + 1,
        );
        assert_eq!(
            vec![UntetheredEvent::Unavailable {
                subscription_registration_id: i64::from(stalled) + 100,
                counter_id: stalled,
            }],
            events,
            "only the reader that is behind is put aside: the other one is tethered"
        );

        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns
                + publication.untethered_linger_timeout_ns
                + 2,
        );
        assert!(
            events.is_empty(),
            "a rejoining reader waits rather than closing"
        );

        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns
                + publication.untethered_linger_timeout_ns
                + publication.untethered_resting_timeout_ns
                + 3,
        );
        assert_eq!(
            vec![UntetheredEvent::Available {
                subscription_registration_id: i64::from(stalled) + 100,
                counter_id: stalled,
                join_position: 100_000,
            }],
            events,
            "woken where the slowest active reader is, which is the one that kept up"
        );
        assert_eq!(
            Some(100_000),
            manager.value(&regions, stalled),
            "and its counter is seeded there, not left where it stalled"
        );
        assert_eq!(
            Some(TetherState::Active),
            publication
                .subscribers
                .find_by_counter(stalled)
                .map(|position| position.state)
        );
    }

    #[test]
    fn a_reader_that_is_not_rejoining_is_closed_and_keeps_its_place_in_the_set() {
        let dir = TempDir::new();
        let mut region_holder = Regions::new();
        let (mut manager, regions) = region_holder.open();
        let (mut publication, stalled, _) =
            stalled_and_reading(&dir, &mut manager, &regions, false);

        let _ = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns + 1,
        );

        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns
                + publication.untethered_linger_timeout_ns
                + 2,
        );
        assert_eq!(
            vec![UntetheredEvent::Closed {
                counter_id: stalled
            }],
            events,
            "a reader that is not rejoining is done"
        );

        let closed = publication
            .subscribers
            .find_by_subscription(i64::from(stalled) + 100);

        assert_eq!(1, closed.len(), "the position is still there");
        assert_eq!(TetherState::Closed, closed[0].state);
        assert_eq!(
            deepmsg_cnc::layout::NULL_COUNTER_ID,
            closed[0].counter_id,
            "and its id is no longer one the set points at, which is what stops \
             the teardown giving the counter back twice"
        );
    }

    #[test]
    fn the_logs_connected_byte_follows_a_reader_that_is_woken() {
        // Waking is the case the add and remove hooks cannot see: nothing is
        // added and nothing is removed, so the log's `is_connected` byte is
        // either the reference's per-tier recomputation (`:533-534`) or it is
        // stale — and it is what a producer reads to tell "nobody is reading"
        // from "a slow reader".
        let dir = TempDir::new();
        let mut region_holder = Regions::new();
        let (mut manager, regions) = region_holder.open();
        let (mut publication, _stalled, reading) =
            stalled_and_reading(&dir, &mut manager, &regions, true);

        let _ = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns + 1,
        );
        let _ = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns
                + publication.untethered_linger_timeout_ns
                + 2,
        );

        // The reader that kept up leaves while the other one waits. That is a
        // removal, so the hook sees it: nobody is reading.
        assert!(publication.remove_subscriber(reading).is_some());
        publication.update_connected_status();
        assert_eq!(
            0,
            metadata_i32(&publication, descriptor::IS_CONNECTED_OFFSET)
        );

        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_window_limit_timeout_ns
                + publication.untethered_linger_timeout_ns
                + publication.untethered_resting_timeout_ns
                + 3,
        );
        assert_eq!(1, events.len(), "the waiting reader is woken");

        publication.update_connected_status();
        assert_eq!(
            1,
            metadata_i32(&publication, descriptor::IS_CONNECTED_OFFSET),
            "and the producer can see someone reading again"
        );
    }

    #[test]
    fn a_tethered_reader_is_never_put_aside_and_neither_is_the_only_one() {
        let dir = TempDir::new();
        let mut region_holder = Regions::new();
        let (mut manager, regions) = region_holder.open();
        let mut publication = publication(&dir, &mut manager, &regions);

        let one = manager
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let two = manager
            .allocate(&regions, 4, &[], b"sub-pos", 1)
            .expect("a counter");
        let _ = manager.set_value(&regions, one, 0);
        let _ = manager.set_value(&regions, two, 0);

        publication.consumer_position = 100_000;

        assert!(publication.add_subscriber(reader(one, true, true)));
        assert!(publication.add_subscriber(reader(two, true, false)));

        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_resting_timeout_ns * 4,
        );
        assert!(
            events.is_empty(),
            "a tethered reader keeps its claim forever"
        );

        // And an untethered one is only late *relative to another reader*: the
        // limit moves with the fastest, so a publication whose only reader has
        // stopped has nothing for it to be behind.
        assert!(publication.remove_subscriber(two).is_some());
        let lone = publication.subscribers.positions()[0].counter_id;
        let _ = manager.set_value(&regions, lone, 40_000);
        publication.consumer_position = 40_000;

        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            publication.untethered_resting_timeout_ns * 4,
        );
        assert!(
            events.is_empty(),
            "the only reader is the fastest reader, however slowly it reads"
        );
    }

    /// The parameters a bare `aeron:ipc` channel resolves to, with this
    /// module's small term length instead of the 64 MiB default.
    fn publication_params() -> PublicationParams {
        PublicationParams {
            term_length: TERM_LENGTH,
            term_length_named: false,
            mtu_length: MTU,
            mtu_length_named: false,
            publication_window_length: TERM_LENGTH / 2,
            max_resend: 0,
            entity_tag: -1,
            response_correlation_id: -1,
            is_response: false,
            session_id: None,
            linger_timeout_ns: 5_000_000_000,
            untethered_window_limit_timeout_ns: 5_000_000_000,
            untethered_linger_timeout_ns: 5_000_000_000,
            untethered_resting_timeout_ns: 10_000_000_000,
            is_sparse: true,
            signal_eos: true,
            spies_simulate_connection: false,
            starting_position: None,
            initial_term_id: 17,
        }
    }

    /// A publication with its two counters allocated **in the caller's
    /// regions**, as the conductor leaves it.
    ///
    /// The regions matter: a publisher's and its subscribers' counters share one
    /// manager, so allocating them in a scratch one would hand the subscribers
    /// the publisher's ids.
    fn publication(
        dir: &TempDir,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
    ) -> IpcPublication {
        let log = Box::new(
            LogFile::create(
                &dir.0.join("pub.logbuffer"),
                TERM_LENGTH,
                PAGE_SIZE as usize,
                false,
            )
            .expect("a log buffer"),
        );

        let identity = PublicationIdentity {
            registration_id: 99,
            client_id: 7,
            session_id: 100,
            stream_id: 1001,
            channel: b"aeron:ipc".to_vec(),
            is_exclusive: false,
        };
        let params = publication_params();
        let socket_buffers = SocketBufferLengths {
            rcvbuf: 212_992,
            sndbuf: 212_992,
        };

        let mut publication = IpcPublication::create(
            log,
            identity,
            &params,
            PAGE_SIZE,
            socket_buffers,
            0,
            0,
            crate::publication_image::IMAGE_LIVENESS_TIMEOUT_NS,
            crate::config::PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT,
        )
        .expect("a publication");

        publication.pub_pos_counter_id = crate::position::allocate_publisher_position(
            manager,
            regions,
            7,
            99,
            100,
            1001,
            b"aeron:ipc",
            false,
            0,
        )
        .expect("a pub-pos counter");
        publication.pub_lmt_counter_id = crate::position::allocate_publisher_limit(
            manager,
            regions,
            7,
            99,
            100,
            1001,
            b"aeron:ipc",
            0,
        )
        .expect("a pub-lmt counter");

        publication
    }

    /// Attach a subscriber whose counter holds `position`, the way
    /// `link_subscribable` would.
    fn subscribe(
        publication: &mut IpcPublication,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        registration_id: i64,
        position: i64,
    ) -> i32 {
        let counter_id = crate::position::allocate_subscription_position(
            manager,
            regions,
            7,
            registration_id,
            100,
            1001,
            b"aeron:ipc",
            position,
            0,
        )
        .expect("a sub-pos counter");
        manager
            .set_value(regions, counter_id, position)
            .expect("in range");

        assert!(publication.add_subscriber(TetherablePosition {
            counter_id,
            subscription_registration_id: registration_id,
            time_of_last_update_ns: 0,
            state: crate::subscribable::TetherState::Active,
            is_tether: true,
            is_rejoin: false,
        }));

        counter_id
    }

    /// A 64-bit field of the log's metadata block.
    fn metadata_i64(publication: &IpcPublication, offset: usize) -> i64 {
        publication
            .log
            .metadata()
            .expect("the metadata block")
            .load_i64_relaxed(offset)
            .expect("in range")
    }

    /// A 32-bit field of the log's metadata block.
    fn metadata_i32(publication: &IpcPublication, offset: usize) -> i32 {
        publication
            .log
            .metadata()
            .expect("the metadata block")
            .load_i32_relaxed(offset)
            .expect("in range")
    }

    /// A field of the default frame header template.
    fn template_i32(publication: &IpcPublication, field: usize) -> i32 {
        metadata_i32(publication, descriptor::DEFAULT_FRAME_HEADER_OFFSET + field)
    }

    #[test]
    fn a_created_publication_writes_the_metadata_the_reference_writes() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let publication = publication(&dir, &mut manager, &region_pair);

        assert_eq!(
            i64::MAX,
            metadata_i64(&publication, descriptor::END_OF_STREAM_POSITION_OFFSET),
            "end of stream is MAX until the stream ends"
        );
        assert_eq!(
            0,
            metadata_i32(&publication, descriptor::IS_CONNECTED_OFFSET)
        );
        assert_eq!(
            0,
            metadata_i32(&publication, descriptor::RECEIVER_WINDOW_LENGTH_OFFSET),
            "nobody receives on the other side of an IPC channel"
        );
        assert_eq!(
            TERM_LENGTH,
            metadata_i32(&publication, descriptor::TERM_LENGTH_OFFSET)
        );
        assert_eq!(
            MTU,
            metadata_i32(&publication, descriptor::MTU_LENGTH_OFFSET)
        );
        assert_eq!(
            TERM_LENGTH / 2,
            metadata_i32(&publication, descriptor::PUBLICATION_WINDOW_LENGTH_OFFSET),
            "half a term by default"
        );
        assert_eq!(
            Some(0),
            publication
                .log
                .metadata()
                .expect("the block")
                .load_u8(descriptor::TYPE_OFFSET),
            "a concurrent publication"
        );
        assert_eq!(
            frame::DATA_HEADER_LENGTH as i32,
            metadata_i32(&publication, descriptor::DEFAULT_FRAME_HEADER_LENGTH_OFFSET)
        );

        let metadata = publication.log.metadata().expect("the block");
        assert_eq!(
            Some(99),
            metadata.load_i64_relaxed(descriptor::CORRELATION_ID_OFFSET),
            "the publication's registration id names the log"
        );
        // The untethered linger timeout is -1 in the configuration and 5 s in
        // the file: an unset value means "the window limit timeout", and that
        // substitution happens as the parameters are resolved
        // (`aeron-driver/src/main/c/uri/aeron_driver_uri.c:414-427`).
        assert_eq!(
            Some(5_000_000_000),
            metadata.load_i64_unaligned(descriptor::UNTETHERED_LINGER_TIMEOUT_NS_OFFSET),
            "an unset linger timeout falls back to the window limit timeout"
        );
        assert_eq!(
            Some(-1),
            metadata.load_i64_relaxed(descriptor::ENTITY_TAG_OFFSET),
            "no entity tag in the URI is the reference's invalid tag"
        );
        assert_eq!(
            Some(-1),
            metadata.load_i64_relaxed(descriptor::RESPONSE_CORRELATION_ID_OFFSET),
            "this is nobody's response channel"
        );
        assert_eq!(
            Some(1),
            metadata.load_u8(descriptor::SPARSE_OFFSET),
            "aeron.term.buffer.sparse.file defaults true, and the byte says so"
        );
        assert_eq!(
            Some(1),
            metadata.load_u8(descriptor::SIGNAL_EOS_OFFSET),
            "eos defaults true"
        );
        assert_eq!(
            Some(0),
            metadata.load_u8(descriptor::SPIES_SIMULATE_CONNECTION_OFFSET)
        );

        // The socket buffer lengths are the kernel's answer, not zeroes: two
        // of the six are what a fresh socket reports, and the other four are
        // about a socket this publication does not have.
        let socket_buffers = SocketBufferLengths {
            rcvbuf: 212_992,
            sndbuf: 212_992,
        };
        assert_eq!(
            Some(i64::from(socket_buffers.rcvbuf)),
            metadata
                .load_i32_relaxed(descriptor::OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET)
                .map(i64::from)
        );
        assert_eq!(
            Some(i64::from(socket_buffers.sndbuf)),
            metadata
                .load_i32_relaxed(descriptor::OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET)
                .map(i64::from)
        );
        assert_eq!(
            Some(0),
            metadata.load_i32_relaxed(descriptor::SOCKET_SNDBUF_LENGTH_OFFSET)
        );
        assert_eq!(
            Some(0),
            metadata.load_i32_relaxed(descriptor::SOCKET_RCVBUF_LENGTH_OFFSET)
        );

        // The session and the stream are in the template header — the metadata
        // block has no fields for them (`aeron_logbuffer_descriptor.h:42-88`).
        assert_eq!(
            100,
            template_i32(&publication, frame::SESSION_ID_FIELD_OFFSET)
        );
        assert_eq!(
            1001,
            template_i32(&publication, frame::STREAM_ID_FIELD_OFFSET)
        );
        assert_eq!(17, template_i32(&publication, frame::TERM_ID_FIELD_OFFSET));
    }

    #[test]
    fn a_resumed_stream_starts_its_consumer_where_the_offset_says() {
        let dir = TempDir::new();
        let log = Box::new(
            LogFile::create(
                &dir.0.join("pub.logbuffer"),
                TERM_LENGTH,
                PAGE_SIZE as usize,
                false,
            )
            .expect("a log buffer"),
        );

        let identity = PublicationIdentity {
            registration_id: 99,
            client_id: 7,
            session_id: 100,
            stream_id: 1001,
            channel: b"aeron:ipc".to_vec(),
            is_exclusive: false,
        };
        let mut params = publication_params();
        params.starting_position = Some(crate::publication_params::StartingPosition {
            initial_term_id: 17,
            term_id: 19,
            term_offset: 8192,
        });

        let publication = IpcPublication::create(
            log,
            identity,
            &params,
            PAGE_SIZE,
            SocketBufferLengths {
                rcvbuf: 212_992,
                sndbuf: 212_992,
            },
            0,
            0,
            crate::publication_image::IMAGE_LIVENESS_TIMEOUT_NS,
            crate::config::PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT,
        )
        .expect("a publication");

        // The consumer, like cleanup, starts where the tails say the stream
        // is — the term the URI named *and* the offset into it — which is the
        // reference's producer position over the freshly initialised tails
        // (`aeron_ipc_publication.c:182-184`). Dropping the offset would put
        // the limit logic a term's head behind a stream that resumed midway.
        let bits = position::bits_to_shift(TERM_LENGTH).expect("a power of two");
        let expected = Position::new(19, 8192, bits, 17).raw();
        assert_eq!(expected, publication.consumer_position);
        assert_eq!(expected, publication.clean_position);
    }

    #[test]
    fn a_producer_with_no_subscribers_cannot_publish() {
        // The heart of IPC backpressure: the limit starts at the position the
        // log begins at, and with nobody reading it can only fall.
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        // The first pass has nothing to correct: the limit already sits at the
        // position the log begins at.
        assert!(!publication.update_pub_pos_and_lmt(&mut manager, &region_pair));

        assert_eq!(
            Some(0),
            manager.value(&region_pair, publication.pub_lmt_counter_id),
            "no readers, no window"
        );
        assert_eq!(
            Some(0),
            manager.value(&region_pair, publication.pub_pos_counter_id)
        );

        // A producer that writes anyway moves `pub-pos`, and the limit still
        // does not follow: with nobody reading it can only be lowered.
        {
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(17, 4096).raw(),
                )
                .expect("in range");
        }
        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);

        assert_eq!(
            Some(4096),
            manager.value(&region_pair, publication.pub_pos_counter_id),
            "the driver reports how far the producer got"
        );
        assert_eq!(
            Some(0),
            manager.value(&region_pair, publication.pub_lmt_counter_id),
            "and still refuses to let it write"
        );
    }

    #[test]
    fn a_subscriber_gives_the_producer_a_window_of_one_term() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);
        subscribe(&mut publication, &mut manager, &region_pair, 100, 0);

        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);

        assert_eq!(
            Some(i64::from(TERM_LENGTH / 2)),
            manager.value(&region_pair, publication.pub_lmt_counter_id),
            "the reader's position plus the term window"
        );
        assert_eq!(
            1,
            metadata_i32(&publication, descriptor::IS_CONNECTED_OFFSET),
            "and the log says somebody is connected"
        );
    }

    #[test]
    fn a_reader_below_the_consumer_position_cannot_drag_it_back() {
        // The running maximum is seeded with the consumer position
        // (`aeron_ipc_publication.c:292`), so a subscription that reports a
        // position the publication has already passed — which is what a
        // rejoining reader holds until it catches up — does not pull the
        // consumer position, and everything derived from it, backwards.
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);
        subscribe(&mut publication, &mut manager, &region_pair, 100, 64);
        publication.consumer_position = 4096;

        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);

        assert_eq!(
            4096, publication.consumer_position,
            "the consumer position is monotonic"
        );
    }

    #[test]
    fn the_limit_follows_the_slowest_reader() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);
        let slow = subscribe(&mut publication, &mut manager, &region_pair, 100, 0);
        let fast = subscribe(&mut publication, &mut manager, &region_pair, 101, 0);
        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);

        // The fast reader moves on; the limit may not, because the slow one has
        // not read past where it was.
        manager
            .set_value(&region_pair, fast, 4096)
            .expect("in range");
        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);
        assert_eq!(
            Some(i64::from(TERM_LENGTH / 2)),
            manager.value(&region_pair, publication.pub_lmt_counter_id),
            "one reader is still at zero"
        );

        // When the slow one moves, the limit is still not written: the value it
        // would take is exactly the trip limit, and the reference writes a new
        // limit only when it *passes* that mark.
        manager
            .set_value(&region_pair, slow, 4096)
            .expect("in range");
        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);
        assert_eq!(
            Some(i64::from(TERM_LENGTH / 2)),
            manager.value(&region_pair, publication.pub_lmt_counter_id),
            "equal to the trip limit is not past it"
        );

        // One byte further and it is — for *both* readers, because the limit
        // follows the slowest one and the fast one is already past the mark.
        manager
            .set_value(&region_pair, fast, 4097)
            .expect("in range");
        manager
            .set_value(&region_pair, slow, 4097)
            .expect("in range");
        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);
        assert_eq!(
            Some(4097 + i64::from(TERM_LENGTH / 2)),
            manager.value(&region_pair, publication.pub_lmt_counter_id)
        );
    }

    #[test]
    fn removing_the_last_subscriber_disconnects_the_log() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);
        let subscriber = subscribe(&mut publication, &mut manager, &region_pair, 100, 0);
        publication.update_pub_pos_and_lmt(&mut manager, &region_pair);
        assert_eq!(
            1,
            metadata_i32(&publication, descriptor::IS_CONNECTED_OFFSET)
        );

        publication
            .remove_subscriber(subscriber)
            .expect("it was attached");

        assert_eq!(
            0,
            metadata_i32(&publication, descriptor::IS_CONNECTED_OFFSET),
            "the last reader leaving closes the window"
        );
    }

    #[test]
    fn cleanup_zeroes_everything_the_readers_have_finished_with() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, 128).expect("in range");
            term.store_i64_release(8, 0x5A5A_5A5A_5A5A_5A5A)
                .expect("in range");
        }

        publication.clean_buffer(4096);

        let term = publication.log.term(0).expect("the first term");
        assert_eq!(Some(0), term.load_i32(0), "the length word is zero");
        assert_eq!(Some(0), term.load_i64(8), "and so is the body");
    }

    #[test]
    fn a_publication_with_readers_on_a_fresh_log_is_still_drained() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);
        subscribe(&mut publication, &mut manager, &region_pair, 100, 0);

        // Nothing has been written, so a reader at zero has read it all.
        assert!(publication.is_drained(&manager, &region_pair));
        assert!(publication.is_accepting_subscriptions(&manager, &region_pair));

        // Move the producer on, and it is no longer drained.
        {
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(17, 128).raw(),
                )
                .expect("in range");
        }

        assert!(!publication.is_drained(&manager, &region_pair));
    }

    /// The active arm's blocked-publisher duty: a reader that has not moved for
    /// longer than the unblock window, with a claim stalled in its way.
    #[test]
    fn a_claim_that_has_blocked_a_quiet_publication_is_padded() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        let stalled = 128i32;
        {
            // The producer claimed 128 bytes and died: the length is negative,
            // and no reader can pass it.
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            // And the tail says it got that far — which is what makes the log
            // *possibly* blocked rather than merely idle.
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(17, stalled).raw(),
                )
                .expect("in range");
        }

        // The window has run out: the reader has been at zero since the epoch.
        publication.unblock_timeout_ns = 1_000;

        assert!(
            publication.on_time_event(&mut manager, &region_pair, 2_000),
            "the deadline passed and the log was blocked, so it was unblocked"
        );

        let term = publication.log.term(0).expect("the first term");
        let frame = frame::Frame::new(&term, 0);
        assert_eq!(Some(stalled), frame.frame_length());
        assert!(frame.is_padding(), "the claim is padding now");
    }

    /// The same unblock with the **system** counter in place, because that is
    /// the observable: counter 19 is one per process, not one per publication
    /// (`aeron_ipc_publication.c:186-188`), so nothing about the publication
    /// shows it was counted.
    #[test]
    fn unblocking_moves_the_system_counter() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();

        // The system counters take ids 0..N, so they go in first — the
        // publication's own counters follow them, as they do in the driver.
        crate::system_counters::allocate_all(
            &mut manager,
            &region_pair,
            0,
            0,
            &crate::system_counters::LabelSuffixes {
                resolver_name: "",
                threading_mode: "SHARED",
                conductor_cycle_threshold_ns: 0,
                sender_cycle_threshold_ns: 0,
                receiver_cycle_threshold_ns: 0,
                name_resolver_threshold_ns: 0,
            },
        )
        .expect("the system counters");

        let mut publication = publication(&dir, &mut manager, &region_pair);
        let unblocked = crate::system_counters::id::UNBLOCKED_PUBLICATIONS;
        assert_eq!(
            Some(0),
            manager.value(&region_pair, unblocked),
            "nothing has been unblocked yet"
        );

        let stalled = 128i32;
        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(17, stalled).raw(),
                )
                .expect("in range");
        }

        publication.unblock_timeout_ns = 1_000;

        assert!(publication.on_time_event(&mut manager, &region_pair, 2_000));
        assert_eq!(
            Some(1),
            manager.value(&region_pair, unblocked),
            "and the one that happened is counted"
        );
    }

    /// The other half of the same rule: a publication whose reader keeps moving
    /// refreshes its own deadline on every pass, so it never reaches one.
    #[test]
    fn a_publication_whose_reader_keeps_up_is_never_unblocked() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        let stalled = 128i32;
        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(17, stalled).raw(),
                )
                .expect("in range");
        }

        publication.unblock_timeout_ns = 1_000;
        publication.consumer_position = 32;
        publication.last_consumer_position = 0;

        assert!(
            !publication.on_time_event(&mut manager, &region_pair, 2_000),
            "the reader moved, so the timer was refreshed rather than fired"
        );
        assert_eq!(
            2_000, publication.time_of_last_consumer_position_change_ns,
            "to now"
        );
        assert_eq!(32, publication.last_consumer_position);

        let term = publication.log.term(0).expect("the first term");
        assert_eq!(
            Some(-stalled),
            frame::Frame::new(&term, 0).frame_length(),
            "the claim is still in flight"
        );
    }

    /// The draining arm's duty: a reader stuck behind a dead claim while the
    /// publication is on its way out. No waiting, and no `is_exclusive` test —
    /// the publication is draining, so the claim will never be committed.
    #[test]
    fn a_draining_publication_unblocks_the_claim_its_reader_is_stuck_behind() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);
        subscribe(&mut publication, &mut manager, &region_pair, 100, 0);

        let stalled = 128i32;
        {
            let term = publication.log.term(0).expect("the first term");
            term.store_i32_release(0, -stalled).expect("in range");

            // The producer got to 128, so a reader at zero has not read the
            // stream out and the publication is not drained.
            let metadata = publication.log.metadata().expect("the block");
            metadata
                .store_i64_release(
                    descriptor::TERM_TAIL_COUNTERS_OFFSET,
                    RawTail::new(17, stalled).raw(),
                )
                .expect("in range");
        }

        publication.state = State::Draining;

        // `now_ns` is the epoch and the window is the fifteen-second default:
        // a gated arm would not have fired.
        assert!(
            publication.on_time_event(&mut manager, &region_pair, 0),
            "the reader is stuck, so the log is unblocked for it"
        );

        let term = publication.log.term(0).expect("the first term");
        let frame = frame::Frame::new(&term, 0);
        assert_eq!(Some(stalled), frame.frame_length());
        assert!(frame.is_padding(), "the claim is padding now");
    }

    /// And a draining publication that is already drained moves on rather than
    /// trying: the unblock is the `else` of the drained test
    /// (`aeron_ipc_publication.c:556-585`).
    #[test]
    fn a_drained_publication_does_not_look_for_anything_to_unblock() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        publication.state = State::Draining;

        assert!(publication.on_time_event(&mut manager, &region_pair, 0));
        assert_eq!(State::Linger, publication.state, "nothing to wait for");
    }

    #[test]
    fn a_rejection_refuses_readers_until_the_cool_down_runs_out() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        let now = 1_000_000_000i64;
        let expires = now + publication.liveness_timeout_ns;

        assert!(!publication.is_in_cool_down());
        assert!(publication.is_accepting_subscriptions(&manager, &region_pair));

        publication.enter_cool_down(now);

        assert!(publication.is_in_cool_down());
        assert!(
            !publication.is_accepting_subscriptions(&manager, &region_pair),
            "a publication that has just refused its readers takes no new ones"
        );

        // The comparison is the reference's strict `<` — a deadline of exactly
        // now is not yet past (`aeron_ipc_publication.c:471`).
        assert!(!publication.cool_down_has_expired(expires - 1));
        assert!(!publication.cool_down_has_expired(expires));

        assert!(publication.cool_down_has_expired(expires + 1));
        assert!(!publication.is_in_cool_down());
        assert!(
            publication.is_accepting_subscriptions(&manager, &region_pair),
            "and readers are welcome again"
        );

        // Once only: the second call finds no cool down to end, which is what
        // stops the caller re-linking the same subscriptions on every tier.
        assert!(!publication.cool_down_has_expired(expires + 2));
    }

    #[test]
    fn a_second_rejection_inside_the_window_only_pushes_the_deadline_out() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let mut publication = publication(&dir, &mut manager, &region_pair);

        // The reference's guard is `if (!publication->in_cool_down)`
        // (`aeron_ipc_publication.c:252`): everything that takes the readers
        // away is skipped the second time, and this is what tells the caller
        // so.
        publication.enter_cool_down(1_000);
        assert!(publication.is_in_cool_down());
        assert!(!publication.cool_down_has_expired(1_000 + publication.liveness_timeout_ns));

        publication.enter_cool_down(2_000);
        assert!(publication.is_in_cool_down());
        assert!(
            !publication.cool_down_has_expired(1_000 + publication.liveness_timeout_ns + 1),
            "the later rejection moved the deadline, not the first one"
        );
        assert!(publication.cool_down_has_expired(2_000 + publication.liveness_timeout_ns + 1));
    }

    #[test]
    fn rejecting_a_publication_closes_its_connection_and_gives_the_readers_back() {
        let dir = TempDir::new();
        let mut regions = Regions::new();
        let (mut manager, region_pair) = regions.open();
        let (mut publication, stalled, reading) =
            stalled_and_reading(&dir, &mut manager, &region_pair, true);

        assert_eq!(Some(1), connected(&publication), "two readers are reading");

        let free_before = manager.free_list_len();

        publication.mark_disconnected();
        publication.clear_subscribers(&mut manager, &region_pair, 0);

        assert_eq!(
            Some(0),
            connected(&publication),
            "nobody is connected to a publication that just refused them"
        );
        assert!(
            publication.subscribers.is_empty(),
            "the set is empty, not merely inactive"
        );
        assert_eq!(
            free_before + 2,
            manager.free_list_len(),
            "and exactly the two readers' counters came back — {stalled} and {reading}; \
             the publication's own `pub-pos` and `pub-lmt` are still out"
        );
    }
}
