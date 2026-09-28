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

use crate::publication_params::PublicationParams;
use crate::subscribable::{Subscribable, SubscribableHooks, TetherablePosition};
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
    /// Where the limit may next jump to (`aeron_ipc_publication.h:172`).
    trip_gain: i32,
    trip_limit: i64,
    /// The highest subscriber position seen on the last pass.
    pub consumer_position: i64,
    /// How far the terms have been zeroed.
    clean_position: i64,
    /// How many clients hold a link to this publication.
    refcount: i32,
    /// When the state last changed (`managed_resource.time_of_last_state_change_ns`).
    time_of_last_state_change_ns: i64,
    state: State,
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
            trip_gain: params.publication_window_length / 8,
            trip_limit: 0,
            consumer_position: start_position,
            clean_position: start_position,
            refcount: 0,
            time_of_last_state_change_ns: 0,
            state: State::Active,
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

    /// The timeout tier's turn for this publication
    /// (`aeron_ipc_publication_on_time_event`, `:481-585`).
    ///
    /// Only the two states a publication can be in on its way out are handled
    /// here; the active state's other duties — the untethered subscription
    /// sweep, the blocked-publisher unblocker and the cool-down that follows one
    /// — are not this commit's.
    ///
    /// Returns whether anything happened.
    pub fn on_time_event(
        &mut self,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ns: i64,
    ) -> bool {
        match self.state {
            State::Active => false,
            State::Draining => {
                let producer_position = self.publisher_position().unwrap_or(0);
                counters.set_value(regions, self.pub_pos_counter_id, producer_position);

                if !self.is_drained(counters, regions) {
                    return false;
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
            let (min, max) = (
                self.subscribers.min_active_position(manager, regions),
                self.subscribers.max_active_position(manager, regions),
            );

            let (Some(min_sub_pos), Some(max_sub_pos)) = (min, max) else {
                return false;
            };

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
    /// one that still has unread data.
    pub fn is_accepting_subscriptions(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
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

        let mut publication =
            IpcPublication::create(log, identity, &params, PAGE_SIZE, socket_buffers, 0, 0)
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
}
