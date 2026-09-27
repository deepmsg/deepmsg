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

use crate::subscribable::{Subscribable, SubscribableHooks, TetherablePosition};

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

/// The defaults an IPC publication's metadata block is initialised with.
///
/// The values the reference's `aeron_ipc_publication_create` call site passes
/// (`aeron_ipc_publication.c:106-141`) with the context's defaults filled in.
/// The two `os_*` socket-buffer pairs are written as **zero** here where the
/// reference queries the operating system — recorded in `docs/compat.md`, and
/// harmless on this path because only the network transport reads them.
pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS: i64 = 5_000_000_000;
/// `AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT` (`aeron_driver_context.c:216`).
pub const UNTETHERED_RESTING_TIMEOUT_NS: i64 = 10_000_000_000;

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
    /// `log2(term_length)`.
    pub bits_to_shift: u32,
    /// The channel as the client sent it, which is what the counters' keys and
    /// labels carry.
    pub channel: Vec<u8>,
    /// Whether this publication has exactly one producer
    /// (`log_meta_data->type`).
    pub is_exclusive: bool,
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
    /// Where the limit may next jump to (`aeron_ipc_publication.h:172`).
    trip_gain: i32,
    trip_limit: i64,
    /// The highest subscriber position seen on the last pass.
    pub consumer_position: i64,
    /// How far the terms have been zeroed.
    clean_position: i64,
    /// How many clients hold a link to this publication.
    refcount: i32,
    state: State,
    has_reached_end_of_life: bool,
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
    /// `mtu_length` and `term_window_length` come from the driver's
    /// configuration; `page_size` is the file's.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        log: Box<LogFile>,
        registration_id: i64,
        client_id: i64,
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        channel: Vec<u8>,
        is_exclusive: bool,
        term_length: i32,
        mtu_length: i32,
        term_window_length: i32,
        page_size: i32,
    ) -> Option<Self> {
        // The term length is the caller's, not the metadata's: this function is
        // what writes the metadata, so reading it here would read zero.
        let bits_to_shift = position::bits_to_shift(term_length)?;

        if !log.initialise_tails(initial_term_id, None) {
            return None;
        }

        {
            let metadata = log.metadata()?;
            let init = descriptor::LogMetadataInit {
                end_of_stream_position: i64::MAX,
                is_connected: 0,
                active_transport_count: 0,
                correlation_id: registration_id,
                initial_term_id,
                mtu_length,
                term_length,
                page_size,
                publication_window_length: term_window_length,
                receiver_window_length: 0,
                socket_sndbuf_length: 0,
                os_default_socket_sndbuf_length: 0,
                os_max_socket_sndbuf_length: 0,
                socket_rcvbuf_length: 0,
                os_default_socket_rcvbuf_length: 0,
                os_max_socket_rcvbuf_length: 0,
                max_resend: 0,
                session_id,
                stream_id,
                entity_tag: 0,
                response_correlation_id: 0,
                linger_timeout_ns: 0,
                untethered_window_limit_timeout_ns: UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS,
                untethered_linger_timeout_ns: deepmsg_cnc::layout::NULL_VALUE,
                untethered_resting_timeout_ns: UNTETHERED_RESTING_TIMEOUT_NS,
                group: 0,
                is_response: false,
                rejoin: false,
                reliable: false,
                sparse: false,
                signal_eos: false,
                spies_simulate_connection: false,
                tether: false,
                is_exclusive,
            };

            descriptor::initialise(&metadata, &init)?;
        }

        // Nothing has been written, so the first term is where cleanup starts.
        let clean_position =
            Position::new(initial_term_id, 0, bits_to_shift, initial_term_id).raw();

        Some(Self {
            registration_id,
            client_id,
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            bits_to_shift,
            channel,
            is_exclusive,
            log,
            pub_pos_counter_id: 0,
            pub_lmt_counter_id: 0,
            subscribers: Subscribable::new(registration_id),
            term_window_length,
            trip_gain: term_window_length / 8,
            trip_limit: 0,
            consumer_position: clean_position,
            clean_position,
            refcount: 0,
            state: State::Active,
            has_reached_end_of_life: false,
        })
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
            )
            .expect("a log buffer"),
        );

        let mut publication = IpcPublication::create(
            log,
            99,
            7,
            100,
            1001,
            17,
            b"aeron:ipc".to_vec(),
            false,
            TERM_LENGTH,
            MTU,
            TERM_LENGTH / 2,
            PAGE_SIZE,
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
        assert_eq!(
            Some(deepmsg_cnc::layout::NULL_VALUE),
            metadata.load_i64_unaligned(descriptor::UNTETHERED_LINGER_TIMEOUT_NS_OFFSET),
            "the operator has not set a linger timeout"
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
