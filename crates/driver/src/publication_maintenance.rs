//! The half of a network publication the **conductor** owns.
//!
//! The reference has one `aeron_network_publication_t` that its conductor and
//! its sender both hold a pointer to, and it keeps the two apart by *naming*:
//! a `conductor_fields` sub-struct (`aeron-driver/src/main/c/aeron_network_publication.h:68-80`)
//! holds the state only the conductor touches — `state`, `subscribable`,
//! `max_spy_position`, `clean_position`, `last_snd_pos` — and it is padded out
//! to four cache lines (`:82-83`) so that the two threads' writes do not share
//! one. Everything the other thread needs from that group is a `volatile` field
//! read with `AERON_GET_ACQUIRE` (`:75`, `:137-142`).
//!
//! This build cannot share the object: the live publication belongs to the
//! sender thread (`sender.rs`'s `publications`) and the conductor keeps a
//! `NetworkPublicationRecord`, which had no log buffer and no readers in it. So
//! the same split is written as **two types** — this one and the sender's
//! [`NetworkPublication`](crate::network_publication::NetworkPublication) — and
//! the handful of readings that genuinely cross the boundary are
//! [`PublicationSightlines`], one allocation both threads hold.
//!
//! # What lives here, and why it has to be together
//!
//! `update_pub_pos_and_lmt` (`aeron_network_publication.c:947-1010`) is the
//! conductor's in the reference (`aeron_driver_conductor.c:3395-3399`), and this
//! is everything it reaches: the log buffer it cleans
//! ([`LogFile::open`]'s second mapping — same pages, no copy), the readers whose
//! slowest one sets the limit, the position it has cleaned up to, and the
//! counters it writes the two published positions into.
//!
//! They cannot be separated. The reader set is the one thing the function reads
//! that the sender also has an opinion about — the spies' positions decide what
//! the limit is — so a conductor without it can only ask the sender, and a
//! limit computed from a reading that crossed a thread is a limit computed from
//! a different moment. Splitting them would be a change of meaning, not a
//! change of address.
//!
//! # The one writer
//!
//! `pub-pos` and `pub-lmt` are written **here and nowhere else**. That is not
//! tidiness: `CounterManager::set_value` is a plain release store
//! (`crates/cnc/src/counters.rs:391-399`), so a second writer can move
//! `pub-lmt` *backwards* — the value it read is stale by the time it stores it,
//! and in between the other thread has written a larger one. Measured while
//! this move was being prepared: the producer was held to a limit that went
//! down and the client sent 69.7% of its messages. §6.4 of
//! `doc/deepmsg-ownership-move-plan.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

use deepmsg_cnc::{CounterManager, CounterRegions};
use deepmsg_core::logbuffer::logfile::LogFile;
use deepmsg_core::logbuffer::position::{self, Position};

use crate::network_publication::{PublicationCounters, update_connected_status};
use crate::publication_params::PublicationParams;
use crate::subscribable::{
    Subscribable, SubscribableHooks, TetherState, TetherablePosition, UntetheredEvent,
};

/// The readings a conductor and a sender take of one publication.
///
/// One allocation per publication, made by the conductor and handed to the
/// sender with the publication itself. The four are exactly the fields the
/// reference marks `volatile`, plus the one derived reading it cannot precompute:
///
/// * `receivers_satisfy_flow_control` — the sender's, read by both. The
///   reference keeps `has_receivers` (`:137`) and asks the flow control
///   `has_required_receivers` at each use (`:760`); the two are folded into one
///   here because the strategy is the sender's alone, and a conductor that
///   could not ask it would have to hold a second stale copy of the answer.
/// * `has_spies` (`:138`) — the conductor's, read by the sender.
/// * `working_spies` — the count `has_subscribers` actually wants
///   (`aeron_driver_subscribable_has_working_positions`, `:762`). It is **not**
///   the same reading as `has_spies`: the removal hook sets that flag from the
///   count *before* the removal (`:1364-1378`), so a set whose only working
///   reader is joined by a resting one and then loses the resting one reads
///   `has_spies == false` with a working reader still in it. The build has
///   always had both readings and they can disagree; keeping both keeps the
///   behaviour.
/// * `max_spy_position` (`:75`) — the conductor's, read by the sender's idle
///   branch (`:620-623`).
///
/// Every write is a release store and every read an acquire load, which is what
/// the reference's `AERON_SET_RELEASE`/`AERON_GET_ACQUIRE` are. The ones the
/// sender writes are written **only when the answer changes**, as the
/// reference's own `aeron_network_publication_update_has_receivers` does
/// (`:84-96`): a store every pass would put one cache line in two cores'
/// exclusive caches for nothing.
#[derive(Debug)]
pub struct PublicationSightlines {
    receivers_satisfy_flow_control: AtomicBool,
    has_spies: AtomicBool,
    working_spies: AtomicUsize,
    max_spy_position: AtomicI64,
}

impl PublicationSightlines {
    /// Nothing has been heard from and nobody is reading.
    pub fn new() -> Self {
        Self {
            receivers_satisfy_flow_control: AtomicBool::new(false),
            has_spies: AtomicBool::new(false),
            working_spies: AtomicUsize::new(0),
            max_spy_position: AtomicI64::new(0),
        }
    }

    /// Whether a receiver has been heard from **and** the flow control is
    /// satisfied that it has the ones it requires (`:758-760`).
    pub fn receivers_satisfy_flow_control(&self) -> bool {
        self.receivers_satisfy_flow_control.load(Ordering::Acquire)
    }

    /// The sender's answer, stored only when it differs from the last one.
    pub fn set_receivers_satisfy_flow_control(&self, satisfied: bool) {
        if self.receivers_satisfy_flow_control.load(Ordering::Acquire) != satisfied {
            self.receivers_satisfy_flow_control
                .store(satisfied, Ordering::Release);
        }
    }

    /// Whether a spy is reading (`has_spies`), which is the flag the idle branch
    /// reads beside the position below.
    pub fn has_spies(&self) -> bool {
        self.has_spies.load(Ordering::Acquire)
    }

    /// Whether any reader the publication counts is there
    /// (`has_working_positions`).
    pub fn has_working_spies(&self) -> bool {
        self.working_spies.load(Ordering::Acquire) > 0
    }

    /// The largest position any spy has reported (`max_spy_position`).
    pub fn max_spy_position(&self) -> i64 {
        self.max_spy_position.load(Ordering::Acquire)
    }

    fn set_max_spy_position(&self, position: i64) {
        self.max_spy_position.store(position, Ordering::Release);
    }

    fn set_working_spies(&self, count: usize) {
        self.working_spies.store(count, Ordering::Release);
    }
}

impl Default for PublicationSightlines {
    fn default() -> Self {
        Self::new()
    }
}

/// What a publication does when one of its readers comes or goes
/// (`aeron_network_publication_add_subscriber_hook`, `:1353-1362`, and its
/// removal twin, `:1364-1378`).
///
/// The reference's hook is also where `is_connected` is rewritten when `ssc`
/// asked for it; here that is the caller's job, because the write needs the log
/// buffer's metadata block and the hook runs while the set is borrowed.
struct SpyHooks<'a> {
    has_spies: &'a AtomicBool,
}

impl SubscribableHooks for SpyHooks<'_> {
    fn position_added(&mut self, _position: &TetherablePosition) {
        self.has_spies.store(true, Ordering::Release);
    }

    fn position_removed(&mut self, _position: &TetherablePosition, working_before: usize) {
        // `working_before` is the count with the position still in it, which is
        // what the reference's hook reads: one working reader before the
        // removal means none after it.
        if working_before == 1 {
            self.has_spies.store(false, Ordering::Release);
        }
    }
}

/// The state of one network publication that only the conductor touches.
pub struct PublicationMaintenance {
    /// A second mapping of the publication's log buffer: same file, same pages,
    /// a different address. The sender holds the first.
    log: LogFile,
    /// The spies reading this publication — its only *local* readers. A remote
    /// one is a receiver, arrives as a status message and is the sender's.
    subscribers: Subscribable,
    /// How far the terms have been zeroed behind the readers
    /// (`conductor_fields.clean_position`, `:76`).
    clean_position: i64,
    /// The counters this writes. The publication has its own copy of the same
    /// ids; counters are shared memory, and both sides name them.
    counters: PublicationCounters,
    /// The geometry a position is read with.
    term_length: i32,
    term_window_length: i32,
    position_bits_to_shift: u32,
    initial_term_id: i32,
    /// `ssc`: whether a spy counts as a connection here.
    spies_simulate_connection: bool,
    /// The three deadlines of the tether cycle (`:1138-1140`).
    untethered_window_limit_timeout_ns: i64,
    untethered_linger_timeout_ns: i64,
    untethered_resting_timeout_ns: i64,
    /// The readings the sender also takes.
    sightlines: Arc<PublicationSightlines>,
}

impl std::fmt::Debug for PublicationMaintenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationMaintenance")
            .field("log", &self.log)
            .field("subscribers", &self.subscribers)
            .field("clean_position", &self.clean_position)
            .field("term_length", &self.term_length)
            .finish_non_exhaustive()
    }
}

impl PublicationMaintenance {
    /// Take the conductor's half of a publication whose log buffer has just
    /// been mapped.
    pub fn new(
        log: LogFile,
        registration_id: i64,
        params: &PublicationParams,
        counters: PublicationCounters,
        sightlines: Arc<PublicationSightlines>,
    ) -> Self {
        Self {
            log,
            subscribers: Subscribable::new(registration_id),
            // Nothing has been sent yet, so nothing has been read past yet
            // (`aeron_network_publication.c:249`; the reference re-seats it on
            // `snd-pos` when a publication is re-started, `:313`).
            clean_position: 0,
            counters,
            term_length: params.term_length,
            term_window_length: params.publication_window_length,
            // A term length the URI's own resolution already refused once; the
            // fallback is unreachable and says so rather than panicking.
            position_bits_to_shift: position::bits_to_shift(params.term_length).unwrap_or(0),
            initial_term_id: params.initial_term_id,
            spies_simulate_connection: params.spies_simulate_connection,
            untethered_window_limit_timeout_ns: params.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: params.untethered_linger_timeout_ns,
            untethered_resting_timeout_ns: params.untethered_resting_timeout_ns,
            sightlines,
        }
    }

    /// The readers, for a caller that has to walk or unlink them.
    pub fn subscribers(&self) -> &Subscribable {
        &self.subscribers
    }

    /// How far the terms have been zeroed, for a caller reporting it.
    pub const fn clean_position(&self) -> i64 {
        self.clean_position
    }

    /// Give this publication a local reader
    /// (`aeron_driver_subscribable_add_position`, `aeron_driver_conductor.c:3497-3523`).
    ///
    /// The hook runs before the position counts, which is why the reference's
    /// add hook writes `true` for the connected status rather than asking: at
    /// that moment a publication with one spy still looks like one with none.
    ///
    /// # Returns
    ///
    /// Whether the connected status has to be rewritten, which is `ssc` — the
    /// caller does the write, because it needs the log buffer.
    pub fn add_spy(&mut self, position: TetherablePosition) -> bool {
        let mut hooks = SpyHooks {
            has_spies: &self.sightlines.has_spies,
        };
        self.subscribers.add_position(position, &mut hooks);
        self.publish_working_spies();

        self.spies_simulate_connection
    }

    /// Take a local reader away (`aeron_driver_conductor.c:3525-3545`).
    ///
    /// The reference's remove hook asks `has_subscribers` for the status to
    /// write, and that expression is **true by construction** where it runs:
    /// the hook is called with the position still in the set, and the position
    /// being removed counts as a working one either way — it was active, or its
    /// `inactive_count` has already come down. So the status it writes is
    /// `true`, and what settles it afterwards is the next pass of
    /// [`Self::update_pub_pos_and_lmt`].
    pub fn remove_spy(&mut self, counter_id: i32) -> bool {
        let mut hooks = SpyHooks {
            has_spies: &self.sightlines.has_spies,
        };
        let _ = self.subscribers.remove_position(counter_id, &mut hooks);
        self.publish_working_spies();

        self.spies_simulate_connection
    }

    /// Whether this publication counts a reader at all
    /// (`aeron_network_publication_has_subscribers`, `:755-766`).
    ///
    /// Two ways to have one, and they are not the same kind of thing: a
    /// **receiver** is a remote reader that has said it is there — the sender's
    /// answer, and the flow control's, read from [`PublicationSightlines`] —
    /// and a **spy** is a local one reading this buffer, which counts only when
    /// `ssc` asked it to.
    pub fn has_subscribers(&self) -> bool {
        self.sightlines.receivers_satisfy_flow_control()
            || (self.spies_simulate_connection && self.sightlines.has_working_spies())
    }

    /// Write the log buffer's connected byte from the answer above
    /// (`aeron_network_publication_update_connected_status`, `:765-777`).
    ///
    /// Called by the spy paths when `ssc` makes a spy a connection, and by
    /// [`Self::update_pub_pos_and_lmt`]'s no-subscriber arm; the sender calls
    /// the same free function on its own mapping.
    pub fn refresh_connected_status(&self) {
        update_connected_status(&self.log, self.has_subscribers());
    }

    /// The count `has_subscribers` reads, kept in step by every mutation of the
    /// set rather than by the two hooks — `set_state` moves a position between
    /// active and inactive without either hook running.
    fn publish_working_spies(&self) {
        self.sightlines
            .set_working_spies(self.subscribers.working_position_count());
    }

    /// The highest position the producer has published
    /// (`aeron_network_publication_producer_position`, `:400-430`): the largest
    /// of the log buffer's three term tail counters.
    pub fn producer_position(&self) -> Option<i64> {
        self.log.producer_position(self.initial_term_id)
    }

    /// Write `pub-pos` and recompute `pub-lmt`
    /// (`aeron_network_publication_update_pub_pos_and_lmt`, `:947-1010`).
    ///
    /// The rule is two-stage, and that is the whole of network backpressure:
    ///
    /// * with no local reader, `pub-lmt` is `snd-pos` — the producer may get
    ///   one window ahead of what has actually left the machine;
    /// * with one, it is the slowest reader's position plus a term window, and
    ///   the term is cleaned behind it.
    ///
    /// Returns whether it did any work, for the conductor's cycle counter.
    pub fn update_pub_pos_and_lmt(
        &mut self,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> bool {
        let Some(producer_position) = self.producer_position() else {
            return false;
        };

        let snd_pos = counters.value(regions, self.counters.snd_pos).unwrap_or(0);

        let _ = counters.set_value(regions, self.counters.pub_pos, producer_position);

        if self.has_subscribers() {
            // The furthest any local reader has got, which only the `ssc` idle
            // branch below reads (`:961-980`): it is seeded at `snd-pos` and
            // never falls, so moving `snd-pos` up to it can never move it
            // backwards. Nothing here but the spies — a remote reader's
            // position arrives as a status message and is not in this set.
            // Both readings come out of one walk of the set; the reference
            // takes them inside a single loop for the same reason
            // (`:966-977`).
            let bounds = self.subscribers.active_position_bounds(counters, regions);

            if !self.subscribers.is_empty() {
                let max_consumer = bounds.map_or(snd_pos, |(_, max)| max);

                if max_consumer > self.sightlines.max_spy_position() {
                    self.sightlines.set_max_spy_position(max_consumer);
                }
            }

            let min_consumer = bounds.map_or(snd_pos, |(min, _)| min);

            let new_limit = min_consumer + i64::from(self.term_window_length);
            let current = counters.value(regions, self.counters.pub_lmt).unwrap_or(0);

            if new_limit > current {
                // The term one behind the slowest reader is one it is done with
                // (`:985`).
                self.clean_buffer(min_consumer - i64::from(self.term_length));

                // The limit moves only once the zeroing has caught up with the
                // term that is about to become the active one. A limit that
                // outran the cleaning would let the producer write into slots
                // still holding the previous generation, and this publication's
                // scan for availability reads a term's frames rather than
                // stopping at `pub-pos`.
                let clean_position = self.clean_position;
                let dirty_term_id = Position::from_raw(clean_position)
                    .term_id(self.position_bits_to_shift, self.initial_term_id);
                let active_term_id = Position::from_raw(new_limit)
                    .term_id(self.position_bits_to_shift, self.initial_term_id);
                let term_gap = position::term_count(active_term_id, dirty_term_id);
                let clean_offset =
                    Position::from_raw(clean_position).term_offset(self.position_bits_to_shift);

                if term_gap < 2 || (term_gap == 2 && clean_offset != 0) {
                    let _ = counters.set_value(regions, self.counters.pub_lmt, new_limit);
                }

                return true;
            }

            return false;
        }

        if counters.value(regions, self.counters.pub_lmt).unwrap_or(0) > snd_pos {
            update_connected_status(&self.log, false);
            let _ = counters.set_value(regions, self.counters.pub_lmt, snd_pos);
            self.clean_buffer(snd_pos - i64::from(self.term_length));
            return true;
        }

        false
    }

    /// Zero the terms the readers have finished with, a chunk at a time
    /// (`aeron_network_publication_clean_buffer`, `:923-945`).
    ///
    /// A producer reusing a term writes into slots the frames of a full buffer
    /// ago still occupy, and the log buffer's rule — write only into an empty
    /// slot — refuses that write. Zeroing behind the readers is what makes the
    /// slots empty again.
    ///
    /// Everything past the first eight bytes is zeroed first, and the
    /// frame-length word goes to zero last with a **release**: a reader that
    /// already saw the old length finds the bytes it describes still untouched.
    /// Zeroing the length first would let a reader see a frame whose body had
    /// been cleared underneath it.
    pub fn clean_buffer(&mut self, position: i64) {
        if position <= self.clean_position {
            return;
        }

        let index = Position::from_raw(self.clean_position).index(self.position_bits_to_shift);
        let clean_offset = Position::from_raw(self.clean_position)
            .term_offset(self.position_bits_to_shift)
            .unsigned_abs() as usize;

        let bytes_left_in_term = self.term_length as usize - clean_offset;
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

    /// Whether a reader has stopped reading, and what to do about it
    /// (`aeron_network_publication_check_untethered_subscriptions`,
    /// `aeron_network_publication.c:1120-1236`).
    ///
    /// The same three states as an IPC publication's readers, judged against
    /// the same shape of limit, and differing in two things — which is the
    /// whole of what a reader comparing the two C functions has to hold in
    /// mind:
    ///
    /// * the limit is measured from **`snd-pos`**: a reader is behind when it
    ///   has not got past `snd-pos - term_window + term_window/4` (`:1123-1125`).
    ///   An IPC publication measures from its *fastest* reader instead, because
    ///   there is no sender to say how far the stream has got;
    /// * a woken reader is seeded at **`snd-pos`** (`:1206`), which is the same
    ///   number its image can read from — an IPC reader is seeded at the
    ///   publication's join position.
    ///
    /// A tethered reader is never put aside, and neither is one that is merely
    /// slow: the limit moves with the stream, so a reader that keeps up is
    /// never behind it.
    ///
    /// The reference runs this from the **conductor**, on its timer tier
    /// (`:1277`, reached from `aeron_driver_conductor_on_check_managed_resources`),
    /// and so does this. What the reader set *is* decides the thread, not how
    /// soon a deadline is noticed.
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

        let sender_position = counters.value(regions, self.counters.snd_pos).unwrap_or(0);
        let window_length = i64::from(self.term_window_length);
        let untethered_window_limit = (sender_position - window_length) + (window_length / 4);

        // Copied out for the same reason the IPC publication's copy is: a
        // woken reader is seeded and re-stated while the set is being walked.
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
                            // The counter is the conductor's to give back, so
                            // the reader stays in the set with no id rather
                            // than leaving it: an id that is still there would
                            // be given back twice.
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
                        let _ = counters.set_value(regions, position.counter_id, sender_position);
                        let _ = self.subscribers.set_state(
                            position.counter_id,
                            TetherState::Active,
                            now_ns,
                        );

                        events.push(UntetheredEvent::Available {
                            subscription_registration_id: position.subscription_registration_id,
                            counter_id: position.counter_id,
                            join_position: sender_position,
                        });
                    }
                }
                TetherState::Closed => {}
            }
        }

        // A reader moved between active and inactive changes what
        // `has_subscribers` answers, and `set_state` runs no hook.
        self.publish_working_spies();

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_cnc::layout::NULL_COUNTER_ID;
    use deepmsg_core::buffer::AtomicBuffer;

    const TERM_LENGTH: i32 = 64 * 1024;
    const PAGE_SIZE: usize = 4096;

    /// A directory the test's log buffer lands in, removed when it ends.
    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "deepmsg-publication-maintenance-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("a temp directory");

            Self(dir)
        }

        /// The publication's buffer, as a **second view** of it — the shape the
        /// conductor gets, mapped over a file someone else made.
        fn log_buffer(&self) -> LogFile {
            let first = LogFile::create(
                &self.0.join("publication.logbuffer"),
                TERM_LENGTH,
                PAGE_SIZE,
                false,
            )
            .expect("a log buffer");

            first.initialise_tails(INITIAL_TERM_ID, None);

            LogFile::open(
                &self.0.join("publication.logbuffer"),
                TERM_LENGTH,
                PAGE_SIZE,
            )
            .expect("a second view")
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

    const INITIAL_TERM_ID: i32 = 1_000;

    /// The counters a publication is given, with the four positions at ids the
    /// fixture can name.
    const COUNTERS: PublicationCounters = PublicationCounters {
        fc_receivers: None,
        pub_pos: 0,
        pub_lmt: 1,
        snd_pos: 2,
        snd_lmt: 3,
        snd_bpe: 4,
        snd_naks_received: 5,
    };

    fn params(window: i32) -> PublicationParams {
        PublicationParams {
            term_length: TERM_LENGTH,
            term_length_named: false,
            mtu_length: 1408,
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
            initial_term_id: INITIAL_TERM_ID,
        }
    }

    struct Fixture {
        /// Held for its `Drop`: the log buffer the maintenance reads lives here.
        _dir: TempDir,
        counters: Counters,
        maintenance: PublicationMaintenance,
    }

    fn fixture() -> Fixture {
        let dir = TempDir::new();
        let log = dir.log_buffer();
        let sightlines = Arc::new(PublicationSightlines::new());

        Fixture {
            maintenance: PublicationMaintenance::new(
                log,
                7,
                &params(32 * 1024),
                COUNTERS,
                Arc::clone(&sightlines),
            ),
            counters: Counters::new(),
            _dir: dir,
        }
    }

    /// The position the stream has got to in the three tether tests below.
    ///
    /// Big enough to be well past the window limit a reader has to clear: the
    /// window is a term of 32 KiB, so the limit is `200_000 - 32_768 + 8_192`
    /// and a reader parked at zero is behind it by more than three quarters of
    /// a window — which is what "has stopped reading" means here.
    const SENT_POSITION: i64 = 200_000;

    /// Give the publication a reader sitting at `position`, and answer its
    /// counter id.
    fn add_reader(
        publication: &mut PublicationMaintenance,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        subscription_registration_id: i64,
        position: i64,
        is_tether: bool,
        is_rejoin: bool,
    ) -> i32 {
        let counter_id = counters
            .allocate(regions, 4, &[], b"sub-pos", 0)
            .expect("a counter");
        let _ = counters.set_value(regions, counter_id, position);
        let _ = publication.add_spy(TetherablePosition {
            counter_id,
            subscription_registration_id,
            time_of_last_update_ns: 0,
            state: TetherState::Active,
            is_tether,
            is_rejoin,
        });

        counter_id
    }

    /// A publication whose stream has been sent to [`SENT_POSITION`].
    ///
    /// What it has *not* done is leave the counters open: the regions borrow
    /// the fixture, so a test that wants a manager opens them itself.
    fn fixture_at_sent_position() -> Fixture {
        let mut fixture = fixture();

        {
            let (counters, regions) = fixture.counters.open();
            let _ = counters.set_value(
                &regions,
                fixture.maintenance.counters.snd_pos,
                SENT_POSITION,
            );
        }

        fixture
    }

    fn timeouts_never() -> i64 {
        i64::MAX / 2
    }

    #[test]
    fn a_spy_that_stops_reading_is_put_aside_and_woken_at_the_send_position() {
        let mut fixture = fixture_at_sent_position();
        let (mut manager, regions) = fixture.counters.open();
        let publication = &mut fixture.maintenance;

        // Two readers, both parked at zero: one is tethered, so it is never put
        // aside however slow it is, and the other is rejoining.
        let tethered = add_reader(publication, &mut manager, &regions, 7, 0, true, false);
        let rejoining = add_reader(publication, &mut manager, &regions, 8, 0, false, true);

        let window = publication.untethered_window_limit_timeout_ns;

        // Behind the limit and quiet for longer than the window timeout: the
        // one that is not tethered is told its image has gone.
        let events = publication.check_untethered_subscriptions(&mut manager, &regions, window + 1);
        assert_eq!(
            vec![UntetheredEvent::Unavailable {
                subscription_registration_id: 8,
                counter_id: rejoining,
            }],
            events,
            "only the untethered reader is put aside"
        );

        // A rejoining reader waits in linger rather than closing.
        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            window + publication.untethered_linger_timeout_ns + 2,
        );
        assert!(
            events.is_empty(),
            "a rejoining reader waits rather than closing"
        );
        assert_eq!(
            Some(TetherState::Resting),
            publication
                .subscribers()
                .find_by_counter(rejoining)
                .map(|position| position.state)
        );

        // And the resting timeout wakes it where the *stream* is, not where it
        // stalled — an image it can read from, which is the whole point.
        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            window
                + publication.untethered_linger_timeout_ns
                + publication.untethered_resting_timeout_ns
                + 3,
        );
        assert_eq!(
            vec![UntetheredEvent::Available {
                subscription_registration_id: 8,
                counter_id: rejoining,
                join_position: SENT_POSITION,
            }],
            events
        );
        assert_eq!(
            Some(SENT_POSITION),
            manager.value(&regions, rejoining),
            "the counter is seeded where the stream is, not left where it stalled"
        );
        assert_eq!(
            Some(TetherState::Active),
            publication
                .subscribers()
                .find_by_counter(rejoining)
                .map(|position| position.state)
        );
        assert_eq!(
            Some(TetherState::Active),
            publication
                .subscribers()
                .find_by_counter(tethered)
                .map(|position| position.state),
            "and the tethered reader was never moved"
        );
    }

    #[test]
    fn a_spy_that_is_not_rejoining_is_closed_and_keeps_its_place_in_the_set() {
        let mut fixture = fixture_at_sent_position();
        let (mut manager, regions) = fixture.counters.open();
        let publication = &mut fixture.maintenance;

        let leaving = add_reader(publication, &mut manager, &regions, 9, 0, false, false);
        let window = publication.untethered_window_limit_timeout_ns;

        let events = publication.check_untethered_subscriptions(&mut manager, &regions, window + 1);
        assert_eq!(
            vec![UntetheredEvent::Unavailable {
                subscription_registration_id: 9,
                counter_id: leaving,
            }],
            events
        );

        // Linger runs out and this one is not coming back: it is closed, and
        // the counter is named for the conductor to give back.
        let events = publication.check_untethered_subscriptions(
            &mut manager,
            &regions,
            window + publication.untethered_linger_timeout_ns + 2,
        );
        assert_eq!(
            vec![UntetheredEvent::Closed {
                counter_id: leaving
            }],
            events
        );

        // The reader stays in the set with no counter, which is what stops the
        // same id being given back twice — and it is never moved again.
        assert_eq!(
            Some(NULL_COUNTER_ID),
            publication
                .subscribers()
                .positions()
                .first()
                .map(|position| position.counter_id)
        );
        assert_eq!(
            Some(TetherState::Closed),
            publication
                .subscribers()
                .find_by_counter(NULL_COUNTER_ID)
                .map(|position| position.state)
        );
        assert!(
            publication
                .check_untethered_subscriptions(&mut manager, &regions, timeouts_never())
                .is_empty(),
            "a closed reader is not a reader anything happens to"
        );
    }

    #[test]
    fn a_spy_that_keeps_up_is_never_put_aside() {
        let mut fixture = fixture_at_sent_position();
        let (mut manager, regions) = fixture.counters.open();
        let publication = &mut fixture.maintenance;

        // Both readers are at the stream's position, which is past the limit a
        // reader has to clear to count as keeping up.
        let keeping_up = add_reader(
            publication,
            &mut manager,
            &regions,
            10,
            SENT_POSITION,
            false,
            false,
        );
        let behind_but_reading = add_reader(
            publication,
            &mut manager,
            &regions,
            11,
            SENT_POSITION - i64::from(publication.term_window_length) / 2,
            false,
            false,
        );

        // Far past every timeout: the one that is moving is not put aside for
        // being quiet, because it is not behind.
        let events =
            publication.check_untethered_subscriptions(&mut manager, &regions, timeouts_never());
        assert!(
            events.is_empty(),
            "a reader past the window limit is never behind it"
        );

        for counter_id in [keeping_up, behind_but_reading] {
            assert_eq!(
                Some(TetherState::Active),
                publication
                    .subscribers()
                    .find_by_counter(counter_id)
                    .map(|position| position.state)
            );
        }
    }
}
