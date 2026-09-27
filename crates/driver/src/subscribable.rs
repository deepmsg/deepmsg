//! The set of positions reading one publication.
//!
//! Mirrors the reference's `aeron_subscribable_t` and `aeron_tetherable_position_t`
//! (`aeron-driver/src/main/c/aeron_driver_common.h:80-111`) and the operations
//! over them (`aeron_driver_conductor.c:3497-3691`). It is a *reusable*
//! structure there — a network publication's images are added to the same kind
//! of set as an IPC publication's subscribers — which is why it is its own
//! module here rather than a `Vec` inside the IPC publication.
//!
//! # Two counters, not one
//!
//! `length` is how many positions the set holds; `inactive_count` is how many of
//! them are in a state the publication should ignore (`RESTING` or `CLOSED` —
//! `is_active_state` at `:3657-3660`). Everything that decides whether the
//! publication has a reader asks `length > inactive_count` (`:3652-3655`), and
//! the publisher limit takes its minimum over the *active* ones only. Keeping
//! the two in step is what [`Subscribable::set_state`] exists for, and getting it
//! wrong shows up as a publication that either stalls with readers attached or
//! writes past them.
//!
//! # The hooks are called at a particular moment
//!
//! Adding a position calls the add hook before the position counts
//! (`:3515-3517`); removing one calls the remove hook **before** the position
//! leaves the array (`:3533-3537`), which is what makes IPC's `is_connected`
//! rule work: the hook sees one working position and knows it is the last.
//! [`SubscribableHooks::position_removed`] is therefore passed the count as it
//! was *before* the removal, because that is what the reference's hook can see.

use deepmsg_cnc::{CounterManager, CounterRegions};

/// Where a subscription's position sits in its tether life cycle
/// (`aeron_subscription_tether_state_t`, `aeron_driver_common.h:73-77`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TetherState {
    /// Being read.
    Active,
    /// Tethered, but the publication is going away: it stays readable until the
    /// stream is drained.
    Linger,
    /// Put aside because the reader stopped reading. It can be woken by
    /// activity, and while it is here the publication does not count it.
    Resting,
    /// Gone.
    Closed,
}

impl TetherState {
    /// Whether a publication with this position should treat it as a reader.
    ///
    /// The reference's `aeron_driver_subscribable_is_active_state`
    /// (`aeron_driver_conductor.c:3657-3660`): everything that is not resting or
    /// closed counts — including [`TetherState::Linger`], which is the point of
    /// a grace state.
    pub const fn is_active(self) -> bool {
        !matches!(self, Self::Resting | Self::Closed)
    }
}

/// One reader's position inside a publication
/// (`aeron_tetherable_position_t`, `aeron_driver_common.h:80-90`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TetherablePosition {
    /// The counter the **client** writes its position into. The reference
    /// holds an address; an id is the same thing here, and the address is
    /// derived from the regions the caller holds.
    pub counter_id: i32,
    /// The subscription this position belongs to — the link's registration id,
    /// which is what `ON_UNAVAILABLE_IMAGE` names.
    pub subscription_registration_id: i64,
    /// When the state last changed.
    pub time_of_last_update_ns: i64,
    /// Where it is in the tether cycle.
    pub state: TetherState,
    /// Whether the subscription asked to be tethered. A tethered reader is not
    /// put to rest for being slow.
    pub is_tether: bool,
    /// Whether the subscription is rejoining a stream it had left.
    pub is_rejoin: bool,
}

/// What a publication does when one of its positions comes or goes.
///
/// The reference threads these as two function pointers with a `clientd`
/// (`aeron_driver_common.h:106-108`). A trait is the same shape with the
/// client's identity in the receiver — and it keeps the borrow honest: the hook
/// runs while the set is borrowed, so what it may touch arrives as an argument
/// rather than as a field of the thing that owns the set.
pub trait SubscribableHooks {
    /// A position was added. For IPC this is where the log's `is_connected`
    /// byte goes to one.
    fn position_added(&mut self, position: &TetherablePosition);

    /// A position is about to be removed.
    ///
    /// `working_before` is `length - inactive_count` **before** the removal,
    /// because that is what the reference's hook reads: it sets IPC's
    /// `is_connected` to zero when the answer is one, meaning this was the last
    /// reader (`aeron_ipc_publication.h:130-144`).
    fn position_removed(&mut self, position: &TetherablePosition, working_before: usize);
}

/// What a state change did, for a caller that wants to report it.
///
/// The reference passes a logging function into every transition
/// (`aeron_untethered_subscription_state_change_func_t`); this returns the same
/// information so the caller decides what to do with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateChange {
    /// Where the position was.
    pub from: TetherState,
    /// Where it is now.
    pub to: TetherState,
    /// The subscription the position belongs to.
    pub subscription_registration_id: i64,
}

/// The positions reading one publication.
#[derive(Debug)]
pub struct Subscribable {
    /// The publication this set belongs to, as `ON_UNAVAILABLE_IMAGE` names it
    /// (`aeron_ipc_publication.c:144-151` sets it to the registration id).
    pub correlation_id: i64,
    positions: Vec<TetherablePosition>,
    /// How many of `positions` are resting or closed.
    inactive_count: usize,
}

impl Subscribable {
    /// An empty set for the publication whose registration id this is.
    pub const fn new(correlation_id: i64) -> Self {
        Self {
            correlation_id,
            positions: Vec::new(),
            inactive_count: 0,
        }
    }

    /// How many positions the set holds, active or not.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the set holds no positions at all.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// How many positions are not resting or closed
    /// (`aeron_driver_conductor.c:3647-3650`).
    pub fn working_position_count(&self) -> usize {
        self.positions.len() - self.inactive_count
    }

    /// Whether the publication has a reader at all (`:3652-3655`).
    pub fn has_working_positions(&self) -> bool {
        !self.positions.is_empty() && self.working_position_count() > 0
    }

    /// The positions, in insertion order.
    pub fn positions(&self) -> &[TetherablePosition] {
        &self.positions
    }

    /// The position reading through this counter, if there is one.
    ///
    /// Counters are per (subscription, publication) pair, so this is how a
    /// caller holding a counter id — an unlink, a stale-counter sweep — finds
    /// the reader it belongs to (`:3525-3545` searches the same way).
    pub fn find_by_counter(&self, counter_id: i32) -> Option<&TetherablePosition> {
        self.positions
            .iter()
            .find(|position| position.counter_id == counter_id)
    }

    /// The positions belonging to one subscription.
    pub fn find_by_subscription(&self, registration_id: i64) -> Vec<&TetherablePosition> {
        self.positions
            .iter()
            .filter(|position| position.subscription_registration_id == registration_id)
            .collect()
    }

    /// Add a reader at `counter_id`, as `link_subscribable` does
    /// (`aeron_driver_conductor.c:3497-3523`).
    ///
    /// The hook runs **before** the position counts, which is the reference's
    /// order: a publication that has just been handed a reader is already
    /// connected when the reader looks.
    pub fn add_position(
        &mut self,
        position: TetherablePosition,
        hooks: &mut impl SubscribableHooks,
    ) {
        hooks.position_added(&position);
        self.positions.push(position);
    }

    /// Remove the position reading through `counter_id`
    /// (`aeron_driver_conductor.c:3525-3545`).
    ///
    /// Returns whether one was there. The reference's version is a no-op
    /// otherwise, and it removes by swapping the last position into the hole —
    /// so the order of a publication's readers is not stable across removals,
    /// which is worth knowing before writing a test that assumes it is.
    pub fn remove_position(
        &mut self,
        counter_id: i32,
        hooks: &mut impl SubscribableHooks,
    ) -> Option<TetherablePosition> {
        let index = self
            .positions
            .iter()
            .position(|position| position.counter_id == counter_id)?;

        let position = self.positions[index];
        let working_before = self.working_position_count();

        if !position.state.is_active() {
            self.inactive_count -= 1;
        }

        hooks.position_removed(&position, working_before);
        self.positions.swap_remove(index);

        Some(position)
    }

    /// Move a position to another state, keeping the inactive count in step
    /// (`aeron_driver_conductor.c:3619-3645`).
    ///
    /// # Errors
    ///
    /// `None` if no position reads through that counter.
    pub fn set_state(
        &mut self,
        counter_id: i32,
        state: TetherState,
        now_ns: i64,
    ) -> Option<StateChange> {
        let index = self
            .positions
            .iter()
            .position(|position| position.counter_id == counter_id)?;

        let from = self.positions[index].state;
        if state != from {
            if !state.is_active() {
                self.inactive_count += 1;
            } else if !from.is_active() {
                self.inactive_count -= 1;
            }
        }

        self.positions[index].state = state;
        self.positions[index].time_of_last_update_ns = now_ns;

        Some(StateChange {
            from,
            to: state,
            subscription_registration_id: self.positions[index].subscription_registration_id,
        })
    }

    /// The smallest position among the **active** readers, or `None` when there
    /// is none.
    ///
    /// This is the input the publisher limit is computed from
    /// (`aeron_ipc_publication.c:299-312` reads the same field with an acquire,
    /// because the client is the one writing it).
    pub fn min_active_position(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Option<i64> {
        self.positions
            .iter()
            .filter(|position| position.state.is_active())
            .filter_map(|position| manager.value(regions, position.counter_id))
            .min()
    }

    /// The largest position among the **active** readers, or `None` when there
    /// is none.
    pub fn max_active_position(
        &self,
        manager: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Option<i64> {
        self.positions
            .iter()
            .filter(|position| position.state.is_active())
            .filter_map(|position| manager.value(regions, position.counter_id))
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hook that records what it was told.
    #[derive(Debug, Default)]
    struct Recorder {
        events: Vec<String>,
    }

    impl SubscribableHooks for Recorder {
        fn position_added(&mut self, position: &TetherablePosition) {
            self.events.push(format!("added:{}", position.counter_id));
        }

        fn position_removed(&mut self, position: &TetherablePosition, working_before: usize) {
            self.events.push(format!(
                "removed:{}:working={working_before}",
                position.counter_id
            ));
        }
    }

    fn position(counter_id: i32, registration_id: i64) -> TetherablePosition {
        TetherablePosition {
            counter_id,
            subscription_registration_id: registration_id,
            time_of_last_update_ns: 0,
            state: TetherState::Active,
            is_tether: true,
            is_rejoin: false,
        }
    }

    #[test]
    fn adding_a_position_hooks_before_it_counts() {
        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();

        set.add_position(position(0, 7), &mut hooks);

        assert_eq!(1, set.len());
        assert_eq!(1, set.working_position_count());
        assert!(set.has_working_positions());
        assert_eq!(vec!["added:0"], hooks.events);
    }

    #[test]
    fn removing_a_position_hooks_before_it_leaves() {
        // The order is the contract: IPC's hook reads the *pre-removal* count
        // to decide whether the publication is still connected.
        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();
        set.add_position(position(0, 7), &mut hooks);
        hooks.events.clear();

        set.remove_position(0, &mut hooks).expect("it was there");

        assert_eq!(vec!["removed:0:working=1"], hooks.events);
        assert!(set.is_empty());
        assert_eq!(0, set.working_position_count());
        assert!(!set.has_working_positions());
    }

    #[test]
    fn a_resting_position_stops_counting_but_stays_in_the_set() {
        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();
        set.add_position(position(0, 7), &mut hooks);
        set.add_position(position(1, 9), &mut hooks);

        let change = set
            .set_state(0, TetherState::Resting, 42)
            .expect("it was there");

        assert_eq!(TetherState::Active, change.from);
        assert_eq!(TetherState::Resting, change.to);
        assert_eq!(2, set.len(), "still attached");
        assert_eq!(1, set.working_position_count(), "but not a reader");
        assert!(set.has_working_positions());

        // And coming back is symmetric.
        set.set_state(0, TetherState::Active, 43)
            .expect("it was there");
        assert_eq!(2, set.working_position_count());

        // Linger counts as active: a grace state is still a reader.
        set.set_state(0, TetherState::Linger, 44)
            .expect("it was there");
        assert_eq!(2, set.working_position_count());

        set.set_state(0, TetherState::Closed, 45)
            .expect("it was there");
        assert_eq!(1, set.working_position_count());
    }

    #[test]
    fn the_last_reader_is_the_one_the_hook_sees() {
        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();
        set.add_position(position(0, 7), &mut hooks);
        set.add_position(position(1, 9), &mut hooks);
        hooks.events.clear();

        set.remove_position(0, &mut hooks);
        set.remove_position(1, &mut hooks);

        assert_eq!(
            vec!["removed:0:working=2", "removed:1:working=1"],
            hooks.events,
            "the second removal is the one that sees a single working position"
        );
    }

    #[test]
    fn removing_a_resting_position_keeps_the_inactive_count_honest() {
        // A resting position leaves the set without ever counting as a reader,
        // so the inactive count has to come down with it.
        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();
        set.add_position(position(0, 7), &mut hooks);
        set.add_position(position(1, 9), &mut hooks);
        set.set_state(0, TetherState::Resting, 1)
            .expect("it was there");

        set.remove_position(0, &mut hooks).expect("it was there");

        assert_eq!(1, set.len());
        assert_eq!(1, set.working_position_count(), "the other one still reads");
        assert!(set.has_working_positions());
    }

    #[test]
    fn the_minimum_is_taken_over_active_readers_only() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        let slow = manager
            .allocate(&regions, 4, &[], b"slow", 0)
            .expect("an id");
        let fast = manager
            .allocate(&regions, 4, &[], b"fast", 0)
            .expect("an id");
        manager.set_value(&regions, slow, 10).expect("in range");
        manager.set_value(&regions, fast, 500).expect("in range");

        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();
        set.add_position(position(slow, 7), &mut hooks);
        set.add_position(position(fast, 9), &mut hooks);

        assert_eq!(Some(10), set.min_active_position(&manager, &regions));
        assert_eq!(Some(500), set.max_active_position(&manager, &regions));

        // A resting reader is not one the limit may be held back by.
        set.set_state(slow, TetherState::Resting, 1)
            .expect("it was there");
        assert_eq!(Some(500), set.min_active_position(&manager, &regions));

        // And with nobody active there is no minimum at all.
        set.set_state(fast, TetherState::Resting, 2)
            .expect("it was there");
        assert_eq!(None, set.min_active_position(&manager, &regions));
        assert!(!set.has_working_positions());
    }

    #[test]
    fn a_position_can_be_found_by_its_counter_or_its_subscription() {
        let mut set = Subscribable::new(99);
        let mut hooks = Recorder::default();
        set.add_position(position(0, 7), &mut hooks);
        set.add_position(position(1, 9), &mut hooks);

        assert_eq!(
            9,
            set.find_by_counter(1)
                .expect("there")
                .subscription_registration_id
        );
        assert_eq!(1, set.find_by_subscription(9).len());
        assert!(set.find_by_counter(42).is_none());
    }

    use deepmsg_core::buffer::AtomicBuffer;

    #[repr(align(64))]
    struct Region(Vec<u8>);

    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new() -> Self {
            const VALUES_LENGTH: usize = 16 * 1024;
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
            let manager = CounterManager::new(16 * 1024, 1_000).expect("room");

            (manager, regions)
        }
    }
}
