//! The four counters a persistent subscription publishes (114–117).
//!
//! # Which counters these are, and who owns them
//!
//! An application watching a persistent subscription wants four things it
//! cannot get from the API: which state the machine is in, how far the live
//! stream was ahead when it was joined, and how many times it has joined and
//! left the live stream. They are **client-side counter types**
//! (`aeron-client/src/main/c/aeron_counters.h:203-209`), not archive ones: the
//! subscription publishes them, into whatever driver it happens to be talking
//! to, and `AeronStat` reads them from the same file as everything else.
//!
//! # A caller may bring its own, and the reference still closes them
//!
//! `conclude` **auto-allocates whichever of the four were not provided**
//! (`…persistent_subscription.c:541-584`) — each of the four independently, so a
//! caller can provide one and let the other three be made. What is surprising
//! is the other end: `context_close` (`:117-140`) closes **all four**, provided
//! or not, and `shouldUseUserProvidedCounters`
//! (`…_persistent_subscription_test.cpp:930-935`) asserts exactly that. A
//! caller that hands in its own counters does not get them back at the end. This
//! build does the same, because a subscription whose teardown closed only the
//! counters it allocated would leave the ones it was handed alive and
//! unannounced.
//!
//! # The label is how a reader finds them
//!
//! `"%s: %d %s %d %s"` (`:353-355`) — the name, then the replay stream and
//! channel, then the live stream and channel. Every one of the four carries the
//! same four facts, so a file with several subscriptions in it says which is
//! which without a registry anywhere.

use std::time::Duration;

use deepmsg_client::client::{Client, CommandError};
use deepmsg_client::counter::Counter;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

/// `AERON_PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID`
/// (`aeron-client/src/main/c/aeron_counters.h:203`): which of the eighteen
/// states the subscription is in.
pub const STATE_TYPE_ID: i32 = 114;

/// `AERON_PERSISTENT_SUBSCRIPTION_JOIN_DIFFERENCE_TYPE_ID` (`:205`): how far
/// the live stream was ahead of the replay position when live was joined, or
/// [`NOTHING_JOINED`].
pub const JOIN_DIFFERENCE_TYPE_ID: i32 = 115;

/// `AERON_PERSISTENT_SUBSCRIPTION_LIVE_LEFT_COUNT_TYPE_ID` (`:207`).
pub const LIVE_LEFT_COUNT_TYPE_ID: i32 = 116;

/// `AERON_PERSISTENT_SUBSCRIPTION_LIVE_JOINED_COUNT_TYPE_ID` (`:209`).
pub const LIVE_JOINED_COUNT_TYPE_ID: i32 = 117;

/// The name the state counter's label starts with (`:544`).
pub const STATE_NAME: &str = "Persistent Subscription State";

/// The name the join-difference counter's label starts with (`:556`).
pub const JOIN_DIFFERENCE_NAME: &str = "Persistent Subscription Join Difference";

/// The name the live-left counter's label starts with (`:568`).
pub const LIVE_LEFT_NAME: &str = "Persistent Subscription Live Left Count";

/// The name the live-joined counter's label starts with (`:580`).
pub const LIVE_JOINED_NAME: &str = "Persistent Subscription Live Joined Count";

/// What a join difference reads before the live stream has ever been joined:
/// `INT64_MIN`, which is not a difference any position could produce
/// (`:1058`, `:1155`).
pub const NOTHING_JOINED: i64 = i64::MIN;

/// The channel and stream a counter's label names — the replay side first,
/// then the live one (`:353-355`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identity<'a> {
    /// The replay channel as configured, **after** `conclude` rewrote it.
    pub replay_channel: &'a str,
    /// The replay stream id.
    pub replay_stream_id: i32,
    /// The live channel as configured.
    pub live_channel: &'a str,
    /// The live stream id.
    pub live_stream_id: i32,
}

impl Identity<'_> {
    /// The label of a counter named `name` (`:353-355`).
    #[must_use]
    pub fn label(&self, name: &str) -> String {
        format!(
            "{name}: {} {} {} {}",
            self.replay_stream_id, self.replay_channel, self.live_stream_id, self.live_channel
        )
    }
}

/// The counters a caller brought, so that [`Counters::set_up`] allocates only
/// what is missing (`:541-584`).
///
/// All four are independent, which is the reference's shape rather than a
/// convenience: each is guarded by its own `if (NULL == context->…_counter)`.
#[derive(Debug, Default)]
pub struct Provided {
    /// The state counter, if the caller has one.
    pub state: Option<Counter>,
    /// The join-difference counter, if the caller has one.
    pub join_difference: Option<Counter>,
    /// The live-left counter, if the caller has one.
    pub live_left: Option<Counter>,
    /// The live-joined counter, if the caller has one.
    pub live_joined: Option<Counter>,
}

/// A persistent subscription's four counters, allocated and ready to be
/// written.
///
/// Holding this is what makes writing one possible, and handing it to
/// [`close`](Self::close) is what ends it: the reference guards every write with
/// `!aeron_counter_is_closed(counter)` (`:915`, `:927`, `:939`), and a value
/// that cannot be moved twice cannot be written after its counters are gone.
#[derive(Debug)]
pub struct Counters {
    state: Counter,
    join_difference: Counter,
    live_left: Counter,
    live_joined: Counter,
}

impl Counters {
    /// Allocate the four that are missing, and put the join difference at
    /// [`NOTHING_JOINED`].
    ///
    /// The four are allocated with their own type ids and with the label
    /// [`Identity::label`] builds; a caller-provided one is used as it is.
    ///
    /// **Only the join difference is written here.** The other three are left
    /// at whatever they hold — zero for a counter this build has just
    /// allocated, and whatever the caller left in one it provided, which is
    /// what `shouldUseUserProvidedCounters`' assertions describe. Writing them
    /// to zero would be a behaviour the reference does not have.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the driver will not allocate one of them.
    pub fn set_up(
        client: &mut Client,
        identity: &Identity<'_>,
        provided: Provided,
        timeout: Duration,
    ) -> Result<Self, CommandError> {
        let mut allocate = |slot: Option<Counter>, type_id: i32, name: &str| match slot {
            Some(counter) => Ok(counter),
            None => client.add_counter(type_id, &[], &identity.label(name), timeout),
        };

        let state = allocate(provided.state, STATE_TYPE_ID, STATE_NAME)?;
        let join_difference = allocate(
            provided.join_difference,
            JOIN_DIFFERENCE_TYPE_ID,
            JOIN_DIFFERENCE_NAME,
        )?;
        let live_left = allocate(provided.live_left, LIVE_LEFT_COUNT_TYPE_ID, LIVE_LEFT_NAME)?;
        let live_joined = allocate(
            provided.live_joined,
            LIVE_JOINED_COUNT_TYPE_ID,
            LIVE_JOINED_NAME,
        )?;

        let counters = Self {
            state,
            join_difference,
            live_left,
            live_joined,
        };

        if let Some(writable) = client.counters_writable() {
            counters.set_join_difference(&writable, NOTHING_JOINED);
        }

        Ok(counters)
    }

    /// Which of the eighteen states the machine is in (`:77-81`).
    #[must_use]
    pub fn state(&self, counters: &CountersReader<'_>) -> Option<i64> {
        self.state.value(counters)
    }

    /// How far live was ahead when it was joined (`:1969-1970`), or
    /// [`NOTHING_JOINED`].
    #[must_use]
    pub fn join_difference(&self, counters: &CountersReader<'_>) -> Option<i64> {
        self.join_difference.value(counters)
    }

    /// How many times the live stream has been left (`:941-945`).
    #[must_use]
    pub fn live_left(&self, counters: &CountersReader<'_>) -> Option<i64> {
        self.live_left.value(counters)
    }

    /// How many times the live stream has been joined (`:925-929`).
    #[must_use]
    pub fn live_joined(&self, counters: &CountersReader<'_>) -> Option<i64> {
        self.live_joined.value(counters)
    }

    /// Write the state (`:77-81`).
    pub fn set_state(&self, counters: &CountersReader<'_, ReadWrite>, value: i64) -> bool {
        self.state.set_value(counters, value)
    }

    /// Write the join difference (`:909-919`).
    pub fn set_join_difference(
        &self,
        counters: &CountersReader<'_, ReadWrite>,
        value: i64,
    ) -> bool {
        self.join_difference.set_value(counters, value)
    }

    /// Count a join, answering the new value (`:923-929`).
    pub fn note_live_joined(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        self.note(&self.live_joined, counters)
    }

    /// Count a departure, answering the new value (`:939-945`).
    pub fn note_live_left(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        self.note(&self.live_left, counters)
    }

    /// Add one to a counter and answer what it became.
    ///
    /// The reference increments in place with release ordering
    /// (`aeron_counter_increment_release`, `:927`) because it holds an address;
    /// this holds a name, so it reads and writes the slot. A subscription polls
    /// on one thread, so the pair is not a race here — and the two calls are the
    /// same two the reference makes, in the same order.
    fn note(&self, counter: &Counter, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        // Read through the region rather than through the handle: a writable
        // reader is not the read-only one `Counter::value` takes, and the id is
        // all either of them needs.
        let next = counters.value(counter.counter_id())?.wrapping_add(1);
        counter.set_value(counters, next).then_some(next)
    }

    /// Close all four, provided or allocated (`context_close`, `:117-140`).
    ///
    /// Every one is attempted: a tear-down that stopped at the first refusal
    /// would leave counters behind, which is the state the reference's `close`
    /// exists to prevent. The first refusal is answered, and the rest have still
    /// been tried.
    ///
    /// # Errors
    ///
    /// [`CommandError`] from the first counter the driver would not remove.
    pub fn close(self, client: &mut Client, timeout: Duration) -> Result<(), CommandError> {
        let mut first_error = None;

        for counter in [
            self.state,
            self.join_difference,
            self.live_left,
            self.live_joined,
        ] {
            if let Err(error) = client.remove_counter(&counter, timeout) {
                first_error.get_or_insert(error);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDENTITY: Identity<'_> = Identity {
        replay_channel: "aeron:udp?endpoint=localhost:0|rejoin=false",
        replay_stream_id: 2000,
        live_channel: "aeron:ipc",
        live_stream_id: 1000,
    };

    /// `:353-355`: `"%s: %d %s %d %s"`, with the replay side first. A reader
    /// finds a counter by this text, so it is a contract rather than a message.
    #[test]
    fn a_label_names_both_sides_in_the_references_order() {
        assert_eq!(
            "Persistent Subscription State: 2000 aeron:udp?endpoint=localhost:0|rejoin=false \
             1000 aeron:ipc",
            IDENTITY.label(STATE_NAME)
        );
    }

    /// The four labels differ only in their name, so a file with several
    /// subscriptions in it can be read by name *and* by which recording.
    #[test]
    fn the_four_labels_share_the_identity_they_were_built_with() {
        let labels = [
            IDENTITY.label(STATE_NAME),
            IDENTITY.label(JOIN_DIFFERENCE_NAME),
            IDENTITY.label(LIVE_LEFT_NAME),
            IDENTITY.label(LIVE_JOINED_NAME),
        ];

        for label in &labels {
            assert!(
                label
                    .ends_with(": 2000 aeron:udp?endpoint=localhost:0|rejoin=false 1000 aeron:ipc"),
                "{label}"
            );
        }

        let names: Vec<&str> = labels
            .iter()
            .map(|label| label.split(": ").next().expect("a label has a name"))
            .collect();
        assert_eq!(
            vec![
                "Persistent Subscription State",
                "Persistent Subscription Join Difference",
                "Persistent Subscription Live Left Count",
                "Persistent Subscription Live Joined Count",
            ],
            names
        );
    }

    /// The type ids are the reference's (`aeron_counters.h:203-209`), and they
    /// are consecutive because that is how the file was written.
    #[test]
    fn the_type_ids_are_the_clients_114_to_117() {
        assert_eq!(114, STATE_TYPE_ID);
        assert_eq!(115, JOIN_DIFFERENCE_TYPE_ID);
        assert_eq!(116, LIVE_LEFT_COUNT_TYPE_ID);
        assert_eq!(117, LIVE_JOINED_COUNT_TYPE_ID);
    }

    /// Nothing has joined, and no position could say so: `INT64_MIN` is not a
    /// difference two positions can have.
    #[test]
    fn nothing_joined_is_not_a_difference() {
        assert_eq!(i64::MIN, NOTHING_JOINED);
        assert_ne!(0, NOTHING_JOINED);
    }
}
