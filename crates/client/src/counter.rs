//! A counter this client asked the driver for.
//!
//! A counter is a slot in the CnC file's values region, and the slot is all a
//! handle needs to name: the value never leaves the host, so cross-process
//! references pass the 4-byte id and every reader — this client, another
//! client, the driver itself — opens the same slot on its own mapping (M20).
//! The reference's handle is `aeron_counter_t`
//! (`aeron-client/src/main/c/aeron_counter.h:30-46`) plus the constants
//! snapshot it also exposes (`aeron_counter_constants_t`, `aeronc.h:2478-2496`).
//!
//! The value address the reference resolves once at create, this handle
//! re-derives from the regions per call, which is the same trick the rest of
//! this crate plays with the CnC file: a Rust value cannot borrow a window out
//! of a mapping its owner also holds, so the regions travel with the call
//! (`aeron_counter_create`, `aeron_counter.h:19-48`, does no IO either way).
//!
//! The handle is a *name*, not a lease: the client that asked for the counter
//! is the owner, and [`crate::client::Client::counter`] saying it is still
//! held is what says it still exists. The reference's `is_closed` lives there
//! too, as the client's own list.

use deepmsg_cnc::counters::{CounterDescriptor, CountersReader};
use deepmsg_core::buffer::ReadWrite;

/// A counter announcement from the broadcast, for whoever is watching.
///
/// The to-clients ring is one broadcast every client reads in full, so a
/// client sees **every** counter appear and go away — its own, another
/// client's, and the heartbeats the driver allocates on its own initiative.
/// The reference delivers these through callback pairs an application
/// registers (`aeron_add_available_counter_handler` /
/// `aeron_add_unavailable_counter_handler`, fired from
/// `aeron_client_conductor.c:850-895` and `:898-906`); this crate's shape is
/// a queue the caller drains ([`crate::client::Client::counter_events`]),
/// because a poll-driven client has no thread to run a callback on.
///
/// Neither kind of event carries a resource with it. A [`Counter`] handle is
/// a name, and a slot that has been reclaimed already answers every question
/// about itself with "gone" — which is exactly how the reference behaves too:
/// its `on_unavailable_counter` fires the handlers and does not close or even
/// look up the counter it names (`aeron_client_conductor.c:898-906`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterEvent {
    /// A counter was allocated, by any client.
    Ready {
        /// The registration id it was allocated under — the owning client's
        /// id for a heartbeat, the request's correlation id otherwise.
        correlation_id: i64,
        /// The counter's id, for [`CountersReader`].
        counter_id: i32,
    },
    /// A counter went away — removed by its owner, or reclaimed with the
    /// client that held it.
    Unavailable {
        /// The registration id it had been allocated under.
        correlation_id: i64,
        /// The id the slot had.
        counter_id: i32,
    },
}

/// A counter the **driver** owns, allocated at this client's request.
///
/// It is a separate type from [`Counter`] because the difference is not
/// cosmetic and is not the caller's to remember: a static counter is not in the
/// driver's list for this client, so nothing announces it as unavailable when
/// this client goes, nothing frees it, and a `REMOVE_COUNTER` for it would free
/// something the client does not own. The reference marks it with a resource
/// *type* and matches on that type when a removal arrives
/// (`aeron_client_conductor_resource_type_match`,
/// `aeron-client/src/main/c/aeron_client_conductor.c:3061-3070`), and its
/// `close` for one is a no-op (`:1283-1287`). Here the type is the match:
/// [`Client::remove_counter`](crate::client::Client::remove_counter) takes a
/// [`Counter`], so there is no way to hand it one of these.
///
/// What is left is what a reader of somebody else's counter needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StaticCounter {
    /// The id it was allocated under, which is what its `key` searches on
    /// together with its type id.
    registration_id: i64,
    /// The slot in the values region.
    counter_id: i32,
}

impl StaticCounter {
    /// Bind the ids an `ON_STATIC_COUNTER` named.
    pub(crate) const fn new(registration_id: i64, counter_id: i32) -> Self {
        Self {
            registration_id,
            counter_id,
        }
    }

    /// The registration id the counter was asked for under.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The counter's id — the slot in the values region, and the only thing
    /// another process needs to read the same counter.
    pub const fn counter_id(&self) -> i32 {
        self.counter_id
    }

    /// The counter's value, as the file has it right now.
    pub fn value(&self, counters: &CountersReader<'_>) -> Option<i64> {
        counters.value(self.counter_id)
    }

    /// Set the counter's value.
    ///
    /// A static counter is usually *read* by clients and written by whoever
    /// asked for it — a driver or another service — but nothing in the format
    /// says so, and the reference does not check either.
    pub fn set_value(&self, counters: &CountersReader<'_, ReadWrite>, value: i64) -> bool {
        counters.set_value(self.counter_id, value).is_some()
    }

    /// The counter's descriptor — type, key, label, owner — as the file has it
    /// right now.
    ///
    /// `owner_id` reads [`deepmsg_cnc::layout::NULL_VALUE`] for one of these,
    /// which is the whole of what "static" means on the wire.
    pub fn descriptor(&self, counters: &CountersReader<'_>) -> Option<CounterDescriptor> {
        counters.get(self.counter_id)
    }
}

/// A counter allocated by the driver on this client's behalf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counter {
    /// The correlation id the `ADD_COUNTER` was sent under.
    correlation_id: i64,
    /// The registration id the driver allocated it under — for a counter a
    /// client asked for, the same as the correlation id, kept separate
    /// because the heartbeat counter (whose handle the driver makes itself)
    /// differs.
    registration_id: i64,
    /// The slot in the values region, which is the whole address of a counter.
    counter_id: i32,
}

impl Counter {
    /// Bind the ids a ready response named (`aeron_counter_create`,
    /// `aeron_counter.h:19-48`).
    pub(crate) const fn new(correlation_id: i64, registration_id: i64, counter_id: i32) -> Self {
        Self {
            correlation_id,
            registration_id,
            counter_id,
        }
    }

    /// The correlation id of the `ADD_COUNTER` that asked for this counter.
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// The registration id the driver allocated it under, which is what a
    /// `REMOVE_COUNTER` names it by.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The counter's id — the slot in the values region, and the only thing
    /// another process needs to read or write the same counter.
    pub const fn counter_id(&self) -> i32 {
        self.counter_id
    }

    /// The counter's value, as the file has it right now.
    ///
    /// `None` means the slot is no longer an allocated counter: reclaimed by
    /// a removal, or taken back with the client that owned it.
    pub fn value(&self, counters: &CountersReader<'_>) -> Option<i64> {
        counters.value(self.counter_id)
    }

    /// Set the counter's value. `false` when the slot is not writable — the
    /// same condition [`Counter::value`] reports as `None`.
    ///
    /// This is the whole of a counter's life as far as the driver is
    /// concerned: nothing validates the value, and nothing echoes it anywhere.
    pub fn set_value(&self, counters: &CountersReader<'_, ReadWrite>, value: i64) -> bool {
        counters.set_value(self.counter_id, value).is_some()
    }

    /// The counter's descriptor — type, key, label, owner — as the file has
    /// it right now.
    pub fn descriptor(&self, counters: &CountersReader<'_>) -> Option<CounterDescriptor> {
        counters.get(self.counter_id)
    }
}
