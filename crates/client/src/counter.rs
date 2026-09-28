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
