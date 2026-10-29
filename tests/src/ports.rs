//! Ports for the single-driver tests: **one slot per process, above the
//! ephemeral range**.
//!
//! A test that binds inside `ip_local_port_range` is racing every outbound
//! socket the machine opens — including the ones the driver under test opens.
//! The cluster suite keeps its bands below that range (`cluster::bands`); the
//! single-driver tests used to compute `20_000 + pid % 20_000`, which puts
//! about a third of the processes inside it, and two of them put the *whole*
//! range inside it.
//!
//! The only safe window above the range is `61 000..65 535`, and the three
//! tests that already sat there (`image_lifecycle`, `unreliable_stream`,
//! `short_datagram`) had claimed it unevenly — 500, 500 and 3 000 ports wide
//! for two ports between them. This module is the whole window's arithmetic:
//! a process takes one slot, and a test inside it takes an offset in that slot.
//!
//! # The one invariant the compiler cannot check
//!
//! Test functions in one binary run in **parallel threads of one process**, so
//! they share a slot: **every test in a binary must use a distinct offset**.
//! [`port`] checks that `offset` stays inside the slot; nothing checks that two
//! tests of the same binary did not pick the same one.

use std::process;

/// The first port Linux hands out by itself — this machine's
/// `net.ipv4.ip_local_port_range` is `32768 60999`, the same numbers the
/// driver's own `EPHEMERAL_PORTS` carries.
pub const EPHEMERAL_RANGE_START: u16 = 32_768;

/// The last port Linux hands out by itself (`60999` here).
pub const EPHEMERAL_RANGE_END: u16 = 60_999;

/// The first port above [`EPHEMERAL_RANGE_END`]: where the window starts.
pub const SINGLE_DRIVER_PORT_BASE: u16 = 61_000;

/// How many ports one process owns.
///
/// 128 rather than 64 because the widest offset any single-driver test uses is
/// 124 (`response_channel`); 64 would mean renumbering its thirty call sites to
/// buy slots the suite has not needed.
pub const SINGLE_DRIVER_SLOT_WIDTH: u16 = 128;

/// How many slots the window holds: `35 * 128 = 4 480`, ending at 65 479 and
/// leaving `65 480..65 535` unclaimed for the day a slot is not enough.
///
/// `cargo test` runs test binaries one at a time, so a slot only collides when
/// two runs of the suite overlap (this repository is developed in several
/// worktrees): one run in thirty-five.
pub const SINGLE_DRIVER_SLOTS: u16 = 35;

/// The base of **this process's** slot.
#[must_use]
pub fn slot_base() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let slot = (process::id() as u16) % SINGLE_DRIVER_SLOTS;
    SINGLE_DRIVER_PORT_BASE + slot * SINGLE_DRIVER_SLOT_WIDTH
}

/// A port this process owns, `offset` ports into its slot.
///
/// # Panics
///
/// When `offset` would leave the slot and land in the next process's.
#[must_use]
pub fn port(offset: u16) -> u16 {
    assert!(
        offset < SINGLE_DRIVER_SLOT_WIDTH,
        "offset {offset} leaves this process's {SINGLE_DRIVER_SLOT_WIDTH}-port slot: \
         widen `SINGLE_DRIVER_SLOT_WIDTH`, or take an offset under it"
    );
    slot_base() + offset
}
