//! The two kinds of map this track is allowed to use, and why there are two.
//!
//! 1. [`agrona`] — a bit-exact `Long2LongHashMap`. **Required wherever the
//!    iteration order is observable**, which in this codebase means the timer
//!    table: `WheelTimerService.snapshot` writes its records in this map's
//!    order, so reimplementing "a map with the right pairs" is not enough
//!    (plan §7.4).
//! 2. [`HashMap`] / [`HashSet`] — the ordinary containers, with a **fixed**
//!    hasher. `std::collections::HashMap`'s default hasher is randomly seeded
//!    per process, so its iteration order is not reproducible; that is why
//!    performance spec §4 bans it and this alias exists.
//!
//! The rule of thumb: if the map is only ever queried by key, use the alias.
//! If its iteration order reaches a snapshot, a log or a message, use the
//! Agrona map — and check whether the reference uses Agrona there too, because
//! a different container is a different byte sequence.

use core::hash::{BuildHasherDefault, Hasher};

pub mod agrona;
pub mod ring;

pub use agrona::{Long2LongHashMap, even_hash, find_next_positive_power_of_two, hash};
pub use ring::ExpandableRingBuffer;

/// FNV-1a, 64-bit. Chosen for being boring: deterministic, no seed, one
/// multiply per byte, and short enough to check by eye. It is not a fast
/// hasher for adversarial input — nothing here hashes untrusted input in bulk.
#[derive(Clone, Copy, Debug)]
pub struct FixedHasher(u64);

impl Default for FixedHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl FixedHasher {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    /// A hasher primed with the offset basis, as `Hasher` builders do.
    pub const fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Hasher for FixedHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut h = self.0;
        for byte in bytes {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(Self::PRIME);
        }
        self.0 = h;
    }
}

/// The builder for [`FixedHasher`].
pub type FixedBuildHasher = BuildHasherDefault<FixedHasher>;

/// A `std::collections::HashMap` with a deterministic hasher.
///
/// The `allow` is not a loophole: the whole point of this alias is to name
/// `std::collections::HashMap` once, here, with a hasher that is not the
/// default one. Every other crate uses the alias.
#[allow(clippy::disallowed_types)]
pub type HashMap<K, V> = std::collections::HashMap<K, V, FixedBuildHasher>;

/// A `std::collections::HashSet` with a deterministic hasher. See [`HashMap`].
#[allow(clippy::disallowed_types)]
pub type HashSet<K> = std::collections::HashSet<K, FixedBuildHasher>;
