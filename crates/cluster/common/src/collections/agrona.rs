//! Agrona's `Long2LongHashMap`, reproduced bit for bit.
//!
//! This is not "a hash map with i64 keys". The consensus module's timer service
//! keeps its timers in one of these and **writes them to the snapshot in this
//! map's iteration order** (`WheelTimerService.snapshot`, `:118-133`), so the
//! hash function, the probe sequence, the capacity policy and the deletion
//! compaction are all part of the byte contract (plan §7.4).
//!
//! Everything below was read out of `agrona-2.6.1.jar` with `javap -p -c`, and
//! is pinned against the jar's own behaviour by
//! `tests/agrona_order.rs` — the fixture in `tests/fixtures/agrona-order.tsv`
//! was produced by *running* the jar, not by reading it.
//!
//! Traps worth knowing before changing anything here:
//!
//! - The index is `evenHash(key, mask)`, not `hash(key) & mask`. `evenHash` is
//!   `(hash << 1) - (hash << 8)`, so that keys land on even slots — the array
//!   holds `[key, value, key, value, …]`.
//! - `missingValue` is the sentinel for "this slot's value is absent". A key
//!   slot is never cleared, only its value. `put` refuses to store it.
//! - Removal shifts the tail of the probe chain backwards (`compactChain`), so
//!   the order after a removal is not "the order with one hole in it".

/// `Long2LongHashMap.MIN_CAPACITY`.
pub const MIN_CAPACITY: usize = 8;

/// `Long2LongHashMap`'s default load factor.
pub const DEFAULT_LOAD_FACTOR: f32 = 0.65;

/// A `i64 -> i64` open-addressing map with Agrona's exact layout and order.
///
/// The unit of allocation is a `Vec<i64>` of `2 * capacity`: even slots are
/// keys, odd slots are values. An entry is present exactly when its value slot
/// is not [`Self::missing_value`].
#[derive(Clone, Debug)]
pub struct Long2LongHashMap {
    entries: Vec<i64>,
    missing_value: i64,
    load_factor: f32,
    resize_threshold: usize,
    size: usize,
}

impl Long2LongHashMap {
    /// `new Long2LongHashMap(missingValue)` — the constructor the timer service
    /// uses (`WheelTimerService.java:42`): capacity 8, load factor 0.65.
    pub fn new(missing_value: i64) -> Self {
        Self::with_capacity(MIN_CAPACITY, DEFAULT_LOAD_FACTOR, missing_value)
    }

    /// `new Long2LongHashMap(initialCapacity, loadFactor, missingValue)`.
    ///
    /// # Panics
    ///
    /// If `load_factor` is not in `(0, 1)`, matching Agrona's
    /// `CollectionUtil.validateLoadFactor`.
    pub fn with_capacity(initial_capacity: usize, load_factor: f32, missing_value: i64) -> Self {
        assert!(
            load_factor > 0.0 && load_factor < 1.0,
            "load factor must be in (0, 1)"
        );
        let capacity = find_next_positive_power_of_two(initial_capacity.max(MIN_CAPACITY));
        Self {
            entries: vec![missing_value; 2 * capacity],
            missing_value,
            load_factor,
            resize_threshold: (capacity as f32 * load_factor) as usize,
            size: 0,
        }
    }

    /// The sentinel that marks an absent value.
    pub fn missing_value(&self) -> i64 {
        self.missing_value
    }

    /// The number of entries present.
    pub fn size(&self) -> usize {
        self.size
    }

    /// `capacity()`, i.e. `entries.length / 2`.
    pub fn capacity(&self) -> usize {
        self.entries.len() / 2
    }

    /// `get(long)`, answering `missing_value` when absent.
    pub fn get(&self, key: i64) -> i64 {
        let mask = self.entries.len() - 1;
        let mut index = even_hash(key, mask);
        loop {
            let value = self.entries[index + 1];
            if value == self.missing_value {
                return self.missing_value;
            }
            if key == self.entries[index] {
                return value;
            }
            index = (index + 2) & mask;
        }
    }

    /// Whether `key` is present.
    pub fn contains_key(&self, key: i64) -> bool {
        self.get(key) != self.missing_value
    }

    /// `put(long, long)`, answering the previous value or `missing_value`.
    ///
    /// # Panics
    ///
    /// If `value == missing_value`, matching the reference's
    /// `IllegalArgumentException("cannot accept missingValue")`.
    pub fn put(&mut self, key: i64, value: i64) -> i64 {
        assert!(value != self.missing_value, "cannot accept missing value");
        let mask = self.entries.len() - 1;
        let mut index = even_hash(key, mask);
        loop {
            let existing = self.entries[index + 1];
            if existing == self.missing_value || key == self.entries[index] {
                break;
            }
            index = (index + 2) & mask;
        }
        let previous = self.entries[index + 1];
        if previous == self.missing_value {
            self.size += 1;
        }
        self.entries[index] = key;
        self.entries[index + 1] = value;
        if self.size > self.resize_threshold {
            self.rehash(self.entries.len());
        }
        previous
    }

    /// `remove(long)`, answering the removed value or `missing_value`.
    pub fn remove(&mut self, key: i64) -> i64 {
        let mask = self.entries.len() - 1;
        let mut index = even_hash(key, mask);
        loop {
            let value = self.entries[index + 1];
            if value == self.missing_value {
                return self.missing_value;
            }
            if key == self.entries[index] {
                self.entries[index + 1] = self.missing_value;
                self.size -= 1;
                self.compact_chain(index);
                return value;
            }
            index = (index + 2) & mask;
        }
    }

    /// `forEachLong(LongLongConsumer)` — **the iteration order the snapshot
    /// sees**. Entries come out in slot order, which is hash order.
    pub fn for_each<F: FnMut(i64, i64)>(&self, mut visit: F) {
        let mut i = 1;
        while i < self.entries.len() {
            let value = self.entries[i];
            if value != self.missing_value {
                visit(self.entries[i - 1], value);
            }
            i += 2;
        }
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        self.entries.fill(self.missing_value);
        self.size = 0;
    }

    /// `increaseCapacity()` -> `rehash(entries.length)`, which in turn allocates
    /// twice the capacity. Re-insertion walks the old array in order.
    fn rehash(&mut self, old_len: usize) {
        let old = core::mem::replace(&mut self.entries, vec![self.missing_value; 2 * old_len]);
        self.resize_threshold = (old_len as f32 * self.load_factor) as usize;
        let mask = self.entries.len() - 1;
        let mut i = 1;
        while i < old_len {
            let value = old[i];
            if value != self.missing_value {
                let key = old[i - 1];
                let mut index = even_hash(key, mask);
                while self.entries[index + 1] != self.missing_value {
                    index = (index + 2) & mask;
                }
                self.entries[index] = key;
                self.entries[index + 1] = value;
            }
            i += 2;
        }
    }

    /// `compactChain(int)`: walk the probe chain and pull each displaced entry
    /// back into the hole, one at a time.
    ///
    /// `i` is its own cursor, not a function of `index` — the reference advances
    /// it on every iteration whether or not it moved something, and the walk
    /// ends at the first empty slot. Deriving `i` from `index` loops forever
    /// the first time an entry is not moved.
    fn compact_chain(&mut self, mut index: usize) {
        let mask = self.entries.len() - 1;
        let mut i = index;
        loop {
            i = (i + 2) & mask;
            let value = self.entries[i + 1];
            if value == self.missing_value {
                return;
            }
            let key = self.entries[i];
            let key_hash = even_hash(key, mask);
            let move_it = if i < key_hash {
                key_hash <= index || index <= i
            } else {
                key_hash <= index && index <= i
            };
            if move_it {
                self.entries[index] = key;
                self.entries[index + 1] = value;
                self.entries[i + 1] = self.missing_value;
                index = i;
            }
        }
    }
}

/// `org.agrona.collections.Hashing.hash(long)` — the SplitMix64 finaliser.
///
/// ```text
/// h ^= h >>> 30;  h *= 0xBF58476D1CE4E5B9;
/// h ^= h >>> 27;  h *= 0x94D049BB133111EB;
/// h ^= h >>> 31;  return (int)h ^ (int)(h >>> 32);
/// ```
pub fn hash(value: i64) -> i32 {
    let mut h = value as u64;
    h ^= h >> 30;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^= h >> 31;
    ((h as u32) ^ ((h >> 32) as u32)) as i32
}

/// `org.agrona.collections.Hashing.evenHash(long, int)`:
/// `((hash << 1) - (hash << 8)) & mask`, so the result is an even slot index.
pub fn even_hash(key: i64, mask: usize) -> usize {
    let h = hash(key);
    let even = h.wrapping_shl(1).wrapping_sub(h.wrapping_shl(8));
    ((even as u32) & (mask as u32)) as usize
}

/// `org.agrona.BitUtil.findNextPositivePowerOfTwo`.
pub fn find_next_positive_power_of_two(value: usize) -> usize {
    assert!(value > 0, "value must be positive");
    value.next_power_of_two()
}
