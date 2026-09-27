//! The write half of the counters region.
//!
//! [`CountersReader`](crate::counters::CountersReader) is what a client does to
//! a counter; this is what the driver does. It mirrors
//! `aeron-client/src/main/c/concurrent/aeron_counters_manager.c:45-282`
//! function for function, because the bytes it leaves behind are the contract a
//! reader — including the reference `AeronStat` — decodes.
//!
//! # The four things that are not obvious
//!
//! **Nothing is initialised.** `aeron_counters_manager_init` (`:45-80`) writes
//! no byte of either region: a record is "unused" because the file was created
//! zero-filled (`aeron-driver.c:313` maps the CnC with `fill_with_zeroes`), and
//! the state field's zero *is* `UNUSED`. [`CounterManager::new`] therefore does
//! the same, and a caller that hands it a non-zero region gets the reference's
//! behaviour rather than a safer one.
//!
//! **`state` is written last, with a release.** Everything else a reader needs
//! — the label, its length, the key, the type — is written before it
//! (`:109-121`). That single release is what makes a half-filled record
//! invisible, and it is the only reason a reader may trust the rest.
//!
//! **The free list is in this process's heap, not in the file.** `:72-88` holds
//! it in the manager struct, so it dies with the driver and ids restart from
//! the high-water mark. Nothing in the CnC marks a slot "on the list"; the
//! durable half of reclamation is `state = RECLAIMED` plus the reuse deadline,
//! which is all a *reader* ever sees.
//!
//! **Reuse is a deadline, not a reference count.** `next_counter_id`
//! (`:207-244`) takes the first entry on the list whose
//! `free_for_reuse_deadline_ms <= now_ms`, and an id that is not yet cool is
//! skipped even when it sits at the head. There is no `is_reusable` predicate
//! in the C, and adding one would change which id a client is handed.
//!
//! # What is deliberately different
//!
//! Time arrives as an argument (`now_ms`) rather than through a clock the
//! manager owns: the reference holds a cached clock (`:81`), and a caller that
//! passes the same cached millisecond gets the same behaviour while a test can
//! jump the reuse window without sleeping a second. The failure returns are
//! `Option`/`bool` instead of the reference's `-1`/`0`, and the key and label
//! are byte slices with the reference's truncation rules kept intact — a
//! `&[]` key means "do not touch the key field", which is what a `NULL`/0-length
//! key means in C (`:112-115`), and is *not* the same as "write zeroes".

use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

use crate::layout;

/// The manager's view of the two regions.
///
/// Holds no copy of the counters: the records live in the mapped regions, and
/// the only per-process state is the id allocator and the free list, exactly as
/// in `aeron_counters_manager_stct` (`concurrent/aeron_counters_manager.h:72-88`).
pub struct CounterManager<'a> {
    pub(crate) metadata: AtomicBuffer<'a, ReadWrite>,
    pub(crate) values: AtomicBuffer<'a, ReadWrite>,
    max_counter_id: i32,
    /// The highest id ever handed out, or `-1` for none
    /// (`aeron_counters_manager.c:64`).
    id_high_water_mark: i32,
    free_list: Vec<i32>,
    free_to_reuse_timeout_ms: i64,
}

impl<'a> CounterManager<'a> {
    /// Pair the regions up.
    ///
    /// The lengths must satisfy the reference's rule — the metadata region is
    /// at least four times the values region
    /// (`concurrent/aeron_counters_manager.h:100-101`) — and the id ceiling is
    /// derived from the values region alone
    /// (`aeron_counters_manager.h:170`, `values_length / 128 - 1`).
    pub fn new(
        metadata: AtomicBuffer<'a, ReadWrite>,
        values: AtomicBuffer<'a, ReadWrite>,
        free_to_reuse_timeout_ms: i64,
    ) -> Option<Self> {
        if metadata.len() < values.len().checked_mul(4)? {
            return None;
        }

        #[allow(clippy::cast_possible_truncation)] // a region length, bounded by i32::MAX
        let max_counter_id = (values.len() / layout::COUNTER_VALUE_LENGTH) as i32 - 1;

        Some(Self {
            metadata,
            values,
            max_counter_id,
            id_high_water_mark: -1,
            free_list: Vec::new(),
            free_to_reuse_timeout_ms,
        })
    }

    /// The highest id that can exist, `values_length / 128 - 1`.
    pub const fn max_counter_id(&self) -> i32 {
        self.max_counter_id
    }

    /// The highest id handed out so far.
    ///
    /// Not part of the reference's reader API — it is the writer's, and it is
    /// exposed for reports and tests.
    pub const fn id_high_water_mark(&self) -> i32 {
        self.id_high_water_mark
    }

    /// Ids waiting to be reused. Observer-side only.
    pub fn free_list_len(&self) -> usize {
        self.free_list.len()
    }

    /// Allocate a counter and return its id, or `None` if the region is full.
    ///
    /// The write order is the reference's (`aeron_counters_manager.c:87-124`):
    /// `type_id`, the reuse deadline, the key, the label, `label_length`, and
    /// then `state` with a release. The **value record is not touched** — a
    /// fresh slot keeps whatever the mapping held, which for a file this build
    /// created is zero, and a recycled slot was reset by `next_counter_id`
    /// before it got here.
    ///
    /// `key` and `label` are truncated to the field widths (112 and 380), and
    /// anything past what they cover is left as it was: the reference copies
    /// `min(sizeof(field), length)` bytes and nothing more (`:114`, `:117-119`),
    /// so a recycled slot can show a longer key than the one just written.
    pub fn allocate(&mut self, type_id: i32, key: &[u8], label: &[u8], now_ms: i64) -> Option<i32> {
        let counter_id = self.next_counter_id(now_ms)?;
        let offset = Self::metadata_offset(counter_id)?;

        self.metadata
            .store_i32_relaxed(offset + layout::COUNTER_TYPE_ID_OFFSET, type_id)?;
        self.metadata.store_i64_relaxed(
            offset + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET,
            layout::COUNTER_NOT_FREE_TO_REUSE,
        )?;

        // A key of length zero means "leave the field alone", which is the
        // reference's `NULL != key && key_length > 0` (`:112`). It is not a
        // request to write zeroes.
        if !key.is_empty() {
            let length = key.len().min(layout::COUNTER_KEY_LENGTH);
            self.metadata
                .copy_in(offset + layout::COUNTER_KEY_OFFSET, &key[..length])?;
        }

        let length = label.len().min(layout::COUNTER_LABEL_LENGTH_MAX);
        self.metadata
            .copy_in(offset + layout::COUNTER_LABEL_OFFSET, &label[..length])?;
        #[allow(clippy::cast_possible_truncation)] // bounded by COUNTER_LABEL_LENGTH_MAX
        self.metadata
            .store_i32_relaxed(offset + layout::COUNTER_LABEL_LENGTH_OFFSET, length as i32)?;

        // The publication: every field above is now visible to a reader that
        // acquire-loads this one.
        self.metadata.store_i32_release(
            offset + layout::COUNTER_STATE_OFFSET,
            layout::COUNTER_STATE_ALLOCATED,
        )?;

        Some(counter_id)
    }

    /// Return a counter to the pool.
    ///
    /// False when the id is out of range or the record is not `ALLOCATED` —
    /// the reference returns `-1` for both (`aeron_counters_manager.c:246-254`).
    ///
    /// What is cleared is the `key`, because reclamation zeroes it
    /// (`:262-263`) and a reader is told not to look at a reclaimed record's
    /// key for exactly that reason. What is **not** cleared is `type_id`,
    /// `label` and `label_length` (`:250-282` writes none of them): a scanner
    /// that ignores `state` still sees the last tenant's name.
    pub fn free(&mut self, counter_id: i32, now_ms: i64) -> bool {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return false;
        }
        let Some(offset) = Self::metadata_offset(counter_id) else {
            return false;
        };

        // Plain, not acquire, as the reference's check is (`:250`).
        let state = self
            .metadata
            .load_i32_relaxed(offset + layout::COUNTER_STATE_OFFSET);
        if state != Some(layout::COUNTER_STATE_ALLOCATED) {
            return false;
        }

        if self
            .metadata
            .store_i32_release(
                offset + layout::COUNTER_STATE_OFFSET,
                layout::COUNTER_STATE_RECLAIMED,
            )
            .is_none()
        {
            return false;
        }
        if self
            .metadata
            .zero(
                offset + layout::COUNTER_KEY_OFFSET,
                layout::COUNTER_KEY_LENGTH,
            )
            .is_none()
        {
            return false;
        }
        if self
            .metadata
            .store_i64_relaxed(
                offset + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET,
                now_ms.saturating_add(self.free_to_reuse_timeout_ms),
            )
            .is_none()
        {
            return false;
        }

        self.free_list.push(counter_id);
        true
    }

    /// `registration_id`, which is how a reader finds this counter by owner
    /// (`aeron_counters_manager.c:141-148`).
    pub fn set_registration_id(&self, counter_id: i32, value: i64) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        self.values
            .store_i64_release(offset + layout::COUNTER_REGISTRATION_ID_OFFSET, value)
    }

    /// `owner_id`; a plain write, as `:150-157`.
    pub fn set_owner_id(&self, counter_id: i32, value: i64) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        self.values
            .store_i64_relaxed(offset + layout::COUNTER_OWNER_ID_OFFSET, value)
    }

    /// `reference_id`; a plain write, as `:159-166`.
    pub fn set_reference_id(&self, counter_id: i32, value: i64) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        self.values
            .store_i64_relaxed(offset + layout::COUNTER_REFERENCE_ID_OFFSET, value)
    }

    /// The counter's value, read with an acquire — the same load a driver makes
    /// when it decides whether a client is still alive.
    pub fn value(&self, counter_id: i32) -> Option<i64> {
        let offset = self.value_offset(counter_id)?;
        self.values
            .load_i64_acquire(offset + layout::COUNTER_VALUE_OFFSET)
    }

    /// Publish a counter's value.
    pub fn set_value(&self, counter_id: i32, value: i64) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        self.values
            .store_i64_release(offset + layout::COUNTER_VALUE_OFFSET, value)
    }

    /// Replace a label, truncating at the field width
    /// (`aeron_counters_manager.c:168-178`).
    pub fn update_label(&self, counter_id: i32, label: &[u8]) -> Option<()> {
        let offset = Self::metadata_offset(counter_id)?;
        let length = label.len().min(layout::COUNTER_LABEL_LENGTH_MAX);

        self.metadata
            .copy_in(offset + layout::COUNTER_LABEL_OFFSET, &label[..length])?;
        #[allow(clippy::cast_possible_truncation)] // bounded by COUNTER_LABEL_LENGTH_MAX
        self.metadata
            .store_i32_release(offset + layout::COUNTER_LABEL_LENGTH_OFFSET, length as i32)
    }

    /// Append to a label, truncating silently at the field width
    /// (`aeron_counters_manager.c:180-195`) — how the driver adds its duty
    /// cycle and threshold to the system counter labels.
    pub fn append_to_label(&self, counter_id: i32, label: &[u8]) -> Option<()> {
        let offset = Self::metadata_offset(counter_id)?;
        let current = Self::to_offset(
            self.metadata
                .load_i32_acquire(offset + layout::COUNTER_LABEL_LENGTH_OFFSET)?,
        )?;
        let available = layout::COUNTER_LABEL_LENGTH_MAX.saturating_sub(current);
        let length = label.len().min(available);

        self.metadata.copy_in(
            offset + layout::COUNTER_LABEL_OFFSET + current,
            &label[..length],
        )?;

        let total = current.checked_add(length)?;
        #[allow(clippy::cast_possible_truncation)] // bounded by COUNTER_LABEL_LENGTH_MAX
        self.metadata
            .store_i32_release(offset + layout::COUNTER_LABEL_LENGTH_OFFSET, total as i32)
    }

    /// The next id to use: a cooled entry from the free list, else a new one.
    ///
    /// Mirrors `aeron_counters_manager.c:207-244`. The free list is scanned
    /// from the front and the **first** cool entry wins — a still-warm entry at
    /// the head does not stop the scan, so this is neither LIFO nor FIFO.
    fn next_counter_id(&mut self, now_ms: i64) -> Option<i32> {
        for index in 0..self.free_list.len() {
            let counter_id = self.free_list[index];
            let offset = Self::metadata_offset(counter_id)?;
            let deadline = self
                .metadata
                .load_i64_acquire(offset + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET)?;

            if now_ms >= deadline {
                self.free_list.remove(index);
                self.reset_value(counter_id)?;
                return Some(counter_id);
            }
        }

        if self.id_high_water_mark + 1 > self.max_counter_id {
            return None;
        }

        self.id_high_water_mark += 1;
        Some(self.id_high_water_mark)
    }

    /// Clear a recycled slot's value record (`aeron_counters_manager.c:228-235`).
    fn reset_value(&self, counter_id: i32) -> Option<()> {
        self.set_registration_id(counter_id, layout::COUNTER_REGISTRATION_ID_DEFAULT)?;
        self.set_owner_id(counter_id, layout::COUNTER_OWNER_ID_DEFAULT)?;
        self.set_reference_id(counter_id, layout::COUNTER_REFERENCE_ID_DEFAULT)?;
        self.set_value(counter_id, 0)
    }

    /// A counter id as a byte-offset multiplier, refusing negatives.
    fn to_offset(value: i32) -> Option<usize> {
        usize::try_from(value).ok()
    }

    fn metadata_offset(counter_id: i32) -> Option<usize> {
        Self::to_offset(counter_id)?.checked_mul(layout::COUNTER_METADATA_LENGTH)
    }

    fn value_offset(&self, counter_id: i32) -> Option<usize> {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return None;
        }

        Self::to_offset(counter_id)?.checked_mul(layout::COUNTER_VALUE_LENGTH)
    }
}

impl std::fmt::Debug for CounterManager<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CounterManager")
            .field("max_counter_id", &self.max_counter_id)
            .field("id_high_water_mark", &self.id_high_water_mark)
            .field("free_list", &self.free_list)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An aligned region the tests can write into, standing in for a mapped
    /// one. `AtomicBuffer` cannot read a `Vec<u8>` because of alignment, so the
    /// storage has to be declared aligned.
    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned region")
        }
    }

    /// Two regions sized for `max_counter_id + 1` counters, four-to-one.
    ///
    /// Raw regions rather than a CnC file: the manager's contract is the two
    /// buffers, and a file would add a mapping and a page-size rule to every
    /// test without changing a byte the manager writes.
    struct Fixture {
        metadata: Region,
        values: Region,
    }

    impl Fixture {
        fn new(max_counter_id: usize) -> Self {
            let ids = max_counter_id + 1;
            Self {
                metadata: Region::zeroed(ids * layout::COUNTER_METADATA_LENGTH),
                values: Region::zeroed(ids * layout::COUNTER_VALUE_LENGTH),
            }
        }

        fn manager(&mut self, free_to_reuse_timeout_ms: i64) -> CounterManager<'_> {
            CounterManager::new(
                self.metadata.writable(),
                self.values.writable(),
                free_to_reuse_timeout_ms,
            )
            .expect("regions are four-to-one")
        }
    }

    // Reads go through the manager's own windows: it holds the only writable
    // borrow of the regions, and the bytes it wrote are the same bytes.

    fn state(manager: &CounterManager<'_>, counter_id: usize) -> i32 {
        counter_offset(manager, counter_id, layout::COUNTER_STATE_OFFSET)
    }

    fn type_id(manager: &CounterManager<'_>, counter_id: usize) -> i32 {
        counter_offset(manager, counter_id, layout::COUNTER_TYPE_ID_OFFSET)
    }

    fn free_deadline(manager: &CounterManager<'_>, counter_id: usize) -> i64 {
        let offset = counter_id * layout::COUNTER_METADATA_LENGTH
            + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET;
        manager.metadata.load_i64_relaxed(offset).expect("in range")
    }

    fn counter_offset(manager: &CounterManager<'_>, counter_id: usize, field: usize) -> i32 {
        let offset = counter_id * layout::COUNTER_METADATA_LENGTH + field;
        manager.metadata.load_i32_relaxed(offset).expect("in range")
    }

    fn label_length(manager: &CounterManager<'_>, counter_id: usize) -> usize {
        usize::try_from(counter_offset(
            manager,
            counter_id,
            layout::COUNTER_LABEL_LENGTH_OFFSET,
        ))
        .expect("a non-negative length")
    }

    fn bytes_at(
        manager: &CounterManager<'_>,
        counter_id: usize,
        field: usize,
        len: usize,
    ) -> Vec<u8> {
        let offset = counter_id * layout::COUNTER_METADATA_LENGTH + field;
        let mut out = vec![0u8; len];
        manager
            .metadata
            .copy_out(offset, &mut out)
            .expect("in range");
        out
    }

    fn key(manager: &CounterManager<'_>, counter_id: usize, len: usize) -> Vec<u8> {
        bytes_at(manager, counter_id, layout::COUNTER_KEY_OFFSET, len)
    }

    fn label(manager: &CounterManager<'_>, counter_id: usize) -> String {
        let length = label_length(manager, counter_id);
        String::from_utf8(bytes_at(
            manager,
            counter_id,
            layout::COUNTER_LABEL_OFFSET,
            length,
        ))
        .expect("labels are ascii in these tests")
    }

    fn value_field(manager: &CounterManager<'_>, counter_id: usize, field: usize) -> i64 {
        let offset = counter_id * layout::COUNTER_VALUE_LENGTH + field;
        manager.values.load_i64_relaxed(offset).expect("in range")
    }

    #[test]
    fn ids_are_handed_out_densely_from_zero() {
        let mut fixture = Fixture::new(3);
        let mut manager = fixture.manager(0);

        assert_eq!(Some(0), manager.allocate(0, &[0, 0, 0, 0], b"first", 1));
        assert_eq!(Some(1), manager.allocate(0, &[], b"second", 1));
        assert_eq!(Some(2), manager.allocate(0, &[], b"third", 1));

        assert_eq!(
            layout::COUNTER_STATE_ALLOCATED,
            state(&manager, 0),
            "the first record was published"
        );
        assert_eq!(layout::COUNTER_STATE_ALLOCATED, state(&manager, 1));
        assert_eq!(2, manager.id_high_water_mark());
        assert_eq!(
            layout::COUNTER_NOT_FREE_TO_REUSE,
            free_deadline(&manager, 0),
            "a live counter is never headed for the free list"
        );
    }

    #[test]
    fn a_full_region_runs_out_of_ids() {
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);

        assert_eq!(Some(0), manager.allocate(0, &[], b"a", 0));
        assert_eq!(Some(1), manager.allocate(0, &[], b"b", 0));
        assert_eq!(None, manager.allocate(0, &[], b"c", 0), "no id 2 exists");
    }

    #[test]
    fn a_key_is_written_only_when_it_has_length() {
        let mut fixture = Fixture::new(2);
        let mut manager = fixture.manager(0);

        assert_eq!(
            Some(0),
            manager.allocate(11, &7i64.to_le_bytes(), b"client-heartbeat: id=7", 0)
        );
        assert_eq!(11, type_id(&manager, 0));
        assert_eq!(vec![7, 0, 0, 0, 0, 0, 0, 0], key(&manager, 0, 8));
        assert_eq!("client-heartbeat: id=7", label(&manager, 0));

        // An empty key leaves the field alone — which on a fresh region means
        // zeroes, but on a recycled slot means the previous tenant's bytes.
        assert_eq!(Some(1), manager.allocate(0, &[], b"no key", 0));
        assert_eq!(vec![0u8; 8], key(&manager, 1, 8));
    }

    #[test]
    fn a_key_and_label_longer_than_their_fields_are_truncated() {
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);

        let long_key = vec![0xABu8; layout::COUNTER_KEY_LENGTH + 8];
        let long_label = vec![b'L'; layout::COUNTER_LABEL_LENGTH_MAX + 8];
        assert_eq!(Some(0), manager.allocate(0, &long_key, &long_label, 0));

        assert_eq!(
            vec![0xABu8; layout::COUNTER_KEY_LENGTH],
            key(&manager, 0, layout::COUNTER_KEY_LENGTH)
        );
        assert_eq!(layout::COUNTER_LABEL_LENGTH_MAX, label_length(&manager, 0));
    }

    #[test]
    fn allocating_does_not_touch_the_value_record() {
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);

        assert_eq!(Some(0), manager.allocate(0, &[], b"a", 0));

        assert_eq!(0, value_field(&manager, 0, layout::COUNTER_VALUE_OFFSET));
        assert_eq!(
            layout::COUNTER_REGISTRATION_ID_DEFAULT,
            value_field(&manager, 0, layout::COUNTER_REGISTRATION_ID_OFFSET)
        );
        assert_eq!(
            layout::COUNTER_OWNER_ID_DEFAULT,
            value_field(&manager, 0, layout::COUNTER_OWNER_ID_OFFSET)
        );
        assert_eq!(Some(0), manager.value(0), "and it reads as zero");
    }

    #[test]
    fn the_value_record_is_written_by_the_setters() {
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);
        let id = manager.allocate(11, &[], b"a", 0).expect("an id");

        manager.set_registration_id(id, 42).expect("in range");
        manager.set_owner_id(id, -1).expect("in range");
        manager.set_reference_id(id, 7).expect("in range");
        manager.set_value(id, 1234).expect("in range");

        assert_eq!(
            42,
            value_field(&manager, 0, layout::COUNTER_REGISTRATION_ID_OFFSET)
        );
        assert_eq!(
            -1,
            value_field(&manager, 0, layout::COUNTER_OWNER_ID_OFFSET)
        );
        assert_eq!(
            7,
            value_field(&manager, 0, layout::COUNTER_REFERENCE_ID_OFFSET)
        );
        assert_eq!(Some(1234), manager.value(id));
    }

    #[test]
    fn a_reclaimed_counter_stays_out_of_reuse_until_its_deadline() {
        let mut fixture = Fixture::new(2);
        let mut manager = fixture.manager(1_000);

        let first = manager.allocate(0, &[], b"first", 10).expect("an id");
        assert_eq!(0, first);
        assert!(manager.free(first, 10_000));

        // Still warm: the next allocation is a *new* id, not this one.
        assert_eq!(Some(1), manager.allocate(0, &[], b"second", 10_500));
        assert_eq!(layout::COUNTER_STATE_RECLAIMED, state(&manager, 0));
        assert_eq!(1, manager.free_list_len(), "still waiting");

        // Cooled: the id comes back, and its value record was reset on the way.
        manager.set_value(1, 99).expect("in range");
        assert!(manager.free(1, 12_000));
        assert_eq!(2, manager.free_list_len(), "both ids are waiting now");
        let reused = manager.allocate(0, &[], b"third", 13_000).expect("an id");
        assert_eq!(0, reused, "the first cool entry wins, and it is the oldest");
        assert_eq!(
            1,
            manager.free_list_len(),
            "0 left the list, 1 is still on it"
        );
        assert_eq!(Some(0), manager.value(reused), "reset to zero on reuse");
    }

    #[test]
    fn a_warm_entry_at_the_head_does_not_stop_the_scan() {
        let mut fixture = Fixture::new(3);
        let mut manager = fixture.manager(1_000);

        // Free 0 and 1 at different times, so 1 is cool before 0 is.
        let zero = manager.allocate(0, &[], b"zero", 0).expect("an id");
        let one = manager.allocate(0, &[], b"one", 0).expect("an id");
        assert!(manager.free(zero, 10_000));
        assert!(manager.free(one, 9_000));

        // 0 is warm (deadline 11_000), 1 is cool (deadline 10_000): the scan
        // skips the head and takes 1.
        assert_eq!(Some(1), manager.allocate(0, &[], b"reused", 10_500));
        assert_eq!(1, manager.free_list_len(), "0 is still on the list");
    }

    #[test]
    fn freeing_clears_the_key_but_leaves_the_label_and_type() {
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);
        let id = manager
            .allocate(11, &[1, 2, 3, 4], b"client-heartbeat: id=1", 0)
            .expect("an id");

        assert!(manager.free(id, 100));

        assert_eq!(layout::COUNTER_STATE_RECLAIMED, state(&manager, 0));
        assert_eq!(
            vec![0u8; layout::COUNTER_KEY_LENGTH],
            key(&manager, 0, layout::COUNTER_KEY_LENGTH),
            "the key is the one field reclamation clears"
        );
        assert_eq!("client-heartbeat: id=1", label(&manager, 0));
        assert_eq!(11, type_id(&manager, 0));
        assert_eq!(100, free_deadline(&manager, 0), "now + the reuse timeout");

        // An id cannot be freed twice, nor one that never existed.
        assert!(!manager.free(id, 100));
        assert!(!manager.free(9, 100));
        assert!(!manager.free(-1, 100));
    }

    #[test]
    fn a_reused_slot_keeps_whatever_the_previous_label_left_beyond_the_new_length() {
        // The reference copies only `label_length` bytes and does not clear the
        // tail, so a recycled slot can show the previous tenant's text past the
        // current length. A reader bounds itself by `label_length`, which is
        // why this is legal — and why it must not be "fixed".
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);

        let long = b"a-very-long-label";
        let id = manager.allocate(0, &[], long, 0).expect("an id");
        assert!(manager.free(id, 0));
        let id = manager.allocate(0, &[], b"short", 1).expect("reused");

        assert_eq!("short", label(&manager, id as usize));
        assert_eq!(
            &long[5..],
            &bytes_at(
                &manager,
                id as usize,
                layout::COUNTER_LABEL_OFFSET + 5,
                long.len() - 5
            )[..],
            "the tail is the old label's, exactly as the reference leaves it"
        );
    }

    #[test]
    fn appending_to_a_label_stops_at_the_field_width() {
        let mut fixture = Fixture::new(2);
        let mut manager = fixture.manager(0);

        let full = vec![b'x'; layout::COUNTER_LABEL_LENGTH_MAX];
        let id = manager.allocate(0, &[], &full, 0).expect("an id");
        manager
            .append_to_label(id, b": DEDICATED")
            .expect("in range");
        assert_eq!(
            layout::COUNTER_LABEL_LENGTH_MAX,
            label_length(&manager, 0),
            "a full label has no room left"
        );
        assert_eq!(
            &full[..],
            &bytes_at(
                &manager,
                0,
                layout::COUNTER_LABEL_OFFSET,
                layout::COUNTER_LABEL_LENGTH_MAX
            )[..]
        );

        // With room, the append lands after the current text, not over it.
        let id = manager
            .allocate(0, &[], b"Conductor max", 0)
            .expect("an id");
        manager
            .append_to_label(id, b": DEDICATED")
            .expect("in range");
        assert_eq!("Conductor max: DEDICATED", label(&manager, 1));
    }

    #[test]
    fn updating_a_label_replaces_it_wholesale() {
        let mut fixture = Fixture::new(1);
        let mut manager = fixture.manager(0);
        let id = manager.allocate(0, &[], b"before", 0).expect("an id");

        manager.update_label(id, b"after").expect("in range");

        assert_eq!("after", label(&manager, 0));
        assert_eq!(5, label_length(&manager, 0));
    }

    #[test]
    fn a_metadata_region_smaller_than_four_to_one_is_refused() {
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH - 512);

        assert!(
            CounterManager::new(metadata.writable(), values.writable(), 0).is_none(),
            "the reference asserts metadata >= 4 * values (`aeron_counters_manager.h:100-101`)"
        );
    }

    #[test]
    fn ids_are_bounded_by_the_values_region() {
        let mut fixture = Fixture::new(1);
        let manager = fixture.manager(0);

        assert_eq!(1, manager.max_counter_id());
        assert_eq!(None, manager.set_value(2, 1));
        assert_eq!(None, manager.set_value(-1, 1));
        assert_eq!(Some(()), manager.set_value(1, 1), "the last id is in range");
    }
}
