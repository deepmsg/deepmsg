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
//! the state field's zero *is* `UNUSED`. [`CounterRegions::new`] therefore does
//! the same, and a caller that hands it a non-zero region gets the reference's
//! behaviour rather than a safer one.
//!
//! **`state` is written last, with a release.** Everything else a reader needs
//! — the label, its length, the key, the type — is written before it
//! (`:109-121`). That single release is what makes a half-filled record
//! invisible, and it is the only reason a reader may trust the rest.
//!
//! **The allocator's whole state is in this process's heap, not in the file.**
//! `:72-88` holds both the free list and the high-water mark in the manager
//! struct, so both die with the driver. Nothing in the CnC marks a slot "on the
//! list", and nothing records how many ids were handed out: the durable half of
//! reclamation is `state = RECLAIMED` plus the reuse deadline, which is all a
//! *reader* ever sees. The consequence for a **restart** is worth stating
//! plainly, because it is easy to assume otherwise: the fallback scan starts
//! from id zero again and hands out ids whose records the file still shows as
//! `ALLOCATED` — it never consults the file's state
//! (`aeron_counters_manager.c:207-244` does the same). A crashed driver's
//! counters are not reconciled by the allocator; they are reconciled by the
//! directory discipline, which gives the file to the next driver only when the
//! old one is provably gone.
//!
//! **Reuse is a deadline, not a reference count.** `next_counter_id`
//! (`:207-244`) takes the first entry on the list whose
//! `free_for_reuse_deadline_ms <= now_ms`, and an id that is not yet cool is
//! skipped even when it sits at the head. There is no `is_reusable` predicate
//! in the C, and adding one would change which id a client is handed.
//!
//! # Two types, because a driver owns its own mapping
//!
//! The reference's manager holds pointers to both regions for its whole life
//! (`aeron_counters_manager.h:72-88`), which in Rust would make the type that
//! owns the CnC file borrow one of its own fields — a shape the language
//! rejects. So the split is the one the ring consumers already use: the
//! **process state** — the id allocator and the free list — lives in
//! [`CounterManager`], and the regions arrive per call as [`CounterRegions`].
//! A driver constructs the regions from its `CncFile` each time it touches a
//! counter; a test constructs them once from two arrays.
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

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};

use crate::counters::CountersReader;
use crate::layout;

/// The two regions a counter lives in, borrowed for one call.
pub struct CounterRegions<'a> {
    metadata: AtomicBuffer<'a, ReadWrite>,
    values: AtomicBuffer<'a, ReadWrite>,
}

impl<'a> CounterRegions<'a> {
    /// Pair the regions up.
    ///
    /// The lengths must satisfy the reference's rule — the metadata region is
    /// at least four times the values region
    /// (`concurrent/aeron_counters_manager.h:100-101`) — and a pair that does
    /// not is refused here rather than producing nonsense offsets later.
    pub fn new(
        metadata: AtomicBuffer<'a, ReadWrite>,
        values: AtomicBuffer<'a, ReadWrite>,
    ) -> Option<Self> {
        if metadata.len() < values.len().checked_mul(4)? {
            return None;
        }

        Some(Self { metadata, values })
    }

    /// A read-only view of the same two regions.
    ///
    /// The reader the client half of this crate uses, over a driver's own
    /// windows — which is how a driver checks what it just wrote without a
    /// second constructor for the pair, and how its tests assert against the
    /// same decoding a client would do.
    pub fn reader(&self) -> CountersReader<'_, ReadOnly> {
        CountersReader::new(self.metadata.as_read_only(), self.values.as_read_only())
    }

    /// The highest id that can exist, `values_length / 128 - 1`
    /// (`aeron_counters_manager.h:170`).
    pub fn max_counter_id(&self) -> i32 {
        #[allow(clippy::cast_possible_truncation)] // a region length, bounded by i32::MAX
        let max = (self.values.len() / layout::COUNTER_VALUE_LENGTH) as i32 - 1;
        max
    }
}

/// The allocator: who owns the ids, and which of them are waiting to be reused.
///
/// Everything here is process-local. The records themselves are in the regions,
/// and they outlive this type.
pub struct CounterManager {
    max_counter_id: i32,
    /// The highest id ever handed out, or `-1` for none
    /// (`aeron_counters_manager.c:64`).
    id_high_water_mark: i32,
    free_list: Vec<i32>,
    free_to_reuse_timeout_ms: i64,
}

impl CounterManager {
    /// Set up an allocator over a values region of `values_length` bytes.
    ///
    /// The reference derives the same ceiling at manager init
    /// (`aeron_counters_manager.c:66`) and allocates a two-entry free list
    /// (`:78`); this one grows a `Vec` instead, which is the same list without
    /// a reallocation policy to get wrong. `None` for an empty region: a
    /// region with no room for one value record has no ids at all.
    pub fn new(values_length: usize, free_to_reuse_timeout_ms: i64) -> Option<Self> {
        #[allow(clippy::cast_possible_truncation)] // a region length, bounded by i32::MAX
        let max_counter_id = (values_length / layout::COUNTER_VALUE_LENGTH) as i32 - 1;
        if max_counter_id < 0 {
            return None;
        }

        Some(Self {
            max_counter_id,
            id_high_water_mark: -1,
            free_list: Vec::new(),
            free_to_reuse_timeout_ms,
        })
    }

    /// The highest id that can exist.
    pub const fn max_counter_id(&self) -> i32 {
        self.max_counter_id
    }

    /// The highest id handed out so far.
    pub const fn id_high_water_mark(&self) -> i32 {
        self.id_high_water_mark
    }

    /// Ids waiting to be reused. Observer-side only.
    pub fn free_list_len(&self) -> usize {
        self.free_list.len()
    }

    /// Allocate a counter and return its id, or `None` if there is no room.
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
    pub fn allocate(
        &mut self,
        regions: &CounterRegions<'_>,
        type_id: i32,
        key: &[u8],
        label: &[u8],
        now_ms: i64,
    ) -> Option<i32> {
        let counter_id = self.next_counter_id(regions, now_ms)?;
        let offset = Self::metadata_offset(counter_id)?;

        regions
            .metadata
            .store_i32_relaxed(offset + layout::COUNTER_TYPE_ID_OFFSET, type_id)?;
        regions.metadata.store_i64_relaxed(
            offset + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET,
            layout::COUNTER_NOT_FREE_TO_REUSE,
        )?;

        // A key of length zero means "leave the field alone", which is the
        // reference's `NULL != key && key_length > 0` (`:112`). It is not a
        // request to write zeroes.
        if !key.is_empty() {
            let length = key.len().min(layout::COUNTER_KEY_LENGTH);
            regions
                .metadata
                .copy_in(offset + layout::COUNTER_KEY_OFFSET, &key[..length])?;
        }

        let length = label.len().min(layout::COUNTER_LABEL_LENGTH_MAX);
        regions
            .metadata
            .copy_in(offset + layout::COUNTER_LABEL_OFFSET, &label[..length])?;
        #[allow(clippy::cast_possible_truncation)] // bounded by COUNTER_LABEL_LENGTH_MAX
        regions
            .metadata
            .store_i32_relaxed(offset + layout::COUNTER_LABEL_LENGTH_OFFSET, length as i32)?;

        // The publication: every field above is now visible to a reader that
        // acquire-loads this one.
        regions.metadata.store_i32_release(
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
    pub fn free(&mut self, regions: &CounterRegions<'_>, counter_id: i32, now_ms: i64) -> bool {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return false;
        }
        let Some(offset) = Self::metadata_offset(counter_id) else {
            return false;
        };

        // Plain, not acquire, as the reference's check is (`:250`).
        let state = regions
            .metadata
            .load_i32_relaxed(offset + layout::COUNTER_STATE_OFFSET);
        if state != Some(layout::COUNTER_STATE_ALLOCATED) {
            return false;
        }

        if regions
            .metadata
            .store_i32_release(
                offset + layout::COUNTER_STATE_OFFSET,
                layout::COUNTER_STATE_RECLAIMED,
            )
            .is_none()
        {
            return false;
        }
        if regions
            .metadata
            .zero(
                offset + layout::COUNTER_KEY_OFFSET,
                layout::COUNTER_KEY_LENGTH,
            )
            .is_none()
        {
            return false;
        }
        if regions
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
    pub fn set_registration_id(
        &self,
        regions: &CounterRegions<'_>,
        counter_id: i32,
        value: i64,
    ) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        regions
            .values
            .store_i64_release(offset + layout::COUNTER_REGISTRATION_ID_OFFSET, value)
    }

    /// `owner_id`; a plain write, as `:150-157`.
    pub fn set_owner_id(
        &self,
        regions: &CounterRegions<'_>,
        counter_id: i32,
        value: i64,
    ) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        regions
            .values
            .store_i64_relaxed(offset + layout::COUNTER_OWNER_ID_OFFSET, value)
    }

    /// `reference_id`; a plain write, as `:159-166`.
    pub fn set_reference_id(
        &self,
        regions: &CounterRegions<'_>,
        counter_id: i32,
        value: i64,
    ) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        regions
            .values
            .store_i64_relaxed(offset + layout::COUNTER_REFERENCE_ID_OFFSET, value)
    }

    /// The counter's value, read with an acquire — the same load a driver makes
    /// when it decides whether a client is still alive.
    pub fn value(&self, regions: &CounterRegions<'_>, counter_id: i32) -> Option<i64> {
        let offset = self.value_offset(counter_id)?;
        regions
            .values
            .load_i64_acquire(offset + layout::COUNTER_VALUE_OFFSET)
    }

    /// Publish a counter's value.
    pub fn set_value(
        &self,
        regions: &CounterRegions<'_>,
        counter_id: i32,
        value: i64,
    ) -> Option<()> {
        let offset = self.value_offset(counter_id)?;
        regions
            .values
            .store_i64_release(offset + layout::COUNTER_VALUE_OFFSET, value)
    }

    /// Replace a label, truncating at the field width
    /// (`aeron_counters_manager.c:168-178`).
    pub fn update_label(
        &self,
        regions: &CounterRegions<'_>,
        counter_id: i32,
        label: &[u8],
    ) -> Option<()> {
        let offset = Self::metadata_offset(counter_id)?;
        let length = label.len().min(layout::COUNTER_LABEL_LENGTH_MAX);

        regions
            .metadata
            .copy_in(offset + layout::COUNTER_LABEL_OFFSET, &label[..length])?;
        #[allow(clippy::cast_possible_truncation)] // bounded by COUNTER_LABEL_LENGTH_MAX
        regions
            .metadata
            .store_i32_release(offset + layout::COUNTER_LABEL_LENGTH_OFFSET, length as i32)
    }

    /// Append to a label, truncating silently at the field width
    /// (`aeron_counters_manager.c:180-195`) — how the driver adds its duty
    /// cycle and threshold to the system counter labels.
    pub fn append_to_label(
        &self,
        regions: &CounterRegions<'_>,
        counter_id: i32,
        label: &[u8],
    ) -> Option<()> {
        let offset = Self::metadata_offset(counter_id)?;
        let current = Self::to_offset(
            regions
                .metadata
                .load_i32_acquire(offset + layout::COUNTER_LABEL_LENGTH_OFFSET)?,
        )?;
        let available = layout::COUNTER_LABEL_LENGTH_MAX.saturating_sub(current);
        let length = label.len().min(available);

        regions.metadata.copy_in(
            offset + layout::COUNTER_LABEL_OFFSET + current,
            &label[..length],
        )?;

        let total = current.checked_add(length)?;
        #[allow(clippy::cast_possible_truncation)] // bounded by COUNTER_LABEL_LENGTH_MAX
        regions
            .metadata
            .store_i32_release(offset + layout::COUNTER_LABEL_LENGTH_OFFSET, total as i32)
    }

    /// The next id to use: a cooled entry from the free list, else a new one.
    ///
    /// Mirrors `aeron_counters_manager.c:207-244`. The free list is scanned
    /// from the front and the **first** cool entry wins — a still-warm entry at
    /// the head does not stop the scan, so this is neither LIFO nor FIFO.
    fn next_counter_id(&mut self, regions: &CounterRegions<'_>, now_ms: i64) -> Option<i32> {
        for index in 0..self.free_list.len() {
            let counter_id = self.free_list[index];
            let offset = Self::metadata_offset(counter_id)?;
            let deadline = regions
                .metadata
                .load_i64_acquire(offset + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET)?;

            if now_ms >= deadline {
                // Reset first, remove second: if the reset cannot be written
                // the id must stay on the list, or it is stranded — reclaimed
                // in the file, absent from the list, and never handed out
                // again.
                self.reset_value(regions, counter_id)?;
                self.free_list.remove(index);
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
    fn reset_value(&self, regions: &CounterRegions<'_>, counter_id: i32) -> Option<()> {
        self.set_registration_id(regions, counter_id, layout::COUNTER_REGISTRATION_ID_DEFAULT)?;
        self.set_owner_id(regions, counter_id, layout::COUNTER_OWNER_ID_DEFAULT)?;
        self.set_reference_id(regions, counter_id, layout::COUNTER_REFERENCE_ID_DEFAULT)?;
        self.set_value(regions, counter_id, 0)
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

impl std::fmt::Debug for CounterManager {
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

        /// An allocator and one call's worth of regions over this fixture.
        ///
        /// The borrow is of the fixture, never of the manager — which is the
        /// whole reason the two are separate types.
        fn open(&mut self, free_to_reuse_timeout_ms: i64) -> (CounterManager, CounterRegions<'_>) {
            let values_length = self.values.0.len();
            let regions = CounterRegions::new(self.metadata.writable(), self.values.writable())
                .expect("regions are four-to-one");
            let manager = CounterManager::new(values_length, free_to_reuse_timeout_ms)
                .expect("at least one counter");

            (manager, regions)
        }
    }

    fn state(regions: &CounterRegions<'_>, counter_id: usize) -> i32 {
        counter_field(regions, counter_id, layout::COUNTER_STATE_OFFSET)
    }

    fn type_id(regions: &CounterRegions<'_>, counter_id: usize) -> i32 {
        counter_field(regions, counter_id, layout::COUNTER_TYPE_ID_OFFSET)
    }

    fn counter_field(regions: &CounterRegions<'_>, counter_id: usize, field: usize) -> i32 {
        let offset = counter_id * layout::COUNTER_METADATA_LENGTH + field;
        regions.metadata.load_i32_relaxed(offset).expect("in range")
    }

    fn free_deadline(regions: &CounterRegions<'_>, counter_id: usize) -> i64 {
        let offset = counter_id * layout::COUNTER_METADATA_LENGTH
            + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET;
        regions.metadata.load_i64_relaxed(offset).expect("in range")
    }

    fn label_length(regions: &CounterRegions<'_>, counter_id: usize) -> usize {
        usize::try_from(counter_field(
            regions,
            counter_id,
            layout::COUNTER_LABEL_LENGTH_OFFSET,
        ))
        .expect("a non-negative length")
    }

    fn bytes_at(
        regions: &CounterRegions<'_>,
        counter_id: usize,
        field: usize,
        len: usize,
    ) -> Vec<u8> {
        let offset = counter_id * layout::COUNTER_METADATA_LENGTH + field;
        let mut out = vec![0u8; len];
        regions
            .metadata
            .copy_out(offset, &mut out)
            .expect("in range");
        out
    }

    fn key(regions: &CounterRegions<'_>, counter_id: usize, len: usize) -> Vec<u8> {
        bytes_at(regions, counter_id, layout::COUNTER_KEY_OFFSET, len)
    }

    fn label(regions: &CounterRegions<'_>, counter_id: usize) -> String {
        let length = label_length(regions, counter_id);
        String::from_utf8(bytes_at(
            regions,
            counter_id,
            layout::COUNTER_LABEL_OFFSET,
            length,
        ))
        .expect("labels are ascii in these tests")
    }

    fn value_field(regions: &CounterRegions<'_>, counter_id: usize, field: usize) -> i64 {
        let offset = counter_id * layout::COUNTER_VALUE_LENGTH + field;
        regions.values.load_i64_relaxed(offset).expect("in range")
    }

    #[test]
    fn ids_are_handed_out_densely_from_zero() {
        let mut fixture = Fixture::new(3);
        let (mut manager, regions) = fixture.open(0);

        assert_eq!(
            Some(0),
            manager.allocate(&regions, 0, &[0, 0, 0, 0], b"first", 1)
        );
        assert_eq!(Some(1), manager.allocate(&regions, 0, &[], b"second", 1));
        assert_eq!(Some(2), manager.allocate(&regions, 0, &[], b"third", 1));

        assert_eq!(
            layout::COUNTER_STATE_ALLOCATED,
            state(&regions, 0),
            "the first record was published"
        );
        assert_eq!(layout::COUNTER_STATE_ALLOCATED, state(&regions, 1));
        assert_eq!(2, manager.id_high_water_mark());
        assert_eq!(
            layout::COUNTER_NOT_FREE_TO_REUSE,
            free_deadline(&regions, 0),
            "a live counter is never headed for the free list"
        );
    }

    #[test]
    fn a_full_region_runs_out_of_ids() {
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);

        assert_eq!(Some(0), manager.allocate(&regions, 0, &[], b"a", 0));
        assert_eq!(Some(1), manager.allocate(&regions, 0, &[], b"b", 0));
        assert_eq!(
            None,
            manager.allocate(&regions, 0, &[], b"c", 0),
            "no id 2 exists"
        );
    }

    #[test]
    fn a_key_is_written_only_when_it_has_length() {
        let mut fixture = Fixture::new(2);
        let (mut manager, regions) = fixture.open(0);

        assert_eq!(
            Some(0),
            manager.allocate(
                &regions,
                11,
                &7i64.to_le_bytes(),
                b"client-heartbeat: id=7",
                0
            )
        );
        assert_eq!(11, type_id(&regions, 0));
        assert_eq!(vec![7, 0, 0, 0, 0, 0, 0, 0], key(&regions, 0, 8));
        assert_eq!("client-heartbeat: id=7", label(&regions, 0));

        // An empty key leaves the field alone — which on a fresh region means
        // zeroes, but on a recycled slot means the previous tenant's bytes.
        assert_eq!(Some(1), manager.allocate(&regions, 0, &[], b"no key", 0));
        assert_eq!(vec![0u8; 8], key(&regions, 1, 8));
    }

    #[test]
    fn a_key_and_label_longer_than_their_fields_are_truncated() {
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);

        let long_key = vec![0xABu8; layout::COUNTER_KEY_LENGTH + 8];
        let long_label = vec![b'L'; layout::COUNTER_LABEL_LENGTH_MAX + 8];
        assert_eq!(
            Some(0),
            manager.allocate(&regions, 0, &long_key, &long_label, 0)
        );

        assert_eq!(
            vec![0xABu8; layout::COUNTER_KEY_LENGTH],
            key(&regions, 0, layout::COUNTER_KEY_LENGTH)
        );
        assert_eq!(layout::COUNTER_LABEL_LENGTH_MAX, label_length(&regions, 0));
    }

    #[test]
    fn allocating_does_not_touch_the_value_record() {
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);

        assert_eq!(Some(0), manager.allocate(&regions, 0, &[], b"a", 0));

        assert_eq!(0, value_field(&regions, 0, layout::COUNTER_VALUE_OFFSET));
        assert_eq!(
            layout::COUNTER_REGISTRATION_ID_DEFAULT,
            value_field(&regions, 0, layout::COUNTER_REGISTRATION_ID_OFFSET)
        );
        assert_eq!(
            layout::COUNTER_OWNER_ID_DEFAULT,
            value_field(&regions, 0, layout::COUNTER_OWNER_ID_OFFSET)
        );
        assert_eq!(Some(0), manager.value(&regions, 0), "and it reads as zero");
    }

    #[test]
    fn the_value_record_is_written_by_the_setters() {
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);
        let id = manager.allocate(&regions, 11, &[], b"a", 0).expect("an id");

        manager
            .set_registration_id(&regions, id, 42)
            .expect("in range");
        manager.set_owner_id(&regions, id, -1).expect("in range");
        manager.set_reference_id(&regions, id, 7).expect("in range");
        manager.set_value(&regions, id, 1234).expect("in range");

        assert_eq!(
            42,
            value_field(&regions, 0, layout::COUNTER_REGISTRATION_ID_OFFSET)
        );
        assert_eq!(
            -1,
            value_field(&regions, 0, layout::COUNTER_OWNER_ID_OFFSET)
        );
        assert_eq!(
            7,
            value_field(&regions, 0, layout::COUNTER_REFERENCE_ID_OFFSET)
        );
        assert_eq!(Some(1234), manager.value(&regions, id));
    }

    #[test]
    fn a_reclaimed_counter_stays_out_of_reuse_until_its_deadline() {
        let mut fixture = Fixture::new(2);
        let (mut manager, regions) = fixture.open(1_000);

        let first = manager
            .allocate(&regions, 0, &[], b"first", 10)
            .expect("an id");
        assert_eq!(0, first);
        assert!(manager.free(&regions, first, 10_000));

        // Still warm: the next allocation is a *new* id, not this one.
        assert_eq!(
            Some(1),
            manager.allocate(&regions, 0, &[], b"second", 10_500)
        );
        assert_eq!(layout::COUNTER_STATE_RECLAIMED, state(&regions, 0));
        assert_eq!(1, manager.free_list_len(), "still waiting");

        // Cooled: the id comes back, and its value record was reset on the way.
        manager.set_value(&regions, 1, 99).expect("in range");
        assert!(manager.free(&regions, 1, 12_000));
        assert_eq!(2, manager.free_list_len(), "both ids are waiting now");
        let reused = manager
            .allocate(&regions, 0, &[], b"third", 13_000)
            .expect("an id");
        assert_eq!(0, reused, "the first cool entry wins, and it is the oldest");
        assert_eq!(
            1,
            manager.free_list_len(),
            "0 left the list, 1 is still on it"
        );
        assert_eq!(
            Some(0),
            manager.value(&regions, reused),
            "reset to zero on reuse"
        );
    }

    #[test]
    fn a_recycled_id_stays_on_the_list_until_its_record_is_cleared() {
        // The reset is what makes a recycled slot safe to hand out, so it
        // happens *before* the id leaves the free list: an id that left the
        // list and then failed to reset would be stranded — reclaimed in the
        // file, absent from the list, never handed out again. The visible
        // half of that promise is that the list only ever shrinks when the
        // value record has been cleared, which is what this checks.
        let mut fixture = Fixture::new(2);
        let (mut manager, regions) = fixture.open(0);

        let first = manager
            .allocate(&regions, 0, &[], b"first", 0)
            .expect("an id");
        manager.set_value(&regions, first, 7).expect("in range");
        assert!(manager.free(&regions, first, 0));
        assert_eq!(1, manager.free_list_len());

        // Cooled (the timeout is zero), so the next allocation recycles it —
        // and the value it held is gone by the time anyone can see the id.
        let reused = manager
            .allocate(&regions, 0, &[], b"again", 0)
            .expect("an id");
        assert_eq!(first, reused);
        assert_eq!(Some(0), manager.value(&regions, reused));
        assert_eq!(0, manager.free_list_len());
    }

    #[test]
    fn a_warm_entry_at_the_head_does_not_stop_the_scan() {
        let mut fixture = Fixture::new(3);
        let (mut manager, regions) = fixture.open(1_000);

        // Free 0 and 1 at different times, so 1 is cool before 0 is.
        let zero = manager
            .allocate(&regions, 0, &[], b"zero", 0)
            .expect("an id");
        let one = manager
            .allocate(&regions, 0, &[], b"one", 0)
            .expect("an id");
        assert!(manager.free(&regions, zero, 10_000));
        assert!(manager.free(&regions, one, 9_000));

        // 0 is warm (deadline 11_000), 1 is cool (deadline 10_000): the scan
        // skips the head and takes 1.
        assert_eq!(
            Some(1),
            manager.allocate(&regions, 0, &[], b"reused", 10_500)
        );
        assert_eq!(1, manager.free_list_len(), "0 is still on the list");
    }

    #[test]
    fn freeing_clears_the_key_but_leaves_the_label_and_type() {
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);
        let id = manager
            .allocate(&regions, 11, &[1, 2, 3, 4], b"client-heartbeat: id=1", 0)
            .expect("an id");

        assert!(manager.free(&regions, id, 100));

        assert_eq!(layout::COUNTER_STATE_RECLAIMED, state(&regions, 0));
        assert_eq!(
            vec![0u8; layout::COUNTER_KEY_LENGTH],
            key(&regions, 0, layout::COUNTER_KEY_LENGTH),
            "the key is the one field reclamation clears"
        );
        assert_eq!("client-heartbeat: id=1", label(&regions, 0));
        assert_eq!(11, type_id(&regions, 0));
        assert_eq!(100, free_deadline(&regions, 0), "now + the reuse timeout");

        // An id cannot be freed twice, nor one that never existed.
        assert!(!manager.free(&regions, id, 100));
        assert!(!manager.free(&regions, 9, 100));
        assert!(!manager.free(&regions, -1, 100));
    }

    #[test]
    fn a_reused_slot_keeps_whatever_the_previous_label_left_beyond_the_new_length() {
        // The reference copies only `label_length` bytes and does not clear the
        // tail, so a recycled slot can show the previous tenant's text past the
        // current length. A reader bounds itself by `label_length`, which is
        // why this is legal — and why it must not be "fixed".
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);

        let long = b"a-very-long-label";
        let id = manager.allocate(&regions, 0, &[], long, 0).expect("an id");
        assert!(manager.free(&regions, id, 0));
        let id = manager
            .allocate(&regions, 0, &[], b"short", 1)
            .expect("reused");

        assert_eq!("short", label(&regions, id as usize));
        assert_eq!(
            &long[5..],
            &bytes_at(
                &regions,
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
        let (mut manager, regions) = fixture.open(0);

        let full = vec![b'x'; layout::COUNTER_LABEL_LENGTH_MAX];
        let id = manager.allocate(&regions, 0, &[], &full, 0).expect("an id");
        manager
            .append_to_label(&regions, id, b": DEDICATED")
            .expect("in range");
        assert_eq!(
            layout::COUNTER_LABEL_LENGTH_MAX,
            label_length(&regions, 0),
            "a full label has no room left"
        );
        assert_eq!(
            &full[..],
            &bytes_at(
                &regions,
                0,
                layout::COUNTER_LABEL_OFFSET,
                layout::COUNTER_LABEL_LENGTH_MAX
            )[..]
        );

        // With room, the append lands after the current text, not over it.
        let id = manager
            .allocate(&regions, 0, &[], b"Conductor max", 0)
            .expect("an id");
        manager
            .append_to_label(&regions, id, b": DEDICATED")
            .expect("in range");
        assert_eq!("Conductor max: DEDICATED", label(&regions, 1));
    }

    #[test]
    fn updating_a_label_replaces_it_wholesale() {
        let mut fixture = Fixture::new(1);
        let (mut manager, regions) = fixture.open(0);
        let id = manager
            .allocate(&regions, 0, &[], b"before", 0)
            .expect("an id");

        manager
            .update_label(&regions, id, b"after")
            .expect("in range");

        assert_eq!("after", label(&regions, 0));
        assert_eq!(5, label_length(&regions, 0));
    }

    #[test]
    fn a_metadata_region_smaller_than_four_to_one_is_refused() {
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH - 512);

        assert!(
            CounterRegions::new(metadata.writable(), values.writable()).is_none(),
            "the reference asserts metadata >= 4 * values (`aeron_counters_manager.h:100-101`)"
        );
    }

    #[test]
    fn ids_are_bounded_by_the_values_region() {
        let mut fixture = Fixture::new(1);
        let (manager, regions) = fixture.open(0);

        assert_eq!(1, manager.max_counter_id());
        assert_eq!(1, regions.max_counter_id(), "the same ceiling, twice");
        assert_eq!(None, manager.set_value(&regions, 2, 1));
        assert_eq!(None, manager.set_value(&regions, -1, 1));
        assert_eq!(
            Some(()),
            manager.set_value(&regions, 1, 1),
            "the last id is in range"
        );
    }

    #[test]
    fn an_empty_values_region_has_no_ids() {
        assert!(CounterManager::new(0, 0).is_none());
        assert!(CounterManager::new(layout::COUNTER_VALUE_LENGTH - 1, 0).is_none());
        assert_eq!(
            0,
            CounterManager::new(layout::COUNTER_VALUE_LENGTH, 0)
                .expect("one counter's worth")
                .max_counter_id()
        );
    }
}
