//! Enumerating the counters the driver publishes.
//!
//! Counters are how a client and a driver share small pieces of running state
//! without messaging each other, and `deepmsg` intends to read them the way
//! the reference tooling does: `aeron-stat` output and ours should describe
//! the same file the same way.
//!
//! The enumeration rule mirrors
//! `aeron-client/src/main/c/concurrent/aeron_counters_manager.c:284-321`
//! exactly, including the parts that look like bugs and are not.

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};

use crate::layout;

/// One allocated counter, as the reader sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CounterDescriptor {
    /// Index into both counter regions, `0..=max_counter_id`.
    pub counter_id: i32,
    /// What kind of counter this is. The catalogue of ids lives in the
    /// reference's `aeron-client/src/main/c/aeron_counters.h`.
    pub type_id: i32,
    /// Value of the counter at the moment it was read.
    pub value: i64,
    /// Ties the counter to the client registration that created it.
    pub registration_id: i64,
    /// Owner, as set by the reference's counter manager.
    pub owner_id: i64,
    /// Free-form reference, as set by the reference's counter manager.
    pub reference_id: i64,
    /// Free-form label. Not NUL-terminated on the wire, and its length is
    /// published separately.
    pub label: String,
}

/// What a scan saw.
///
/// The reference does not count the states it skips; ADR-0003 asks that
/// unknown inputs be skipped *and counted* rather than ignored, and this is
/// that count. It is observer-side only and cannot change any byte.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CounterScan {
    /// Records in the `ALLOCATED` state that were visited.
    pub allocated: u32,
    /// Records in the `RECLAIMED` state that were stepped over.
    pub reclaimed: u32,
    /// Records in a state this build does not know.
    pub unknown_state: u32,
}

/// A view over the two counter regions.
///
/// Read-only by default, and writable only when built from writable memory —
/// the same access level as the windows it holds, so a reader cannot acquire
/// the ability to write by asking for a different constructor.
pub struct CountersReader<'a, Access = ReadOnly> {
    metadata: AtomicBuffer<'a, Access>,
    values: AtomicBuffer<'a, Access>,
    max_counter_id: i32,
}

impl<'a, Access> CountersReader<'a, Access> {
    /// Pair the two regions up.
    ///
    /// A trailing partial value record is ignored rather than rejected: the
    /// reference derives the same ceiling by integer division
    /// (`aeron_counters_manager.h:170`), and a partial record cannot be a
    /// counter that was ever allocated.
    pub fn new(metadata: AtomicBuffer<'a, Access>, values: AtomicBuffer<'a, Access>) -> Self {
        #[allow(clippy::cast_possible_truncation)] // a region length, bounded by i32::MAX
        let max_counter_id = (values.len() / layout::COUNTER_VALUE_LENGTH) as i32 - 1;

        Self {
            metadata,
            values,
            max_counter_id,
        }
    }

    /// The highest id that can exist, `values_length / 128 - 1`.
    pub const fn max_counter_id(&self) -> i32 {
        self.max_counter_id
    }

    /// Visit every allocated counter, in id order.
    ///
    /// Stops at the first record whose state is `UNUSED`, because counters are
    /// allocated densely from zero — the reference does the same
    /// (`aeron_counters_manager.c:313-316`) rather than scanning the whole
    /// region. Reclaimed records are stepped over, not reported, and their
    /// `key` is never read: reclamation zeroes it non-atomically
    /// (`aeron_counters_manager.c:261-264`), so it can be torn.
    pub fn for_each(&self, mut visit: impl FnMut(&CounterDescriptor)) -> CounterScan {
        let mut scan = CounterScan::default();
        let mut counter_id: i32 = 0;
        let mut offset: usize = 0;

        while offset + layout::COUNTER_METADATA_LENGTH <= self.metadata.len() {
            let Some(state) = self
                .metadata
                .load_i32_acquire(offset + layout::COUNTER_STATE_OFFSET)
            else {
                break;
            };

            if layout::COUNTER_STATE_UNUSED == state {
                break;
            }

            if layout::COUNTER_STATE_ALLOCATED == state {
                if let Some(descriptor) = self.describe(counter_id, offset) {
                    scan.allocated += 1;
                    visit(&descriptor);
                }
            } else if layout::COUNTER_STATE_RECLAIMED == state {
                scan.reclaimed += 1;
            } else {
                scan.unknown_state += 1;
            }

            offset += layout::COUNTER_METADATA_LENGTH;
            counter_id += 1;
        }

        scan
    }

    /// The id of the counter with this type id **and** this registration id.
    ///
    /// How a client finds its own heartbeat counter: the driver allocates it
    /// with type `11` and the client's id as the registration
    /// (`aeron-driver/src/main/c/aeron_driver_conductor.c:994-1006`), and the
    /// reference finds it the same way
    /// (`aeron-client/src/main/c/aeron_client_conductor.c:1337-1340`).
    pub fn find_by_type_and_registration(&self, type_id: i32, registration_id: i64) -> Option<i32> {
        let mut found = None;
        self.for_each(|counter| {
            if found.is_none()
                && counter.type_id == type_id
                && counter.registration_id == registration_id
            {
                found = Some(counter.counter_id);
            }
        });
        found
    }

    /// The first allocated counter with this type id.
    pub fn find_by_type_id(&self, type_id: i32) -> Option<CounterDescriptor> {
        let mut found = None;
        self.for_each(|counter| {
            if found.is_none() && counter.type_id == type_id {
                found = Some(counter.clone());
            }
        });
        found
    }

    /// One counter's record, by id.
    ///
    /// **This scans the whole catalogue** — it is [`CountersReader::for_each`]
    /// with a filter, and it builds a [`CounterDescriptor`] for every counter
    /// on the way, label and all. A caller that asks this question in a loop is
    /// asking the wrong one: [`CountersReader::is_active`] answers the same
    /// question about a counter a caller already has the id of, in constant
    /// time.
    ///
    /// `None` when the id is out of range or its slot is not a live counter.
    pub fn get(&self, counter_id: i32) -> Option<CounterDescriptor> {
        let mut found = None;

        self.for_each(|counter| {
            if counter.counter_id == counter_id {
                found = Some(counter.clone());
            }
        });

        found
    }

    /// Whether the counter at `counter_id` is still an allocated counter of
    /// this `type_id` with this `registration_id`.
    ///
    /// The question a client asks about a counter it cached an id for, and the
    /// one the reference asks in constant time
    /// (`aeron_counter_heartbeat_timestamp_is_active`,
    /// `aeron-client/src/main/c/aeron_client_conductor.c:1234-1249`): the state
    /// of the slot the id names, its type, and its registration, and nothing
    /// else. It has to be asked again on every use, because the driver reuses
    /// ids — an id that was ours can be somebody else's counter by now.
    ///
    /// Read with an acquire on the state, which is the order the writer
    /// publishes in (`aeron_counters_manager.c:117-121`): a slot that is not
    /// yet allocated has no type or registration to believe.
    ///
    /// `false` for an id outside the region, a slot that is unused or
    /// reclaimed, and a counter whose type or registration has changed.
    #[must_use]
    pub fn is_active(&self, counter_id: i32, type_id: i32, registration_id: i64) -> bool {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return false;
        }

        let metadata_offset = counter_id as usize * layout::COUNTER_METADATA_LENGTH;
        let value_offset = counter_id as usize * layout::COUNTER_VALUE_LENGTH;

        if self
            .metadata
            .load_i32_acquire(metadata_offset + layout::COUNTER_STATE_OFFSET)
            != Some(layout::COUNTER_STATE_ALLOCATED)
        {
            return false;
        }

        self.metadata
            .load_i32_relaxed(metadata_offset + layout::COUNTER_TYPE_ID_OFFSET)
            == Some(type_id)
            && self
                .values
                .load_i64_acquire(value_offset + layout::COUNTER_REGISTRATION_ID_OFFSET)
                == Some(registration_id)
    }

    /// The current value of one counter, by id.
    pub fn value(&self, counter_id: i32) -> Option<i64> {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return None;
        }

        let offset = counter_id as usize * layout::COUNTER_VALUE_LENGTH;
        self.values
            .load_i64_acquire(offset + layout::COUNTER_VALUE_OFFSET)
    }

    /// The raw key of one counter, at its full width.
    ///
    /// The key has no length field: the reference hands every reader
    /// `sizeof(record->key)` bytes and lets the counter's type decide how many
    /// of them mean anything (`aeron_counters_manager.c:309`, and the layout
    /// structs at `aeron_counters_manager.h:37-70` for what each type means).
    /// The client's own heartbeat lookup compares the first eight bytes — the
    /// registration id — to its client id (`aeron_client_conductor.c:1208-1232`).
    ///
    /// A fixed-width array rather than a slice because there is nothing to
    /// trim: a caller that wants eight bytes takes eight, and nothing allocates
    /// on the way.
    pub fn key(&self, counter_id: i32) -> Option<[u8; layout::COUNTER_KEY_LENGTH]> {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return None;
        }

        let offset = counter_id as usize * layout::COUNTER_METADATA_LENGTH;
        let mut out = [0u8; layout::COUNTER_KEY_LENGTH];
        self.metadata
            .copy_out(offset + layout::COUNTER_KEY_OFFSET, &mut out)?;
        Some(out)
    }

    /// Read one metadata record into a descriptor.
    ///
    /// `label_length` is read with an acquire *after* the state was observed
    /// as `ALLOCATED`, which is the order the writer publishes them in
    /// (`aeron_counters_manager.c:117-121`): the label bytes are copied
    /// before the length, and the length before the state.
    fn describe(&self, counter_id: i32, metadata_offset: usize) -> Option<CounterDescriptor> {
        let type_id = self
            .metadata
            .load_i32_relaxed(metadata_offset + layout::COUNTER_TYPE_ID_OFFSET)?;
        let label_length = self
            .metadata
            .load_i32_acquire(metadata_offset + layout::COUNTER_LABEL_LENGTH_OFFSET)?;

        // Clamped, deliberately. The reference hands the raw length to its
        // callback (`aeron_counters_manager.c:308`), which is a latent overread
        // if a writer ever published a longer one. Clamping changes no bytes
        // and cannot lose anything the writer meant to publish, because the
        // field it describes is only 380 bytes wide.
        let label_length = label_length.clamp(0, layout::COUNTER_LABEL_LENGTH_MAX as i32) as usize;
        let mut label = vec![0u8; label_length];
        self.metadata
            .copy_out(metadata_offset + layout::COUNTER_LABEL_OFFSET, &mut label)?;

        let value_offset = counter_id as usize * layout::COUNTER_VALUE_LENGTH;

        Some(CounterDescriptor {
            counter_id,
            type_id,
            value: self
                .values
                .load_i64_acquire(value_offset + layout::COUNTER_VALUE_OFFSET)?,
            registration_id: self
                .values
                .load_i64_acquire(value_offset + layout::COUNTER_REGISTRATION_ID_OFFSET)?,
            owner_id: self
                .values
                .load_i64_relaxed(value_offset + layout::COUNTER_OWNER_ID_OFFSET)?,
            reference_id: self
                .values
                .load_i64_relaxed(value_offset + layout::COUNTER_REFERENCE_ID_OFFSET)?,
            label: String::from_utf8_lossy(&label).into_owned(),
        })
    }
}

/// `AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID`
/// (`aeron-client/src/main/c/aeron_counters.h:98`).
///
/// One per client, allocated when the driver first sees that client's
/// `client_id`, with the client id as its registration id. A client writes it
/// to stay alive: the driver reaps a client — and destroys every subscription
/// it owns — once the counter's age exceeds `aeron.client.liveness.timeout`
/// (`aeron-driver/src/main/c/aeron_driver_conductor.c:1038-1055`).
pub const CLIENT_HEARTBEAT_TYPE_ID: i32 = 11;

/// The write half, available only on counters built from writable memory.
impl<'a> CountersReader<'a, ReadWrite> {
    /// Write a counter's value.
    ///
    /// Release, matching `aeron_counter_set_release`: the driver reads these
    /// with an acquire when it decides whether a client is still alive.
    ///
    /// A client writes its heartbeat counter directly rather than sending a
    /// `CLIENT_KEEPALIVE` command — that is what the reference does, in
    /// `aeron_client_conductor_check_liveness`
    /// (`aeron-client/src/main/c/aeron_client_conductor.c:1305-1375`; the write
    /// is the `aeron_counter_set_release` at `:1373`), and the driver would
    /// ignore the command anyway for a client it has not yet seen
    /// (`aeron_driver_conductor.c:5271-5278`).
    pub fn set_value(&self, counter_id: i32, value: i64) -> Option<()> {
        if counter_id < 0 || counter_id > self.max_counter_id {
            return None;
        }

        let offset = counter_id as usize * layout::COUNTER_VALUE_LENGTH;
        self.values
            .store_i64_release(offset + layout::COUNTER_VALUE_OFFSET, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An aligned region the tests can write into, standing in for a mapped
    /// one. `AtomicBuffer` cannot read a `Vec<u8>` because of alignment, so
    /// the storage has to be declared aligned.
    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn buffer(&self) -> AtomicBuffer<'_> {
            AtomicBuffer::from_slice(&self.0).expect("aligned region")
        }

        fn put_i32(&mut self, offset: usize, v: i32) {
            self.0[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
        }

        fn put_i64(&mut self, offset: usize, v: i64) {
            self.0[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
        }

        fn put_bytes(&mut self, offset: usize, bytes: &[u8]) {
            self.0[offset..offset + bytes.len()].copy_from_slice(bytes);
        }
    }

    /// Write one allocated metadata record at `id`.
    fn allocate(metadata: &mut Region, id: i32, type_id: i32, label: &str) {
        let base = id as usize * layout::COUNTER_METADATA_LENGTH;
        metadata.put_i32(base + layout::COUNTER_TYPE_ID_OFFSET, type_id);
        metadata.put_bytes(base + layout::COUNTER_LABEL_OFFSET, label.as_bytes());
        metadata.put_i32(
            base + layout::COUNTER_LABEL_LENGTH_OFFSET,
            label.len() as i32,
        );
        // Published last, with release, as the reference does.
        metadata.put_i32(
            base + layout::COUNTER_STATE_OFFSET,
            layout::COUNTER_STATE_ALLOCATED,
        );
    }

    fn reader_pair<'a>(metadata: &'a Region, values: &'a Region) -> CountersReader<'a> {
        CountersReader::new(metadata.buffer(), values.buffer())
    }

    /// Write a counter's registration id, which is what ties it to its owner.
    fn set_registration(values: &mut Region, id: i32, registration_id: i64) {
        let offset =
            id as usize * layout::COUNTER_VALUE_LENGTH + layout::COUNTER_REGISTRATION_ID_OFFSET;
        values.put_i64(offset, registration_id);
    }

    /// Write a value record at `id`, through the same arithmetic the reader
    /// uses — spelled with a runtime `id` so it stays a real computation.
    fn set_value(values: &mut Region, id: i32, value: i64) {
        let offset = id as usize * layout::COUNTER_VALUE_LENGTH + layout::COUNTER_VALUE_OFFSET;
        values.put_i64(offset, value);
    }

    #[test]
    fn reports_nothing_for_an_empty_region() {
        let metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);
        let reader = reader_pair(&metadata, &values);

        assert_eq!(3, reader.max_counter_id());
        let mut seen = 0;
        let scan = reader.for_each(|_| seen += 1);
        assert_eq!(
            CounterScan::default(),
            scan,
            "state 0 stops the scan at once"
        );
        assert_eq!(0, seen);
    }

    #[test]
    fn enumerates_allocated_counters_in_id_order() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(&mut metadata, 0, 34, "aeron version");
        allocate(&mut metadata, 1, 101, "pub pos");
        set_value(&mut values, 0, 79_106);
        set_value(&mut values, 1, 512);

        let reader = reader_pair(&metadata, &values);
        let mut seen: Vec<(i32, i32, i64, String)> = Vec::new();
        let scan = reader.for_each(|c| {
            seen.push((c.counter_id, c.type_id, c.value, c.label.clone()));
        });

        assert_eq!(2, scan.allocated);
        assert_eq!(0, scan.reclaimed);
        assert_eq!(
            vec![
                (0, 34, 79_106, "aeron version".to_string()),
                (1, 101, 512, "pub pos".to_string()),
            ],
            seen
        );
    }

    #[test]
    fn stops_at_the_first_unused_record() {
        // Counters are dense from zero, so an unused record ends the list even
        // if a higher one looks allocated.
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(&mut metadata, 0, 34, "first");
        allocate(&mut metadata, 2, 101, "third, past the gap");

        let reader = reader_pair(&metadata, &values);
        let mut seen = 0;
        let scan = reader.for_each(|_| seen += 1);

        assert_eq!(1, seen, "the gap at id 1 ends the scan");
        assert_eq!(1, scan.allocated);
    }

    #[test]
    fn steps_over_reclaimed_records_without_reading_their_key() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(&mut metadata, 0, 34, "live");
        // Record 1's state field: the stride is 512 and the field sits at 0.
        metadata.put_i32(
            layout::COUNTER_METADATA_LENGTH,
            layout::COUNTER_STATE_RECLAIMED,
        );
        allocate(&mut metadata, 2, 101, "live too");

        let reader = reader_pair(&metadata, &values);
        let scan = reader.for_each(|_| {});

        assert_eq!(2, scan.allocated);
        assert_eq!(1, scan.reclaimed, "reclaimed is counted, not reported");
        assert_eq!(0, scan.unknown_state);
    }

    #[test]
    fn counts_a_state_it_does_not_know_instead_of_stopping() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(&mut metadata, 0, 34, "live");
        // Record 1's state field: the stride is 512 and the field sits at 0.
        metadata.put_i32(layout::COUNTER_METADATA_LENGTH, 7);
        allocate(&mut metadata, 2, 101, "after the unknown");

        let reader = reader_pair(&metadata, &values);
        let scan = reader.for_each(|_| {});

        // ADR-0003: skip and count, never panic. A future driver may add a
        // state, and that must not take this reader down.
        assert_eq!(1, scan.unknown_state);
        assert_eq!(2, scan.allocated);
    }

    #[test]
    fn clamps_a_label_length_that_would_overrun() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(&mut metadata, 0, 34, "short");
        // A hostile or corrupt length, well past the 380-byte field.
        metadata.put_i32(layout::COUNTER_LABEL_LENGTH_OFFSET, i32::MAX);

        let reader = reader_pair(&metadata, &values);
        let mut labels = Vec::new();
        reader.for_each(|c| labels.push(c.label.len()));

        assert_eq!(1, labels.len());
        assert_eq!(
            layout::COUNTER_LABEL_LENGTH_MAX,
            labels[0],
            "clamped to the field width, losing nothing the writer published"
        );
    }

    #[test]
    fn finds_a_counter_by_type_id_and_reads_its_value() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(&mut metadata, 0, 34, "aeron version");
        set_value(&mut values, 0, 79_106);

        let reader = reader_pair(&metadata, &values);
        let found = reader.find_by_type_id(34).expect("counter 34 is allocated");

        assert_eq!(0, found.counter_id);
        assert_eq!(79_106, found.value);
        assert_eq!(Some(79_106), reader.value(0));
        assert_eq!(None, reader.find_by_type_id(999));
        assert_eq!(None, reader.value(-1));
        assert_eq!(None, reader.value(99), "past max_counter_id");
    }

    #[test]
    fn a_counter_is_active_while_it_is_the_one_the_caller_means() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        // The client heartbeat counter as the driver allocates it: type 11, the
        // client's id as its registration (`clients.rs::get_or_add`).
        allocate(
            &mut metadata,
            1,
            CLIENT_HEARTBEAT_TYPE_ID,
            "client-heartbeat: id=7",
        );
        set_registration(&mut values, 1, 7);

        let reader = reader_pair(&metadata, &values);

        assert!(reader.is_active(1, CLIENT_HEARTBEAT_TYPE_ID, 7));
        assert!(
            !reader.is_active(1, CLIENT_HEARTBEAT_TYPE_ID, 8),
            "another client's id is not this client's counter"
        );
        assert!(
            !reader.is_active(1, 34, 7),
            "a counter of another type is not the heartbeat"
        );
        assert!(
            !reader.is_active(0, CLIENT_HEARTBEAT_TYPE_ID, 7),
            "a slot nobody allocated is nobody's counter"
        );
        assert!(!reader.is_active(-1, CLIENT_HEARTBEAT_TYPE_ID, 7));
        assert!(
            !reader.is_active(99, CLIENT_HEARTBEAT_TYPE_ID, 7),
            "past max_counter_id"
        );
    }

    #[test]
    fn a_reclaimed_counter_is_not_active_however_its_id_is_asked_about() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        allocate(
            &mut metadata,
            1,
            CLIENT_HEARTBEAT_TYPE_ID,
            "client-heartbeat: id=7",
        );
        set_registration(&mut values, 1, 7);
        metadata.put_i32(
            layout::COUNTER_METADATA_LENGTH + layout::COUNTER_STATE_OFFSET,
            layout::COUNTER_STATE_RECLAIMED,
        );

        let reader = reader_pair(&metadata, &values);

        // The id and the registration are still in the file — a reclaimed slot
        // keeps its bytes — and this is exactly the case the check exists for:
        // the driver hands the id to somebody else.
        assert!(!reader.is_active(1, CLIENT_HEARTBEAT_TYPE_ID, 7));
    }

    #[test]
    fn asking_by_id_agrees_with_the_scan_it_replaces() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        // Contiguous, because that is the state the two views agree in — see
        // the test below for where they do not.
        allocate(&mut metadata, 0, 34, "aeron version");
        set_value(&mut values, 0, 79_106);
        allocate(
            &mut metadata,
            1,
            CLIENT_HEARTBEAT_TYPE_ID,
            "client-heartbeat: id=7",
        );
        set_registration(&mut values, 1, 7);

        let reader = reader_pair(&metadata, &values);

        for counter_id in -1..=3 {
            let by_scan = reader.get(counter_id).is_some_and(|counter| {
                counter.type_id == CLIENT_HEARTBEAT_TYPE_ID && counter.registration_id == 7
            });

            assert_eq!(
                by_scan,
                reader.is_active(counter_id, CLIENT_HEARTBEAT_TYPE_ID, 7),
                "counter {counter_id}"
            );
        }
    }

    #[test]
    fn a_gap_stops_the_scan_and_not_the_question() {
        let mut metadata = Region::zeroed(4 * layout::COUNTER_METADATA_LENGTH);
        let mut values = Region::zeroed(4 * layout::COUNTER_VALUE_LENGTH);

        // Slot 1 is never allocated, and 2 is — which cannot happen in a real
        // file, where allocation is a watermark. It is here because the two
        // readers disagree about it and the disagreement is the reference's
        // own: enumeration stops at the first unused record
        // (`aeron_counters_manager.c:284-321`), while the by-id check reads the
        // slot the caller names (`aeron_client_conductor.c:1234-1249`). A
        // client's heartbeat check has an id already and must not be at the
        // mercy of a gap below it.
        allocate(
            &mut metadata,
            2,
            CLIENT_HEARTBEAT_TYPE_ID,
            "client-heartbeat: id=7",
        );
        set_registration(&mut values, 2, 7);

        let reader = reader_pair(&metadata, &values);

        assert!(reader.get(2).is_none(), "the scan stops at the gap");
        assert!(reader.is_active(2, CLIENT_HEARTBEAT_TYPE_ID, 7));
    }
}
