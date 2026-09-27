//! The forty-six counters a driver publishes before it does anything else.
//!
//! Every counter in this file is allocated at conductor init, in table order,
//! with **id equal to its index** — the reference checks that
//! (`aeron-driver/src/main/c/aeron_system_counters.c:82-117`: it fails init if
//! the manager hands back any other id), which means the system counters must be
//! the first thing allocated on a fresh CnC file and nothing may get in front of
//! them.
//!
//! # What is in the bytes
//!
//! All of them carry type id **0**, a four-byte little-endian **key** that is
//! the index, `registration_id` equal to the index and `owner_id` equal to
//! `NULL_VALUE` (`-1`). The `AeronStat` against the reference driver shows the
//! value record as `registration == index`, which is where that column comes
//! from: `aeron_system_counters_init` writes it (`:110-111`).
//!
//! # Labels that name a build
//!
//! Two labels embed who wrote the file — `Errors: version=… commit=…` (id 15)
//! and `Aeron software: version=… commit=…` (id 34)
//! (`aeron_system_counters.c:41,60`). The reference writes its own
//! `AERON_VERSION_TXT` and git sha; this build writes the Aeron version whose
//! contracts it implements and its own identity
//! ([`deepmsg_core::version::BUILD_IDENTITY`]). The literals below are checked
//! against those constants by a test, and `docs/compat.md` records the
//! divergence.
//!
//! # Labels the conductor appends to at runtime
//!
//! The reference adds a suffix to eight labels right after allocating them
//! (`aeron-driver/src/main/c/aeron_driver_conductor.c:848-951`): the resolver's
//! name, the driver's threading mode and the duty-cycle thresholds. This build
//! appends to three — the conductor's own two and the resolver's empty name —
//! and leaves the sender, receiver and name-resolver counters at their base
//! labels. Those agents do not exist here yet (P1-4); a suffix naming a duty
//! cycle for something that never runs would describe nothing, and the hex
//! digits it changed are not worth that.

use deepmsg_cnc::layout;
use deepmsg_cnc::{CounterManager, CounterRegions};
use deepmsg_core::version;

/// Counter ids this driver writes to while it works.
///
/// The full table is the reference's `aeron_system_counters.h:23-75`; these are
/// the ones with a writer in this build, plus the two the conductor caches an
/// address for.
pub mod id {
    /// `AERON_SYSTEM_COUNTER_ID_ERRORS` — incremented when the driver records
    /// an error.
    pub const ERRORS: i32 = 15;
    /// `AERON_SYSTEM_COUNTER_ID_UNBLOCKED_COMMANDS`.
    pub const UNBLOCKED_COMMANDS: i32 = 20;
    /// `AERON_SYSTEM_COUNTER_ID_CLIENT_TIMEOUTS` — incremented once per client
    /// reaped for silence, never for one that closed itself.
    pub const CLIENT_TIMEOUTS: i32 = 24;
    /// `AERON_SYSTEM_COUNTER_ID_CONDUCTOR_MAX_CYCLE_TIME`.
    pub const CONDUCTOR_MAX_CYCLE_TIME: i32 = 26;
    /// `AERON_SYSTEM_COUNTER_ID_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED`.
    pub const CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED: i32 = 27;
    /// `AERON_SYSTEM_COUNTER_ID_BYTES_CURRENTLY_MAPPED`.
    pub const BYTES_CURRENTLY_MAPPED: i32 = 35;
    /// `AERON_SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION`.
    pub const CONTROL_PROTOCOL_VERSION: i32 = 43;
}

/// How many system counters the reference allocates
/// (`AERON_SYSTEM_COUNTER_DUMMY_LAST`, `aeron_system_counters.h:73`).
pub const COUNT: usize = 46;

/// The conductor's duty-cycle threshold, from the reference's default
/// (`aeron-driver/src/main/c/aeron_driver_context.c:213`,
/// `AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_NS_DEFAULT` = 100 ms).
pub const CONDUCTOR_CYCLE_THRESHOLD_NS: i64 = 100_000_000;

/// The table, in id order (`aeron_system_counters.c:24-71`), verbatim.
///
/// Entries 15 and 34 are the two that carry a build identity; the test
/// `the_two_build_identity_labels_name_this_build` holds them to the constants
/// they are built from.
const LABELS: [&str; COUNT] = [
    "Bytes sent",
    "Bytes received",
    "Failed offers to ReceiverProxy",
    "Failed offers to SenderProxy",
    "Failed offers to DriverConductorProxy",
    "NAKs sent",
    "NAKs received",
    "Status Messages sent",
    "Status Messages received",
    "Heartbeats sent",
    "Heartbeats received",
    "Retransmits sent",
    "Flow control under runs",
    "Flow control over runs",
    "Invalid packets",
    "Errors: version=1.53.2 commit=deepmsg-0.1.0",
    "Short sends",
    "Failed attempts to free log buffers",
    "Sender flow control limits, i.e. back-pressure events",
    "Unblocked Publications",
    "Unblocked Control Commands",
    "Possible TTL Asymmetry",
    "ControllableIdleStrategy status",
    "Loss gap fills",
    "Client liveness timeouts",
    "Resolution changes",
    "Conductor max cycle time doing its work in ns",
    "Conductor work cycle exceeded threshold count",
    "Sender max cycle time doing its work in ns",
    "Sender work cycle exceeded threshold count",
    "Receiver max cycle time doing its work in ns",
    "Receiver work cycle exceeded threshold count",
    "NameResolver max time in ns",
    "NameResolver exceeded threshold count",
    "Aeron software: version=1.53.2 commit=deepmsg-0.1.0",
    "Bytes currently mapped",
    "Retransmitted bytes",
    "Retransmit Pool Overflow count",
    "Error Frames received",
    "Error Frames sent",
    "Publications Revoked",
    "Publication Images Revoked",
    "Images rejected",
    "Control protocol version",
    "Status Messages rejected",
    "Failed offers to NativeResourceAgentProxy",
];

/// The runtime suffixes this build appends, and to which counters.
///
/// The reference's list is longer (`aeron_driver_conductor.c:848-951`) — see
/// the module docs for why the sender, receiver and name-resolver counters are
/// left alone here. `INVOKER` is this driver's threading mode in the
/// reference's vocabulary (`aeron_driver.c:495-503`): one thread invokes every
/// agent, which is what `deepmsg-driver` does today.
const RUNTIME_SUFFIXES: [(i32, &str); 3] = [
    (25, ": driverName="),
    (id::CONDUCTOR_MAX_CYCLE_TIME, ": INVOKER"),
    (
        id::CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED,
        ": threshold=100ms INVOKER",
    ),
];

/// Why the system counters could not be allocated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SystemCounterError {
    /// The manager did not hand out the id the table expected.
    ///
    /// The reference fails init on this (`aeron_system_counters.c:99-107`) and
    /// so does this: the ids are the contract `AeronStat` reads, so a file
    /// where counter 24 is something else is not a file this driver should
    /// publish.
    IdOutOfSequence {
        /// The index in the table.
        expected: i32,
        /// What the manager returned instead.
        got: Option<i32>,
    },
    /// A region could not hold the record — a CnC file too small for its own
    /// counters, which only a broken layout produces.
    RegionTooSmall {
        /// The id whose record did not fit.
        counter_id: i32,
    },
    /// The initial value of a counter could not be written.
    ValueNotWritten {
        /// The id whose value did not fit.
        counter_id: i32,
    },
}

impl std::fmt::Display for SystemCounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdOutOfSequence { expected, got } => match got {
                Some(got) => write!(f, "system counter {expected} was not free: got {got}"),
                None => write!(f, "system counter {expected} could not be allocated"),
            },
            Self::RegionTooSmall { counter_id } => {
                write!(f, "the region is too small for system counter {counter_id}")
            }
            Self::ValueNotWritten { counter_id } => {
                write!(
                    f,
                    "the value of system counter {counter_id} was not written"
                )
            }
        }
    }
}

impl std::error::Error for SystemCounterError {}

/// Allocate the forty-six system counters and publish their initial values.
///
/// `bytes_mapped` is the size of what the driver has mapped, which is counter
/// 35's value: the reference reports its CnC mapping plus its loss report
/// (`aeron-driver.c:947-949`), and this build maps only the CnC file, so the
/// number is the file's length. That is a runtime value rather than a byte
/// contract, and it is the honest one for a driver with no loss report yet.
///
/// # Errors
///
/// [`SystemCounterError`] — see its variants. Every one of them means the CnC
/// file must not be published.
pub fn allocate_all(
    manager: &mut CounterManager,
    regions: &CounterRegions<'_>,
    now_ms: i64,
    bytes_mapped: i64,
) -> Result<(), SystemCounterError> {
    for (index, label) in LABELS.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)] // COUNT ids, far below i32::MAX
        let expected = index as i32;
        let key = expected.to_le_bytes();

        let got = manager.allocate(regions, 0, &key, label.as_bytes(), now_ms);
        if got != Some(expected) {
            return Err(SystemCounterError::IdOutOfSequence { expected, got });
        }

        manager
            .set_registration_id(regions, expected, i64::from(expected))
            .ok_or(SystemCounterError::RegionTooSmall {
                counter_id: expected,
            })?;
        manager
            .set_owner_id(regions, expected, layout::NULL_VALUE)
            .ok_or(SystemCounterError::RegionTooSmall {
                counter_id: expected,
            })?;
    }

    for (counter_id, suffix) in RUNTIME_SUFFIXES {
        manager
            .append_to_label(regions, counter_id, suffix.as_bytes())
            .ok_or(SystemCounterError::RegionTooSmall { counter_id })?;
    }

    // The three values the reference sets once its agents exist
    // (`aeron-driver.c:936-949`); this driver has one agent and sets them at the
    // same point in startup.
    let aeron_version = version::semantic_version_compose(1, 53, 2);
    let control_protocol_version = version::semantic_version_compose(1, 0, 0);
    for (counter_id, value) in [
        (34, i64::from(aeron_version)),
        (
            id::CONTROL_PROTOCOL_VERSION,
            i64::from(control_protocol_version),
        ),
        (id::BYTES_CURRENTLY_MAPPED, bytes_mapped),
    ] {
        manager
            .set_value(regions, counter_id, value)
            .ok_or(SystemCounterError::ValueNotWritten { counter_id })?;
    }

    Ok(())
}

/// Add one to a counter, the way the reference's `aeron_counter_increment_release`
/// does (`concurrent/aeron_counters_manager.h:198-204`): read, add, store with a
/// release.
///
/// Not a fetch-add, deliberately. These counters have exactly one writer — the
/// conductor — and the reference's "release" variant is a plain load and store
/// with an ordering annotation, unlike its `aeron_counter_increment`, which is
/// a real get-and-add. Copying the stronger primitive would work and would
/// misdescribe the writer.
pub fn increment(
    manager: &CounterManager,
    regions: &CounterRegions<'_>,
    counter_id: i32,
) -> Option<i64> {
    let value = manager.value(regions, counter_id)?;
    manager.set_value(regions, counter_id, value + 1)?;
    Some(value)
}

/// Store `candidate` if it is greater than what is there
/// (`aeron_counter_propose_max_release`, `concurrent/aeron_counters_manager.h:244-258`).
///
/// How the duty-cycle trackers keep a high-water mark without reading it twice.
pub fn propose_max(
    manager: &CounterManager,
    regions: &CounterRegions<'_>,
    counter_id: i32,
    candidate: i64,
) -> Option<()> {
    let current = manager.value(regions, counter_id)?;
    if candidate > current {
        manager.set_value(regions, counter_id, candidate)
    } else {
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_cnc::{CounterDescriptor, layout};
    use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

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

    /// A CnC-sized pair of counter regions for a test driver, and the manager
    /// over them.
    struct Fixture {
        metadata: Region,
        values: Region,
    }

    const VALUES_LENGTH: usize = 1024 * 1024;

    impl Fixture {
        fn new() -> Self {
            Self {
                metadata: Region::zeroed(VALUES_LENGTH * 4),
                values: Region::zeroed(VALUES_LENGTH),
            }
        }

        fn open(&mut self) -> (CounterManager, CounterRegions<'_>) {
            let regions = CounterRegions::new(self.metadata.writable(), self.values.writable())
                .expect("four-to-one");
            let manager = CounterManager::new(VALUES_LENGTH, 1_000).expect("room for counters");
            (manager, regions)
        }
    }

    /// One counter's descriptor, as a client would decode it.
    ///
    /// Found by type and registration because that is the pair a system
    /// counter is identified by — type 0, registration equal to its index.
    fn counter(regions: &CounterRegions<'_>, counter_id: i32) -> CounterDescriptor {
        let mut found = None;
        regions.reader().for_each(|descriptor| {
            if found.is_none() && descriptor.counter_id == counter_id {
                found = Some(descriptor.clone());
            }
        });

        found.unwrap_or_else(|| panic!("system counter {counter_id} is allocated"))
    }

    fn label(regions: &CounterRegions<'_>, counter_id: i32) -> String {
        counter(regions, counter_id).label
    }

    fn key(regions: &CounterRegions<'_>, counter_id: i32) -> [u8; 4] {
        let full = regions
            .reader()
            .key(counter_id)
            .unwrap_or_else(|| panic!("system counter {counter_id} has a key"));
        let mut first = [0u8; 4];
        first.copy_from_slice(&full[..4]);
        first
    }

    fn value(regions: &CounterRegions<'_>, counter_id: i32) -> Option<i64> {
        regions.reader().value(counter_id)
    }

    #[test]
    fn every_counter_lands_on_its_own_index_with_its_own_key() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        allocate_all(&mut manager, &regions, 5, 48_238_592).expect("a fresh file");

        for index in 0..COUNT as i32 {
            let descriptor = counter(&regions, index);
            assert_eq!(index, descriptor.counter_id);
            assert_eq!(0, descriptor.type_id);
            assert_eq!(i64::from(index), descriptor.registration_id);
            assert_eq!(layout::NULL_VALUE, descriptor.owner_id);
            assert_eq!(index.to_le_bytes(), key(&regions, index), "key {index}");
        }

        assert_eq!(46, COUNT);
        assert_eq!(46, manager.id_high_water_mark() + 1);
    }

    #[test]
    fn the_labels_are_the_references_plus_this_builds_runtime_suffixes() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        allocate_all(&mut manager, &regions, 0, 0).expect("a fresh file");

        assert_eq!("Bytes sent", label(&regions, 0));
        assert_eq!("Client liveness timeouts", label(&regions, 24));
        assert_eq!(
            "Failed offers to NativeResourceAgentProxy",
            label(&regions, 45)
        );

        // The three this build appends to.
        assert_eq!("Resolution changes: driverName=", label(&regions, 25));
        assert_eq!(
            "Conductor max cycle time doing its work in ns: INVOKER",
            label(&regions, 26)
        );
        assert_eq!(
            "Conductor work cycle exceeded threshold count: threshold=100ms INVOKER",
            label(&regions, 27)
        );

        // And the ones it leaves alone, which the reference suffixes.
        assert_eq!(
            "Sender max cycle time doing its work in ns",
            label(&regions, 28)
        );
        assert_eq!(
            "Receiver max cycle time doing its work in ns",
            label(&regions, 30)
        );
        assert_eq!("NameResolver exceeded threshold count", label(&regions, 33));
    }

    #[test]
    fn the_two_build_identity_labels_name_this_build() {
        let expected = format!(
            "version={} commit={}",
            version::COMPAT_VERSION_TEXT,
            version::BUILD_IDENTITY
        );

        assert_eq!(format!("Errors: {expected}"), LABELS[id::ERRORS as usize]);
        assert_eq!(format!("Aeron software: {expected}"), LABELS[34]);
    }

    #[test]
    fn the_initial_values_are_the_versions_and_the_mapped_bytes() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        allocate_all(&mut manager, &regions, 0, 48_238_592).expect("a fresh file");

        assert_eq!(
            Some(79_106),
            value(&regions, 34),
            "1.53.2 packed is the Aeron software counter"
        );
        assert_eq!(
            Some(65_536),
            value(&regions, id::CONTROL_PROTOCOL_VERSION),
            "control protocol 1.0.0 packed"
        );
        assert_eq!(
            Some(48_238_592),
            value(&regions, id::BYTES_CURRENTLY_MAPPED)
        );
    }

    #[test]
    fn counters_that_are_not_system_counters_are_refused_by_the_id_check() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        // Something got there first, which is what a managed-resource counter
        // allocated before init would look like.
        manager
            .allocate(&regions, 11, &[], b"someone else's", 0)
            .expect("an id");

        assert_eq!(
            Err(SystemCounterError::IdOutOfSequence {
                expected: 0,
                got: Some(1),
            }),
            allocate_all(&mut manager, &regions, 0, 0)
        );
    }

    #[test]
    fn increment_reads_adds_and_stores() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        allocate_all(&mut manager, &regions, 0, 0).expect("a fresh file");

        assert_eq!(
            Some(0),
            increment(&manager, &regions, id::CLIENT_TIMEOUTS),
            "the value it held before, as the reference returns"
        );
        assert_eq!(Some(1), increment(&manager, &regions, id::CLIENT_TIMEOUTS));
        assert_eq!(Some(2), value(&regions, id::CLIENT_TIMEOUTS));
        assert_eq!(None, increment(&manager, &regions, 65_535 + 1));
    }

    #[test]
    fn propose_max_only_ever_moves_up() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        allocate_all(&mut manager, &regions, 0, 0).expect("a fresh file");

        assert_eq!(Some(()), propose_max(&manager, &regions, 26, 500));
        assert_eq!(Some(500), value(&regions, 26));
        assert_eq!(Some(()), propose_max(&manager, &regions, 26, 100));
        assert_eq!(Some(500), value(&regions, 26), "a smaller one is ignored");
        assert_eq!(Some(()), propose_max(&manager, &regions, 26, 900));
        assert_eq!(Some(900), value(&regions, 26));
    }
}
