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
//! name, the driver's threading mode and the four duty-cycle thresholds. All
//! eight are here, built from [`LabelSuffixes`], because every one of them is a
//! setting a deployment can move: the mode from `aeron.threading.mode` and the
//! thresholds from their own names, which are also what the three
//! `*_CYCLE_TIME_THRESHOLD_EXCEEDED` counters count against. The label and the
//! counting are the same number in the reference, and they are here.

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
    /// `AERON_SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED` — incremented when a
    /// `REMOVE_PUBLICATION` carrying the revoke flag cuts a stream off
    /// (`aeron_counters.h:61`, incremented at
    /// `aeron-driver/src/main/c/aeron_ipc_publication.c:520`).
    pub const PUBLICATIONS_REVOKED: i32 = 40;
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

    // The data plane's own counters (`aeron_system_counters.h:25-45`, in id
    // order). Their labels are in [`super::LABELS`]; these are the ids the
    // sender, the receiver and the publications bump.
    /// `AERON_SYSTEM_COUNTER_ID_BYTES_SENT`.
    pub const BYTES_SENT: i32 = 0;
    /// `AERON_SYSTEM_COUNTER_ID_BYTES_RECEIVED`.
    pub const BYTES_RECEIVED: i32 = 1;
    /// `AERON_SYSTEM_COUNTER_ID_SENDER_PROXY_FAILS`.
    pub const SENDER_PROXY_FAILS: i32 = 3;
    /// `AERON_SYSTEM_COUNTER_ID_RECEIVER_PROXY_FAILS`.
    pub const RECEIVER_PROXY_FAILS: i32 = 2;
    /// `AERON_SYSTEM_COUNTER_ID_NAK_MESSAGES_SENT`.
    pub const NAK_MESSAGES_SENT: i32 = 5;
    /// `AERON_SYSTEM_COUNTER_ID_NAK_MESSAGES_RECEIVED`.
    pub const NAK_MESSAGES_RECEIVED: i32 = 6;
    /// `AERON_SYSTEM_COUNTER_ID_STATUS_MESSAGES_SENT`.
    pub const STATUS_MESSAGES_SENT: i32 = 7;
    /// `AERON_SYSTEM_COUNTER_ID_STATUS_MESSAGES_RECEIVED`.
    pub const STATUS_MESSAGES_RECEIVED: i32 = 8;
    /// `AERON_SYSTEM_COUNTER_ID_HEARTBEATS_SENT`.
    pub const HEARTBEATS_SENT: i32 = 9;
    /// `AERON_SYSTEM_COUNTER_ID_HEARTBEATS_RECEIVED`.
    pub const HEARTBEATS_RECEIVED: i32 = 10;
    /// `AERON_SYSTEM_COUNTER_ID_RETRANSMITS_SENT`.
    pub const RETRANSMITS_SENT: i32 = 11;
    /// `AERON_SYSTEM_COUNTER_ID_FLOW_CONTROL_UNDER_RUNS`.
    pub const FLOW_CONTROL_UNDER_RUNS: i32 = 12;
    /// `AERON_SYSTEM_COUNTER_ID_FLOW_CONTROL_OVER_RUNS`.
    pub const FLOW_CONTROL_OVER_RUNS: i32 = 13;
    /// `AERON_SYSTEM_COUNTER_ID_INVALID_PACKETS`.
    pub const INVALID_PACKETS: i32 = 14;
    /// `AERON_SYSTEM_COUNTER_ID_SHORT_SENDS`.
    pub const SHORT_SENDS: i32 = 16;
    /// `AERON_SYSTEM_COUNTER_ID_SENDER_FLOW_CONTROL_LIMITS`.
    pub const SENDER_FLOW_CONTROL_LIMITS: i32 = 18;
    /// `AERON_SYSTEM_COUNTER_ID_UNBLOCKED_PUBLICATIONS`.
    pub const UNBLOCKED_PUBLICATIONS: i32 = 19;
    /// `AERON_SYSTEM_COUNTER_ID_LOSS_GAP_FILLS`.
    pub const LOSS_GAP_FILLS: i32 = 23;
    /// `AERON_SYSTEM_COUNTER_ID_SENDER_MAX_CYCLE_TIME`.
    pub const SENDER_MAX_CYCLE_TIME: i32 = 28;
    /// `AERON_SYSTEM_COUNTER_ID_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED`.
    pub const SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED: i32 = 29;
    /// `AERON_SYSTEM_COUNTER_ID_RECEIVER_MAX_CYCLE_TIME`.
    pub const RECEIVER_MAX_CYCLE_TIME: i32 = 30;
    /// `AERON_SYSTEM_COUNTER_ID_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED`.
    pub const RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED: i32 = 31;
    /// `AERON_SYSTEM_COUNTER_ID_RESOLUTION_CHANGES` (`aeron_system_counters.h:29`),
    /// which carries the resolver's own name in its label.
    pub const RESOLUTION_CHANGES: i32 = 25;
    /// `AERON_SYSTEM_COUNTER_ID_NAME_RESOLVER_MAX_TIME` (`aeron_system_counters.h:34`):
    /// the longest a single resolution has taken, which is what the driver's
    /// own resolver is measured by (`aeron_driver_native_resource_agent.c:39-60`).
    pub const NAME_RESOLVER_MAX_TIME: i32 = 32;
    /// `AERON_SYSTEM_COUNTER_ID_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED` (`:35`).
    pub const NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED: i32 = 33;
    /// `AERON_SYSTEM_COUNTER_ID_RETRANSMITTED_BYTES`.
    pub const RETRANSMITTED_BYTES: i32 = 36;
    /// `AERON_SYSTEM_COUNTER_ID_RETRANSMIT_OVERFLOW`.
    pub const RETRANSMIT_OVERFLOW: i32 = 37;
    /// `AERON_SYSTEM_COUNTER_ID_ERROR_FRAMES_RECEIVED`.
    pub const ERROR_FRAMES_RECEIVED: i32 = 38;
    /// `AERON_SYSTEM_COUNTER_ID_ERROR_FRAMES_SENT`.
    pub const ERROR_FRAMES_SENT: i32 = 39;
    /// `AERON_SYSTEM_COUNTER_ID_PUBLICATION_IMAGES_REVOKED`.
    pub const PUBLICATION_IMAGES_REVOKED: i32 = 41;
    /// `AERON_SYSTEM_COUNTER_ID_IMAGES_REJECTED`.
    pub const IMAGES_REJECTED: i32 = 42;
    /// `AERON_SYSTEM_COUNTER_ID_STATUS_MESSAGES_REJECTED`.
    pub const STATUS_MESSAGES_REJECTED: i32 = 44;
}

/// How many system counters the reference allocates
/// (`AERON_SYSTEM_COUNTER_DUMMY_LAST`, `aeron_system_counters.h:73`).
pub const COUNT: usize = 46;

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

/// The settings the runtime label suffixes are built from
/// (`aeron-driver/src/main/c/aeron_driver_conductor.c:848-951`).
///
/// The reference reads all of them off its context at one point, right after
/// the counters are allocated; this is that point in this build's terms, so
/// nothing here can be written before the settings are known and nothing after
/// them can read a stale label.
pub struct LabelSuffixes<'a> {
    /// The name the resolver was configured with, or `""` — the reference
    /// prints an empty one rather than skipping the suffix (`:852-856`).
    pub resolver_name: &'a str,
    /// The mode this driver runs in, spelled the reference's way
    /// (`aeron_driver_threading_mode_to_string`, `aeron_driver_context.c:53-61`).
    pub threading_mode: &'a str,
    /// The duty-cycle thresholds of the three threads that have one
    /// (`aeron_driver_context.c:1031-1043`), each printed as a duration.
    pub conductor_cycle_threshold_ns: i64,
    /// The sender's, for counter 29.
    pub sender_cycle_threshold_ns: i64,
    /// The receiver's, for counter 31.
    pub receiver_cycle_threshold_ns: i64,
    /// The resolver's threshold (`:1049-1055`), and the one suffix the
    /// reference prints **without** a threading mode: a resolver runs on the
    /// native resource agent, which has no duty cycle of its own to name
    /// (`:937-951`).
    pub name_resolver_threshold_ns: i64,
}

/// The counters a driver allocated for itself, in the order it allocated them.
///
/// A value rather than a range constant: the release path gives back *what was
/// allocated* rather than what this build happens to allocate, so a count that
/// changes between the two — a slice that adds one, an init that stops halfway
/// — cannot make the shutdown free the wrong slots. It also gives the release
/// something to check: every id it holds was handed out by this process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemCounters {
    ids: Vec<i32>,
}

impl SystemCounters {
    /// The ids, in allocation order.
    pub fn ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.ids.iter().copied()
    }

    /// How many counters the driver owns.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether the driver owns none — only true before init.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Return every one of them to the pool, and say how many went back.
    ///
    /// A count rather than nothing, because the caller is a shutdown: a counter
    /// that would not release is an inconsistency worth surfacing, and a `bool`
    /// per counter is what [`CounterManager::free`] already reports.
    pub fn release_all(
        &self,
        manager: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> usize {
        self.ids
            .iter()
            .filter(|counter_id| manager.free(regions, **counter_id, now_ms))
            .count()
    }
}

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
/// Returns what it allocated, so the shutdown can give back exactly that.
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
    suffixes: &LabelSuffixes<'_>,
) -> Result<SystemCounters, SystemCounterError> {
    let mut allocated = Vec::with_capacity(COUNT);

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

        allocated.push(expected);
    }

    // The eight runtime suffixes, in the reference's own four shapes: the
    // resolver's name, the mode alone, the threshold and the mode, and the
    // resolver's threshold alone. Appending is what the reference does —
    // `aeron_counters_manager_append_to_label` (`:848-951`) — so what a reader
    // sees is the base label above plus this.
    let mode = suffixes.threading_mode;
    let conductor = format_duration_ns(suffixes.conductor_cycle_threshold_ns);
    let sender = format_duration_ns(suffixes.sender_cycle_threshold_ns);
    let receiver = format_duration_ns(suffixes.receiver_cycle_threshold_ns);
    let name_resolver = format_duration_ns(suffixes.name_resolver_threshold_ns);

    for (counter_id, suffix) in [
        (
            id::RESOLUTION_CHANGES,
            format!(": driverName={}", suffixes.resolver_name),
        ),
        (id::CONDUCTOR_MAX_CYCLE_TIME, format!(": {mode}")),
        (id::SENDER_MAX_CYCLE_TIME, format!(": {mode}")),
        (id::RECEIVER_MAX_CYCLE_TIME, format!(": {mode}")),
        (
            id::CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED,
            format!(": threshold={conductor} {mode}"),
        ),
        (
            id::SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            format!(": threshold={sender} {mode}"),
        ),
        (
            id::RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            format!(": threshold={receiver} {mode}"),
        ),
        (
            id::NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED,
            format!(": threshold={name_resolver}"),
        ),
    ] {
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

    Ok(SystemCounters { ids: allocated })
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

/// A duration as the reference writes one into a label
/// (`aeron_format_duration_ns`,
/// `aeron-client/src/main/c/util/aeron_parse_util.c:270-333`): the largest
/// unit that divides it **exactly**, and nanoseconds when none does.
///
/// Exact is the whole of it: five seconds is `5s` and 1500 milliseconds is
/// `1500000000ns`, not `1.5s`.
pub fn format_duration_ns(duration_ns: i64) -> String {
    for (unit_ns, suffix) in [(1_000_000_000, "s"), (1_000_000, "ms"), (1_000, "us")] {
        if duration_ns >= unit_ns && duration_ns % unit_ns == 0 {
            return format!("{}{suffix}", duration_ns / unit_ns);
        }
    }

    format!("{duration_ns}ns")
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

/// The system counters a hot path bumps, with the manager and the regions
/// paired so a call site is one line.
///
/// The counters are shared: every agent writes the same forty-six, and the
/// value a reader sees is a plain atomic add. Nothing here takes `&mut`, which
/// is what lets the conductor, the sender and the receiver hold one at once.
#[derive(Clone, Copy)]
pub struct System<'a> {
    manager: &'a CounterManager,
    regions: &'a CounterRegions<'a>,
}

impl<'a> System<'a> {
    /// A view over the counters a driver allocated at startup.
    pub const fn new(manager: &'a CounterManager, regions: &'a CounterRegions<'a>) -> Self {
        Self { manager, regions }
    }

    /// Add one to a system counter, if the id is one.
    pub fn increment(&self, counter_id: i32) {
        let _ = increment(self.manager, self.regions, counter_id);
    }

    /// Add `value` to a system counter.
    pub fn add(&self, counter_id: i32, value: i64) {
        if let Some(current) = self.manager.value(self.regions, counter_id) {
            let _ = self
                .manager
                .set_value(self.regions, counter_id, current + value);
        }
    }

    /// A system counter's value, or zero for an id that is not one.
    pub fn value(&self, counter_id: i32) -> i64 {
        self.manager.value(self.regions, counter_id).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The threshold the labels below quote, which is the reference's default.
    const NAME_RESOLVER_THRESHOLD: i64 = 5 * 1000 * 1000 * 1000;

    /// The label settings a test driver runs with: no resolver name, the
    /// default mode and the default thresholds.
    fn suffixes<'a>(resolver_name: &'a str, name_resolver_threshold_ns: i64) -> LabelSuffixes<'a> {
        LabelSuffixes {
            resolver_name,
            threading_mode: "DEDICATED",
            conductor_cycle_threshold_ns: crate::config::CYCLE_THRESHOLD_NS_DEFAULT,
            sender_cycle_threshold_ns: crate::config::CYCLE_THRESHOLD_NS_DEFAULT,
            receiver_cycle_threshold_ns: crate::config::CYCLE_THRESHOLD_NS_DEFAULT,
            name_resolver_threshold_ns,
        }
    }
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

        allocate_all(
            &mut manager,
            &regions,
            5,
            48_238_592,
            &suffixes("", NAME_RESOLVER_THRESHOLD),
        )
        .expect("a fresh file");

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
        allocate_all(
            &mut manager,
            &regions,
            0,
            0,
            &suffixes("", NAME_RESOLVER_THRESHOLD),
        )
        .expect("a fresh file");

        assert_eq!("Bytes sent", label(&regions, 0));
        assert_eq!("Client liveness timeouts", label(&regions, 24));
        assert_eq!(
            "Failed offers to NativeResourceAgentProxy",
            label(&regions, 45)
        );

        // Every one of the eight suffixes the reference appends is here
        // (`aeron_driver_conductor.c:848-951`), built from the settings.
        assert_eq!("Resolution changes: driverName=", label(&regions, 25));
        assert_eq!(
            "Conductor max cycle time doing its work in ns: DEDICATED",
            label(&regions, 26)
        );
        assert_eq!(
            "Conductor work cycle exceeded threshold count: threshold=100ms DEDICATED",
            label(&regions, 27)
        );

        // The sender and the receiver say which mode they run in too, as the
        // reference's do (`aeron_driver_conductor.c:867-888`).
        assert_eq!(
            "Sender max cycle time doing its work in ns: DEDICATED",
            label(&regions, 28)
        );
        assert_eq!(
            "Receiver max cycle time doing its work in ns: DEDICATED",
            label(&regions, 30)
        );
        assert_eq!(
            "Sender work cycle exceeded threshold count: threshold=100ms DEDICATED",
            label(&regions, 29)
        );
        assert_eq!(
            "Receiver work cycle exceeded threshold count: threshold=100ms DEDICATED",
            label(&regions, 31)
        );

        // The resolver's pair carries no threading mode — a resolver runs on
        // the native resource agent, which is an `INVOKER`, and the reference
        // appends `DEDICATED` only to the three threads that have one
        // (`aeron_driver_conductor.c:857-935`) — but the threshold it counts
        // past is a value, so it goes in the label
        // (`:938-951`, formatted by `aeron_format_duration_ns`).
        assert_eq!(
            "NameResolver exceeded threshold count: threshold=5s",
            label(&regions, 33)
        );
        assert_eq!("NameResolver max time in ns", label(&regions, 32));

        // And a `Resolution changes` counter names the resolver it counts for,
        // which is how two drivers in one `AeronStat` are told apart
        // (`:848-856`).
        assert_eq!("Resolution changes: driverName=", label(&regions, 25));
    }

    /// The suffixes are the **settings**, not constants: the mode a driver was
    /// configured with, the threshold each slot was configured with, and the
    /// resolver's name.
    ///
    /// The values are the ones `DutyCycleLabelFormatTest` sets, which is what
    /// that oracle asks for word for word: `conductorCycleThresholdNs(2h)`,
    /// `senderCycleThresholdNs(321us)`, `receiverCycleThresholdNs(250ms)`,
    /// `nameResolverThresholdNs(15s)`.
    #[test]
    fn the_runtime_suffixes_follow_the_settings() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();

        allocate_all(
            &mut manager,
            &regions,
            0,
            0,
            &LabelSuffixes {
                resolver_name: "aeron:net-driver",
                threading_mode: "SHARED_NETWORK",
                conductor_cycle_threshold_ns: 2 * 60 * 60 * 1_000_000_000,
                sender_cycle_threshold_ns: 321_000,
                receiver_cycle_threshold_ns: 250_000_000,
                name_resolver_threshold_ns: 15 * 1_000_000_000,
            },
        )
        .expect("a fresh file");

        assert_eq!(
            "Resolution changes: driverName=aeron:net-driver",
            label(&regions, 25)
        );
        assert_eq!(
            "Conductor max cycle time doing its work in ns: SHARED_NETWORK",
            label(&regions, 26)
        );
        assert_eq!(
            "Conductor work cycle exceeded threshold count: threshold=7200s SHARED_NETWORK",
            label(&regions, 27)
        );
        assert_eq!(
            "Sender work cycle exceeded threshold count: threshold=321us SHARED_NETWORK",
            label(&regions, 29)
        );
        assert_eq!(
            "Receiver work cycle exceeded threshold count: threshold=250ms SHARED_NETWORK",
            label(&regions, 31)
        );
        assert_eq!(
            "NameResolver exceeded threshold count: threshold=15s",
            label(&regions, 33)
        );
        assert_eq!("NameResolver max time in ns", label(&regions, 32));
    }

    /// Durations are printed in the largest unit that divides them exactly, and
    /// in nanoseconds when none does — `aeron_format_duration_ns`
    /// (`util/aeron_parse_util.c:270-330`), whose four arms this is.
    #[test]
    fn a_duration_takes_the_largest_unit_that_divides_it() {
        assert_eq!("7200s", format_duration_ns(2 * 60 * 60 * 1_000_000_000));
        assert_eq!("15s", format_duration_ns(15 * 1_000_000_000));
        assert_eq!("250ms", format_duration_ns(250_000_000));
        assert_eq!("100ms", format_duration_ns(100_000_000));
        assert_eq!("321us", format_duration_ns(321_000));
        assert_eq!("5s", format_duration_ns(5_000_000_000));

        // Not a whole millisecond, but a whole microsecond: it falls through
        // one unit at a time rather than printing a fraction, because there is
        // no decimal arm.
        assert_eq!("1500us", format_duration_ns(1_500_000));
        assert_eq!("1234567ns", format_duration_ns(1_234_567));
        assert_eq!("999ns", format_duration_ns(999));
        assert_eq!("0ns", format_duration_ns(0));
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
        allocate_all(
            &mut manager,
            &regions,
            0,
            48_238_592,
            &suffixes("", NAME_RESOLVER_THRESHOLD),
        )
        .expect("a fresh file");

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
            allocate_all(
                &mut manager,
                &regions,
                0,
                0,
                &suffixes("", NAME_RESOLVER_THRESHOLD)
            )
        );
    }

    #[test]
    fn increment_reads_adds_and_stores() {
        let mut fixture = Fixture::new();
        let (mut manager, regions) = fixture.open();
        allocate_all(
            &mut manager,
            &regions,
            0,
            0,
            &suffixes("", NAME_RESOLVER_THRESHOLD),
        )
        .expect("a fresh file");

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
        allocate_all(
            &mut manager,
            &regions,
            0,
            0,
            &suffixes("", NAME_RESOLVER_THRESHOLD),
        )
        .expect("a fresh file");

        assert_eq!(Some(()), propose_max(&manager, &regions, 26, 500));
        assert_eq!(Some(500), value(&regions, 26));
        assert_eq!(Some(()), propose_max(&manager, &regions, 26, 100));
        assert_eq!(Some(500), value(&regions, 26), "a smaller one is ignored");
        assert_eq!(Some(()), propose_max(&manager, &regions, 26, 900));
        assert_eq!(Some(900), value(&regions, 26));
    }
}
