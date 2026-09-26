//! P0 acceptance: what a reader that asserts against the reference driver can
//! actually prove.
//!
//! Run with `cargo test -p deepmsg-tests --features interop` (or `just
//! interop`), with the Aeron 1.53.2 checkout present — `docs/reference.md`
//! describes the layout and `DEEPMSG_REF_AERONMD` overrides the path.
//!
//! When no driver can be found the tests print a `SKIPPED` line and pass. That
//! is a deliberate choice and it has a cost: a green run of this file may have
//! proved nothing. `tests/integration/cnc_fixture.rs` is the part that runs in
//! CI, and it is the reason the checks here can stay honest about being
//! opt-in.

use std::time::{SystemTime, UNIX_EPOCH};

use deepmsg_cnc::layout;
use deepmsg_cnc::{CNC_FILE_NAME, CncFile};
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};

/// The *slot* the driver puts its own version in:
/// `AERON_SYSTEM_COUNTER_ID_AERON_VERSION`, from
/// `aeron-client/src/main/c/aeron_counters.h:55`.
const VERSION_COUNTER_ID: i32 = 34;

/// The *kind* every system counter carries: `AERON_COUNTER_SYSTEM_COUNTER_TYPE_ID`,
/// from `aeron-client/src/main/c/aeron_counters.h:69`.
///
/// The two constants are adjacent in the header and mean different things —
/// the id is the slot, the type is the kind — and conflating them is the
/// mistake that made the first run of this test find nothing.
const SYSTEM_COUNTER_TYPE_ID: i32 = 0;

/// What the driver stores in that counter: `aeron_semantic_version_compose` of
/// its own major/minor/patch (`aeron-driver/src/main/c/aeron_driver.c:936-937`),
/// i.e. `1.53.2` packed the same way the CnC version is.
const VERSION_COUNTER_VALUE: i64 = ((1 << 16) | (53 << 8) | 2) as i64;

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch");

    // A millisecond timestamp fits in an i64 for the next few hundred million
    // years. Spelled as a statement rather than as a tail expression because
    // an attribute on an expression is still unstable.
    #[allow(clippy::cast_possible_truncation)]
    let millis = elapsed.as_millis() as i64;
    millis
}

/// Start a driver and wait for its CnC file, or report a skip.
fn start(test_name: &str) -> Option<(ReferenceDriver, CncFile)> {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return None;
    };

    let mut reference =
        ReferenceDriver::start(&binary, test_name).expect("start the reference driver");
    let cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");

    Some((reference, cnc))
}

#[test]
fn cnc_metadata_matches_the_reference_drivers_defaults() {
    let Some((reference, cnc)) = start("metadata") else {
        return;
    };

    // The whole point of the exercise: 0.2.0 packed as
    // (major << 16) | (minor << 8) | patch, which reads as 512 -- not as a
    // (major, minor) pair, and not as 0.2.
    assert_eq!(512, cnc.cnc_version());

    let metadata = cnc.metadata();
    assert_eq!(
        1024 * 1024 + layout::MPSC_RB_TRAILER_LENGTH as i32,
        metadata.to_driver_buffer_length
    );
    assert_eq!(
        1024 * 1024 + layout::BROADCAST_TRAILER_LENGTH as i32,
        metadata.to_clients_buffer_length
    );
    assert_eq!(8 * 1024 * 1024, metadata.counter_values_buffer_length);
    assert_eq!(
        4 * metadata.counter_values_buffer_length,
        metadata.counter_metadata_buffer_length,
        "the reference fixes the 4:1 ratio"
    );
    assert_eq!(4 * 1024 * 1024, metadata.error_log_buffer_length);
    assert_eq!(10_000_000_000, metadata.client_liveness_timeout_ns);
    assert_eq!(4096, metadata.file_page_size);

    // Two oracles that a self-consistently wrong reader cannot both satisfy:
    // the pid is the process we spawned...
    assert_eq!(
        i64::from(reference.pid()),
        metadata.pid,
        "the pid in the file must be the driver we started"
    );
    // ...and the start timestamp is roughly now.
    let age = now_ms() - metadata.start_timestamp_ms;
    assert!(
        (0..60_000).contains(&age),
        "start_timestamp_ms is {age}ms away from now"
    );
}

#[test]
fn regions_cover_the_file_exactly() {
    let Some((_reference, cnc)) = start("regions") else {
        return;
    };

    let metadata = cnc.metadata();
    let sum = layout::VERSION_AND_METADATA_LENGTH
        + metadata.to_driver_buffer_length as usize
        + metadata.to_clients_buffer_length as usize
        + metadata.counter_metadata_buffer_length as usize
        + metadata.counter_values_buffer_length as usize
        + metadata.error_log_buffer_length as usize;
    let aligned = layout::align_up(sum, metadata.file_page_size as usize);

    assert_eq!(
        aligned,
        cnc.file_length(),
        "the writer aligns the total to its page size and nothing else"
    );
    // The last region ends where the regions end, which is *before* the page
    // padding the writer adds to the total. Asserting those two are equal is
    // the mistake this comment exists to prevent.
    assert_eq!(
        aligned,
        layout::align_up(cnc.layout().error_log.end, metadata.file_page_size as usize),
        "the file's length is the regions' end rounded up to the page size"
    );

    // And the regions are contiguous, which is what makes the offsets
    // arithmetic rather than a table.
    assert_eq!(cnc.layout().to_driver.end, cnc.layout().to_clients.start);
    assert_eq!(
        cnc.layout().counters_metadata.end,
        cnc.layout().counters_values.start
    );
}

#[test]
fn a_counter_the_driver_wrote_matches_a_number_computed_only_from_the_reference_source() {
    let Some((_reference, cnc)) = start("counters") else {
        return;
    };

    let counters = cnc.counters().expect("counter regions are reachable");

    let mut version = None;
    let mut seen = 0;
    let scan = counters.for_each(|counter| {
        seen += 1;
        if counter.counter_id == VERSION_COUNTER_ID {
            version = Some(counter.clone());
        }
    });

    // A fresh driver allocates its whole system counter catalogue during
    // startup, before it publishes the CnC version, so a scan that stopped
    // early would be a reader bug rather than a timing race.
    assert!(
        seen > 20,
        "expected a full system counter catalogue, saw {seen}"
    );

    let version = version
        .unwrap_or_else(|| panic!("system counter {VERSION_COUNTER_ID} is allocated at startup"));

    assert_eq!(
        SYSTEM_COUNTER_TYPE_ID, version.type_id,
        "every system counter carries type 0, whichever slot it sits in"
    );

    // No part of this number came from the driver: the slot is from the
    // reference header, and the value is composed by our own
    // `semantic_version_compose`. That it matches is the strongest single
    // statement these tests make.
    assert_eq!(
        VERSION_COUNTER_VALUE, version.value,
        "counter {VERSION_COUNTER_ID} should read 1.53.2 packed, label {:?}",
        version.label
    );

    // The driver sets a system counter's registration id to its own slot
    // (`aeron-driver/src/main/c/aeron_system_counters.c:106`), which is a
    // second field of the file we never told it.
    assert_eq!(i64::from(VERSION_COUNTER_ID), version.registration_id);
    assert_eq!(
        0, scan.unknown_state,
        "no counter should be in an unknown state"
    );
}

#[test]
fn the_driver_heartbeat_is_fresh() {
    let Some((_reference, cnc)) = start("heartbeat") else {
        return;
    };

    let heartbeat = cnc
        .consumer_heartbeat_ms()
        .expect("the to-driver ring trailer is readable");

    assert_ne!(
        layout::NULL_VALUE,
        heartbeat,
        "a running driver must not look deliberately stopped"
    );

    let timeout_ms = cnc.metadata().client_liveness_timeout_ns / 1_000_000;
    assert!(
        cnc.driver_is_active(now_ms(), timeout_ms),
        "heartbeat {heartbeat} should be within {timeout_ms}ms of now"
    );
}

#[test]
fn a_clean_start_leaves_the_error_log_empty() {
    let Some((_reference, cnc)) = start("error-log") else {
        return;
    };

    let errors = cnc.error_log().expect("the error log region is reachable");
    let mut entries = Vec::new();
    let scan = errors.read(i64::MIN, &mut entries);

    // Honest limitation, stated rather than papered over: nothing a pure CnC
    // reader can do makes a *running* reference driver write a deterministic
    // error-log entry -- `aeron_report_existing_errors` is a rescue path taken
    // by a *starting* driver. So the entry-decoding path is covered by the
    // synthetic tests in `deepmsg-cnc`, and this asserts only the negative.
    // A row in `docs/compat.md` claiming more than this would be a claim
    // without a covering test.
    assert_eq!(0, scan.entries);
    assert_eq!(0, scan.malformed);
    assert!(!scan.truncated);
    assert!(!errors.has_entries());
}

#[test]
fn the_driver_removes_its_directory_when_signalled() {
    let Some((mut reference, cnc)) = start("shutdown") else {
        return;
    };

    let dir = reference.aeron_dir().to_path_buf();
    assert!(dir.join(CNC_FILE_NAME).exists());

    let status = reference.stop().expect("the driver must be stoppable");

    // `aeronmd` does not die *of* the signal: it catches it, records the
    // number, leaves its main loop, cleans up and `return`s that number
    // (`aeron-driver/src/main/c/aeronmd.c:39-42,112-113,165-185`). So this is
    // a normal exit carrying 15, not a process killed by SIGTERM — and a test
    // that asserted `signal() == Some(15)` would be asserting the opposite of
    // what the reference does.
    assert_eq!(
        Some(15),
        status.code(),
        "the exit status is the signal number the handler recorded"
    );

    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(
        None,
        status.signal(),
        "the process exited, it was not killed"
    );

    // `aeron.dir.delete.on.shutdown=true` is what keeps a day of runs from
    // filling /dev/shm, so it is asserted rather than assumed.
    assert!(
        !dir.exists(),
        "the driver should have removed {} on the way out",
        dir.display()
    );

    // The mapping outlives the file. Reading it now is not an error, it is
    // just stale -- which is why `CncFile` documents its validity in terms of
    // the driver instance that created it.
    assert_eq!(512, cnc.cnc_version());
}
