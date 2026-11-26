//! The frozen bytes from the nightly runs, replayed at the stable tier.
//!
//! Each case is paired with the production regression that owns the fix: the
//! bytes here catch a guard being removed from the fuzz side, and the crate
//! test (`crates/*/src/**/tests`) drives the real entry point where it lives.
//!
//! The artifacts themselves are under `fuzz/artifacts/` and are ignored; these
//! eight-byte constants are the durable copy.

use deepmsg_fuzz::{fixed_part_fits, variable_data_fits};

/// Found by `01_channel_uri`: `uri[..at - 1]` cut `é` in half.
#[test]
fn f1_a_multibyte_prefix_without_a_separator_is_refused() {
    assert!(deepmsg_core::uri::ChannelUri::parse("éaeron:ipc").is_err());
}

/// Found by `07_sbe_archive_codecs`: a keep-alive request with only its SBE
/// header. The production guard is `control_adapter::dispatch`'s fixed-part
/// check, and the regression is `a_request_with_only_a_header_is_refused`.
#[test]
fn f2_an_archive_request_with_only_a_header_stays_behind_the_guard() {
    const CRASH: [u8; 8] = [16, 0, 61, 0, 101, 0, 12, 0];

    assert!(!fixed_part_fits(
        &CRASH,
        deepmsg_codec::archive::keep_alive_request_codec::SBE_BLOCK_LENGTH
    ));
}

/// The variable-length half of F2: the fixed block is there, and a `u32`
/// length runs past the message.
#[test]
fn f2_a_variable_length_field_past_the_end_stays_behind_the_guard() {
    let mut payload = vec![16_u8, 0, 58, 0, 101, 0, 12, 0];
    payload.extend_from_slice(&[0_u8; 16]);
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    assert!(!variable_data_fits(&payload, 8 + 16, 3));
}

/// Found by `08_sbe_cluster_codecs`: a backup response with only its SBE
/// header. The production regression is
/// `a_backup_response_with_only_a_header_is_refused`.
#[test]
fn f3_a_backup_response_with_only_a_header_stays_behind_the_guard() {
    const CRASH: [u8; 8] = [60, 0, 78, 0, 111, 0, 0, 0];

    assert!(!fixed_part_fits(
        &CRASH,
        deepmsg_codec::cluster::backup_response_codec::SBE_BLOCK_LENGTH
    ));
}

/// Found by `08_sbe_cluster_codecs` while the second batch's guard was being
/// written: template 43 declared a 49-byte acting block, so the decoder read
/// the `activeMembers` group header at offset 57 in a 39-byte payload. The
/// production regression is
/// `crates/cluster/common/src/sbe.rs::a_group_header_past_the_payload_is_refused`.
#[test]
fn f10_a_cluster_group_header_past_the_end_is_refused() {
    const CRASH: [u8; 39] = [
        49, 0, 43, 0, 111, 0, 17, 0, 0, 0, 255, 255, 0, 0, 0, 247, 0, 0, 0, 0, 0, 247, 254, 255,
        255, 255, 255, 255, 255, 247, 0, 0, 0, 57, 0, 0, 0, 0, 0,
    ];

    assert!(deepmsg_cluster_common::sbe::message_fits(&CRASH).is_err());
}

/// Found by `13_mark_files`: an activity timestamp of `i64::MIN` (the bytes at
/// offset 16) wrapped `now_ms - timestamp` into a fresh reading. The production
/// regressions are `cluster/common/src/mark.rs`'s
/// `a_min_timestamp_on_an_existing_file_is_not_alive` and `archive/src/mark.rs`'s
/// `a_timestamp_of_i64_min_is_not_active`.
#[test]
fn f11_a_min_mark_timestamp_is_not_alive() {
    const CRASH: [u8; 32] = [
        75, 248, 200, 0, 110, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 255, 255,
        255, 255, 255, 255, 255, 255,
    ];

    let path = std::env::temp_dir().join(format!("deepmsg-f11-{}.dat", std::process::id()));
    let mut bytes = vec![0_u8; 8 * 1024];
    bytes[..CRASH.len()].copy_from_slice(&CRASH);
    std::fs::write(&path, &bytes).expect("write");

    assert!(!deepmsg_cluster_common::mark::ClusterMarkFile::is_active(
        &path, 0, 10_000
    ));
    let _ = std::fs::remove_file(&path);
}

/// Found by `03_cnc_rings_and_logs`: a broadcast region whose latest counter
/// is not a position, so the liveness check's `cursor + capacity` overflowed.
/// The production regression is `broadcast.rs`'s
/// `a_latest_counter_that_is_not_a_position_is_not_a_ring`; the whole region
/// the nightly run wrote is kept under `corpus_seed/03_cnc_rings_and_logs/`
/// and replayed here.
#[test]
fn f12_a_broadcast_region_without_a_position_reads_as_nothing() {
    const BROADCAST: usize = 4096 + deepmsg_cnc::layout::BROADCAST_TRAILER_LENGTH;

    #[repr(align(64))]
    struct Aligned([u8; BROADCAST]);

    let data = include_bytes!(
        "../corpus_seed/03_cnc_rings_and_logs/crash-3feaa0d2-i64-max-latest-counter"
    );
    let mut bytes = Aligned([0_u8; BROADCAST]);
    let len = data.len().min(BROADCAST);
    bytes.0[..len].copy_from_slice(&data[..len]);

    let region = deepmsg_core::buffer::AtomicBuffer::from_slice(&bytes.0).expect("aligned");
    if let Some(mut receiver) = deepmsg_cnc::broadcast::ToClientsReceiver::new(&region) {
        for _ in 0..16 {
            let _ = receiver.receive(&region);
        }
    }
}

/// Found by `11_cluster_recording_log`: a service snapshot whose id is
/// `i32::MAX`, so the recovery plan's `serviceId + 1` overflowed. The
/// production regression is `recording_log.rs`'s
/// `a_service_id_the_writer_could_not_have_written_is_refused`; the nightly's
/// own bytes are kept under `corpus_seed/11_cluster_recording_log/` and
/// replayed here.
#[test]
fn f13_a_snapshot_slot_no_writer_could_have_written_is_refused() {
    let data =
        include_bytes!("../corpus_seed/11_cluster_recording_log/crash-166e5149-i32-max-service-id");
    let path = std::env::temp_dir().join(format!("deepmsg-f13-{}.log", std::process::id()));
    std::fs::write(&path, data).expect("write");

    let log = deepmsg_cluster::persist::recording_log::RecordingLog::open(&path).expect("the log");
    let service_count = i32::from(data[0] % 8);
    assert!(
        log.create_recovery_plan(|_| None, service_count, -1)
            .is_err(),
        "a service id no writer could have produced"
    );

    let _ = std::fs::remove_file(&path);
}
