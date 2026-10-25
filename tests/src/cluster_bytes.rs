//! P3-7c's rulers: what a cluster member wrote, read back as **bytes**.
//!
//! Three recordings per member, all named by that member's own `recording.log`
//! (`RecordingLog`, which this build and the reference both write and both
//! read — P3-1's golden):
//!
//! * the **log recording** of its last term — the replicated log, whose bytes
//!   the leader decides and the followers copy;
//! * the **consensus module's snapshot** (`entry_type` SNAPSHOT,
//!   `service_id` [`CONSENSUS_MODULE_SERVICE_ID`]);
//! * a **business's snapshot** (`entry_type` SNAPSHOT, that service id).
//!
//! Each is replayed out of the member's *own* archive and compared as payload
//! bytes. The archive's metadata — recording ids, segment sizes, term buffer
//! shapes — is per member and is **not** compared: it is the archive's, not the
//! cluster's (plan §3.4).
//!
//! # Why the recording log, and not the recovery-state counter
//!
//! LC-4a's twin comparison reads the snapshot recording ids out of a
//! **recovery-state counter**, which exists only between a module's startup and
//! its services' acks — so it stops the node, restarts it without containers,
//! reads, and starts them again. A `recording.log` entry carries the same id
//! and is readable at any time, from any implementation, without disturbing the
//! cluster. `ClusterTool describe-latest-cm-snapshot` finds its recording the
//! same way (`operator.rs:1238-1253`).

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_cluster::persist::recording_log::{
    self, CONSENSUS_MODULE_SERVICE_ID, ENTRY_TYPE_SNAPSHOT, RecordingLog,
};

/// One recording, as the member's `recording.log` names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recording {
    /// The archive's id for it, in this member's archive.
    pub id: i64,
    /// The log position the snapshot (or term) covers.
    pub log_position: i64,
    /// The leadership term it was taken in.
    pub leadership_term_id: i64,
    /// The log position the leadership term starts at. A term's log recording
    /// begins at this position, so the bytes to replay for a log position `p`
    /// are `p - term_base_log_position`.
    pub term_base_log_position: i64,
}

/// The three recordings one member's `recording.log` names.
#[derive(Clone, Copy, Debug)]
pub struct MemberRecordings {
    /// The log recording of the member's last term.
    pub log: Option<Recording>,
    /// The consensus module's latest valid snapshot.
    pub module_snapshot: Option<Recording>,
    /// `service_id`'s latest valid snapshot.
    pub service_snapshot: Option<Recording>,
}

/// Read a member's `recording.log` and answer the recordings it names.
#[must_use]
pub fn recordings_of(cluster_dir: &Path, service_id: i32) -> MemberRecordings {
    let path = cluster_dir.join(recording_log::FILENAME);
    let Ok(log) = RecordingLog::open(&path) else {
        return MemberRecordings {
            log: None,
            module_snapshot: None,
            service_snapshot: None,
        };
    };

    let latest_snapshot = |service: i32| {
        log.entries()
            .iter()
            .rev()
            .find(|entry| {
                entry.is_valid
                    && entry.entry_type == ENTRY_TYPE_SNAPSHOT
                    && entry.service_id == service
            })
            .map(|entry| Recording {
                id: entry.recording_id,
                log_position: entry.log_position,
                leadership_term_id: entry.leadership_term_id,
                term_base_log_position: entry.term_base_log_position,
            })
    };

    let log_recording = log.find_last_term().map(|term| Recording {
        id: term.recording_id,
        log_position: term.log_position,
        leadership_term_id: term.leadership_term_id,
        term_base_log_position: term.term_base_log_position,
    });

    MemberRecordings {
        log: log_recording,
        module_snapshot: latest_snapshot(CONSENSUS_MODULE_SERVICE_ID),
        service_snapshot: latest_snapshot(service_id),
    }
}

/// Replay one recording out of a member's archive and answer its payload bytes.
///
/// `length` is the replay protocol's own: **-2** runs to the recording's stop
/// position, which is what a snapshot recording has (it is auto-stopped when
/// the snapshot finishes); a **positive** value is that many bytes, which is
/// what a log recording that is still growing needs — it has no stop, so a
/// replay to the end would never end.
///
/// # Panics
///
/// When the archive cannot be reached, the recording cannot be replayed, or the
/// replay never reaches its end.
#[must_use]
pub fn read_recording(aeron_dir: &Path, recording_id: i64, length: i64) -> Vec<u8> {
    let mut client = Client::connect(aeron_dir).expect("a client onto the member's driver");

    let mut context = ArchiveContext::new();
    context.control_request_channel = Some("aeron:ipc?term-length=64k".to_owned());
    context.control_response_channel = Some("aeron:ipc?term-length=64k".to_owned());
    context.control_request_stream_id = 10;
    context.control_response_stream_id = 9_000;
    let mut archive = Archive::connect(
        &context,
        &mut client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the archive connects");

    let replay_channel = "aeron:ipc";
    let replay_stream_id = 103;
    let subscription_id = archive
        .replay(
            &mut client,
            recording_id,
            replay_channel,
            replay_stream_id,
            &deepmsg_archive::client::proxy::ReplayParams {
                position: 0,
                length,
                ..deepmsg_archive::client::proxy::ReplayParams::default()
            },
        )
        .expect("the recording replays");

    let deadline = Instant::now() + Duration::from_secs(60);
    let registration_id = loop {
        let image = client
            .subscription(subscription_id)
            .and_then(|subscription| subscription.images().first())
            .map(deepmsg_client::image::Image::registration_id);
        if let Some(registration_id) = image {
            break registration_id;
        }
        assert!(Instant::now() < deadline, "the replay image never arrived");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    };

    let mut bytes = Vec::new();
    loop {
        let image_is_end = client
            .subscription(subscription_id)
            .and_then(|subscription| {
                subscription
                    .images()
                    .iter()
                    .find(|image| image.registration_id() == registration_id)
                    .map(deepmsg_client::image::Image::is_end_of_stream)
            })
            .flatten()
            .unwrap_or(false);

        let read = client.poll_image(subscription_id, registration_id, 100, |fragment| {
            let length = fragment.payload_length();
            if let Some((buffer, offset, _)) = fragment.payload_region() {
                let mut payload = vec![0_u8; length];
                if buffer.copy_out(offset, &mut payload).is_some() {
                    bytes.extend_from_slice(&payload);
                }
            }
        });

        if image_is_end && read.unwrap_or(0) == 0 {
            return bytes;
        }
        assert!(
            Instant::now() < deadline,
            "the replay never reached its end"
        );
    }
}

/// Assert two payloads are the same bytes, naming where they stop being.
///
/// A bare `assert_eq!` on two multi-kilobyte vectors prints both of them; what
/// a failing case needs is the length and the first offset that differs.
///
/// # Panics
///
/// When they differ.
pub fn assert_same_bytes(what: &str, left: &[u8], right: &[u8]) {
    if left == right {
        return;
    }
    let first = left
        .iter()
        .zip(right.iter())
        .position(|(left, right)| left != right);
    let window = |bytes: &[u8], at: usize| {
        let start = at.saturating_sub(8);
        let end = usize::min(bytes.len(), at + 16);
        bytes
            .get(start..end)
            .map(|slice| format!("{slice:02x?}"))
            .unwrap_or_default()
    };
    panic!(
        "{what}: the payloads differ — {} vs {} bytes, first difference at {first:?}\n  \
         left  from {first:?}: {}\n  right from {first:?}: {}",
        left.len(),
        right.len(),
        first.map(|at| window(left, at)).unwrap_or_default(),
        first.map(|at| window(right, at)).unwrap_or_default(),
    );
}
