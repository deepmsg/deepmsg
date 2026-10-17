//! Opaque handles the election stores but never inspects.
//!
//! Each of these is a real type in a later slice — `LogReplay` is `P3-4d`,
//! `RecordingReplication` is `P3-6b`. The election only holds them across duty
//! cycles and polls them through the host, so the seam needs a name now and a
//! body later. **Replacing a placeholder with the real type is not a change to
//! the frozen trait**: the trait returns these by value and never names their
//! fields.

/// A log replay in progress (reference `LogReplay.java`). Created by
/// `ElectionHost::new_log_replay` (`ConsensusModuleAgent.java:1840`), polled
/// through `ElectionHost::poll_archive_events` and the replay poll at
/// `ConsensusModuleAgent.java:1845`.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogReplay;

/// An archive-to-archive replication of a log (reference
/// `RecordingReplication.java`). Created by `ElectionHost::new_log_replication`
/// (`ConsensusModuleAgent.java:2951`), advanced by `Election::on_recording_signal`
/// (`Election.java:269-282`).
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordingReplication;
