//! Starting a **backup** beside a cluster this module did not start.
//!
//! A backup is not a member: the reference gives it its own driver, its own
//! archive and a `ClusterBackup` agent that dials the cluster's consensus
//! endpoints (`TestCluster.java:549-620` — `index = memberCount`, so it never
//! enters `aeron.cluster.members`). Both of this repository's backup suites need
//! the same twenty lines to build that, and the clusters they sit beside are
//! different kinds: `cluster_backup_java` starts Java members and
//! `cluster_lc5_backup` starts a mixed one. Everything that differs between
//! them is the port block, how many members there are and the driver
//! directory's name, so that is the whole of the parameter list here.
//!
//! # Ports
//!
//! [`BACKUP_PORT_OFFSET`] is the backup's own query publication, inside the
//! caller's [`ClusterPorts`] block — the reference gives it
//! `localhost:<2<cid>22<memberCount>>` for the same reason: an endpoint the
//! cluster's own members do not bind.

use std::time::{Duration, Instant};

use deepmsg_cluster::BackupState;

use crate::cluster::{ClusterPorts, MEMBER_FACING_PORT_OFFSET};
use crate::cluster_harness::{
    BackupNode, BackupOptions, BackupTarget, CLUSTER_ID, DEADLINE, Faults,
};
use crate::temp::TempDir;

/// The offset, inside a [`ClusterPorts`] block, of the endpoint the backup's own
/// query publication binds.
pub const BACKUP_PORT_OFFSET: u16 = 900;

/// Start a backup beside the cluster `base` describes.
///
/// `name` keys the backup's driver directory only; two backups in one process
/// must not share it.
pub fn start_beside(
    base: u16,
    name: &str,
    members: i32,
    options: BackupOptions,
) -> Option<(BackupNode, Faults)> {
    let ports = ClusterPorts::new(base);
    let faults = Faults::default();
    let target = BackupTarget {
        cluster_id: CLUSTER_ID,
        member_string: ports.members_string(members),
        consensus_endpoints: (0..members)
            .map(|member| ports.endpoint(member, MEMBER_FACING_PORT_OFFSET))
            .collect::<Vec<_>>()
            .join(","),
        consensus_endpoint: format!("localhost:{}", base + BACKUP_PORT_OFFSET),
        name: name.to_owned(),
    };

    let backup = BackupNode::start_with_target(
        target,
        &faults,
        options,
        TempDir::new(&format!("{name}-archive")),
        TempDir::new(&format!("{name}-cluster")),
    )?;

    Some((backup, faults))
}

/// The reference's `awaitBackupLiveLogPosition`: `BACKING_UP`, and the live log
/// position the cluster proposed to it has reached `position`.
///
/// # Panics
///
/// When the backup never gets there; the message carries its state, its live
/// position and its own error log, because a backup that stalls is nearly
/// always a backup the cluster refused and that has reset.
pub fn await_live(backup: &BackupNode, position: i64) {
    let deadline = Instant::now() + DEADLINE;

    while Instant::now() < deadline {
        if backup.state() == Some(BackupState::BackingUp.code())
            && backup
                .live_log_position()
                .is_some_and(|live| live >= position)
        {
            return;
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "the backup never reached the cluster's position {position}: state {:?}, live {:?}, \
         errors {:?}",
        backup.state(),
        backup.live_log_position(),
        backup.error_log()
    );
}
