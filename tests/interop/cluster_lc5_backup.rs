//! P3-7e's backup row: our `ClusterBackup` beside a cluster of **mixed**
//! members.
//!
//! The card's matrix gains a `backup` dimension; the plan does not multiply it
//! into the 24 cells but runs it as one line of its own
//! (`deepmsg-p3-7-plan-mixed-membership.md` §3.7): `{1R2J, 2R1J}`, each with a
//! backup beside it. What that line is worth saying is not "the backup works" —
//! `cluster_backup_java` says that against three Java members and
//! `cluster_backup_cases` says it with both ends ours. What is only true here is
//! that the backup's copy of a log **two implementations are writing** is the
//! same bytes as the members' copy.
//!
//! # The two criteria
//!
//! 1. the backup's live log position reaches the leader's commit position
//!    (`ClusterBackup` counter 209 against the member's counter 30);
//! 2. the backup's log recording and a member's are **the same bytes**, replayed
//!    out of each side's own archive over the same range of log positions.
//!
//! A third assertion rides along and is not a criterion: the backup pulled the
//! cluster's snapshot (counter 240). Without it the cell could be a run in which
//! the snapshot half of the scenario never happened.
//!
//! # Why the range is `position - term_base_log_position`
//!
//! A term's log recording starts at the log position the term starts at
//! (`cluster_bytes::Recording::term_base_log_position`), so the bytes to replay
//! for log position `p` are `p - base`. The two sides are asserted to share
//! `base` before the bytes are compared: a backup whose recording begins
//! somewhere else would otherwise "agree" with a member over a range that is not
//! the same range.
//!
//! # Ports
//!
//! Each case takes a thousand ports from this binary's band
//! ([`bands::LC5_BACKUP`]) — below `ip_local_port_range` (32768
//! here), because a member that loses one of its own endpoints mid-run dies in
//! a way that reads as something else entirely. Three members sit at
//! `base + 0..300`, and the backup's own query publication at `base + 900`
//! (the reference's `cluster_backup::BACKUP_PORT_OFFSET`), which is what the
//! thousand is for.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_tests::cluster::{ProcessMode, bands};
use deepmsg_tests::cluster_backup;
use deepmsg_tests::cluster_bytes::{assert_same_bytes, read_recording, recordings_of};
use deepmsg_tests::cluster_harness::{BackupOptions, CLUSTER_ID};
use deepmsg_tests::cluster_mixed::{self, Leader, MEMBERS, Topology};

/// `test-service`'s id in the fixture's container — the business whose snapshot
/// the third assertion reads.
const SERVICE_ID: i32 = 0;
/// Messages before the snapshot, and after it: the backup has to copy a log that
/// grew around a snapshot, not a log that stopped.
const BEFORE_THE_SNAPSHOT: u64 = 10;
const AFTER_THE_SNAPSHOT: u64 = 7;

/// The tests in this binary each start a whole cluster, a backup and a client.
static SERIAL: Mutex<()> = Mutex::new(());

/// The next cluster's port base, from this binary's band ([`bands::LC5_BACKUP`]).
fn next_port_base() -> u16 {
    bands::LC5_BACKUP.next_base()
}

fn the_backup_rides_beside(topology: Topology) {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = next_port_base();
    let name = format!("lc5-backup-{topology:?}");

    // The backup fixture opens the backup's counters by the harness's cluster id
    // (`NodeCounters`), so the cluster has to carry that id rather than the one
    // the mixed suite's own options name.
    let mut options = cluster_mixed::options(base);
    options.cluster_id = CLUSTER_ID;
    // The admin request has to be allowed by the authorisation service, or the
    // snapshot half of the scenario never happens
    // (`ClusterTest.shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot`).
    let settings = cluster_mixed::options_with(
        cluster_mixed::allowing_every_command(options),
        Leader::Nobody,
    );

    let Some((mut cluster, leader)) =
        cluster_mixed::start(&name, &topology.members(ProcessMode::External), settings)
    else {
        cluster_mixed::skip(&name);
        return;
    };

    let Some(mut session) = cluster_mixed::connect(&name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        cluster.shutdown();
        return;
    };

    cluster_mixed::echo(&mut session, 0, BEFORE_THE_SNAPSHOT);

    // The snapshot half of the scenario, **before** the backup starts: the
    // reference's own case (`shouldBackupClusterWithSnapshot`) takes the
    // snapshot first and the backup catches up over it, which is also the only
    // arrangement in which counter 240 says anything — a backup that is already
    // `BACKING_UP` meets a new snapshot as an ordinary log entry.
    cluster_mixed::take_a_snapshot(&cluster, &mut session, &(0..MEMBERS).collect::<Vec<_>>());
    cluster_mixed::echo(
        &mut session,
        BEFORE_THE_SNAPSHOT,
        BEFORE_THE_SNAPSHOT + AFTER_THE_SNAPSHOT,
    );

    let Some((backup, faults)) = cluster_backup::start_beside(
        base,
        &format!("{name}-backup"),
        MEMBERS,
        BackupOptions::default(),
    ) else {
        cluster_mixed::skip("the backup's driver");
        cluster.shutdown();
        return;
    };

    // A still point: no more client messages, and `position` is the leader's
    // commit position at that moment, so it is an upper bound both sides have
    // to reach and neither can be ahead of.
    let position = cluster_mixed::commit_position(&cluster, leader)
        .expect("the leader publishes its commit position");

    // (1) The backup reaches the cluster's position.
    cluster_backup::await_live(&backup, position);

    // (2) The same bytes over the same range of log positions.
    let member = recordings_of(cluster.cluster_dir(leader), SERVICE_ID)
        .log
        .unwrap_or_else(|| {
            panic!(
                "{name}: the leader's recording.log names no log recording\n{}",
                cluster_mixed::node_logs(&cluster)
            )
        });
    let copy = recordings_of(backup.cluster_dir(), SERVICE_ID)
        .log
        .unwrap_or_else(|| {
            panic!(
                "{name}: the backup's recording.log names no log recording\n\
                 live {:?}, state {:?}, backup errors {:?}",
                backup.live_log_position(),
                backup.state(),
                backup.error_log()
            )
        });

    assert_eq!(
        member.term_base_log_position, copy.term_base_log_position,
        "{name}: the backup's recording starts where the leader's does"
    );

    let member_bytes = read_recording(
        cluster.aeron_dir(leader),
        member.id,
        position - member.term_base_log_position,
    );
    let copy_bytes = read_recording(
        backup.aeron_dir(),
        copy.id,
        position - copy.term_base_log_position,
    );
    assert!(
        !member_bytes.is_empty(),
        "{name}: the leader's log recording replayed no bytes up to {position}"
    );
    assert_same_bytes(
        &format!("{name}: the backup's log recording against the leader's"),
        &copy_bytes,
        &member_bytes,
    );

    // (3) Supporting, not a criterion: the snapshot reached the backup.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && backup.snapshot_retrieve_count().unwrap_or(0) < 1 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        backup.snapshot_retrieve_count().unwrap_or(0) >= 1,
        "{name}: the backup pulled the cluster's snapshot (counter 240)\n\
         state {:?}, live {:?}, backup errors {:?}",
        backup.state(),
        backup.live_log_position(),
        backup.error_log()
    );

    faults.check();
    cluster.shutdown();
}

/// One member of ours, two of the reference's, and a backup beside them.
#[test]
fn a_backup_copies_a_log_mostly_java_members_are_writing() {
    the_backup_rides_beside(Topology::OneRustTwoJava);
}

/// Two of ours, one of the reference's, and a backup beside them.
#[test]
fn a_backup_copies_a_log_mostly_our_members_are_writing() {
    the_backup_rides_beside(Topology::TwoRustOneJava);
}
