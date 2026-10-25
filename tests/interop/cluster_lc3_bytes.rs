//! **P3-7c**: the three byte rulers of LC-3.
//!
//! Every scenario before this one judged a mixed cluster by what a client saw
//! and by counters. This one judges it by **what the members wrote**:
//!
//! * each member's **log recording** — the replicated log, which the leader
//!   decides and the followers copy, so all three must be the same bytes;
//! * each member's **consensus module snapshot**, taken at the same log
//!   position because the SNAPSHOT toggle is a log entry;
//! * each member's **business snapshot**, from the twin business — the
//!   reference's `BasicAuctionClusteredService` on its members and this build's
//!   `basic-auction` on ours, which is the same state machine written twice.
//!
//! # The control comes first
//!
//! Each case runs beside an **all-reference** cluster that goes through exactly
//! the same steps. If the rulers are red there, they are measuring something
//! that is not a difference between implementations — a recording's per-member
//! metadata, a leader id, an ordering — and every red on the mixed side would
//! be unreadable. This is the plan's §3.4 precondition 3, and it is why the
//! control is a case of its own rather than an afterthought.
//!
//! # What is compared, and what is not
//!
//! **Payload bytes only.** Recording ids, segment sizes and term buffer shapes
//! belong to each member's archive and differ by construction; the plan's §3.4
//! says so, and the comparison here never looks at them.
//!
//! # The preconditions the plan fixed
//!
//! * `aeron.cluster.timer.service.supplier` is written down on both sides
//!   (`cluster_mixed::options`), because the snapshot's timer records are
//!   written in the container's iteration order (plan §7.4).
//! * The snapshot is taken through the **SNAPSHOT admin request**, so that
//!   every member reaches the same log position rather than each taking one at
//!   a time of its choosing.
//!
//! # One cluster at a time
//!
//! [`SERIAL`] holds the cases apart and the binary runs with
//! `--test-threads=1`; the port block is this binary's own (19xxx).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Instant;

use deepmsg_tests::cluster::{MemberSpec, ProcessMode, TestCluster};
use deepmsg_tests::cluster_bytes::{
    MemberRecordings, Recording, assert_same_bytes, read_recording, recordings_of,
};
use deepmsg_tests::cluster_client::Session;
use deepmsg_tests::cluster_mixed::{self, DEADLINE, Leader, MEMBERS, Topology, connect, node_logs};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

/// The reference's auction business, as its container loads it by name
/// (`ClusterAuctionServiceProbe.java`'s inner class).
const JAVA_AUCTION: &str = "ClusterAuctionServiceProbe$BasicAuctionClusteredService";

/// Our twin's name in this build's registry.
const RUST_AUCTION: &str = "basic-auction";

/// The service id every member of these clusters runs.
const SERVICE_ID: i32 = 0;

/// This binary's port block.
fn next_port_base() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(19_000);
    NEXT.fetch_add(1_000, Ordering::Relaxed)
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The member table: **the twin business on both sides**, one member per node.
///
/// A mixed cluster is only a statement about the protocol if the two sides run
/// the same business: two implementations of different state machines would
/// disagree for reasons that are not the cluster's. `basic-auction` is the
/// reference's `BasicAuctionClusteredService` written twice
/// (`crates/cluster/service/src/services/basic_auction.rs`).
fn twin_members(topology: Topology, mode: ProcessMode) -> Vec<MemberSpec> {
    (0..MEMBERS)
        .map(|id| {
            if topology.rust_members().contains(&id) {
                MemberSpec::rust_node(id, RUST_AUCTION, mode)
            } else {
                // A Java container is always a process of its own.
                MemberSpec::java_container(id, JAVA_AUCTION)
            }
        })
        .collect()
}

/// The all-reference control's table: the same business on every member.
fn reference_members() -> Vec<MemberSpec> {
    (0..MEMBERS)
        .map(|id| MemberSpec::java_container(id, JAVA_AUCTION))
        .collect()
}

/// Five bids: `correlationId @0`, `customerId @8`, `price @16` — the layout the
/// auction business defines (`config/samples/.../auction/README.md`, and
/// `basic_auction.rs`'s module note).
fn bid(session: &mut Session, correlation_id: i64, customer_id: i64, price: i64) {
    let mut payload = [0_u8; 24];
    payload[0..8].copy_from_slice(&correlation_id.to_le_bytes());
    payload[8..16].copy_from_slice(&customer_id.to_le_bytes());
    payload[16..24].copy_from_slice(&price.to_le_bytes());
    let deadline = Instant::now() + DEADLINE;
    assert!(
        session.offer(&payload, deadline),
        "the session never took bid {correlation_id}"
    );
}

/// One member's three recordings, or a panic naming what is missing.
fn recordings(cluster: &TestCluster, member: i32) -> MemberRecordings {
    let found = recordings_of(cluster.cluster_dir(member), SERVICE_ID);
    assert!(
        found.log.is_some(),
        "member {member}'s recording.log names no log recording\n{}",
        node_logs(cluster)
    );
    assert!(
        found.module_snapshot.is_some(),
        "member {member} has no consensus module snapshot\n{}",
        node_logs(cluster)
    );
    assert!(
        found.service_snapshot.is_some(),
        "member {member} has no business snapshot\n{}",
        node_logs(cluster)
    );
    found
}

/// Assert every member's `pick` names a recording, and that all of them are at
/// the **same log position** — the whole premise of comparing them.
fn same_log_position(cluster: &TestCluster, what: &str, picks: &[(i32, Recording)]) -> i64 {
    let first = picks[0].1;
    for (member, recording) in picks {
        assert_eq!(
            recording.log_position,
            first.log_position,
            "{what}: member {member} took its snapshot at log position {} and member {} at {} — \
             the SNAPSHOT toggle is one log entry, so they cannot differ\n{}",
            recording.log_position,
            picks[0].0,
            first.log_position,
            node_logs(cluster)
        );
        assert_eq!(
            recording.leadership_term_id, first.leadership_term_id,
            "{what}: member {member} took its snapshot in term {} and member {} in {}",
            recording.leadership_term_id, picks[0].0, first.leadership_term_id
        );
    }
    first.log_position
}

/// Run one case: the twin business everywhere, bids, a snapshot through the
/// admin request, then the three rulers.
fn the_three_rulers(name: &str, members: &[MemberSpec], appointed: i32) {
    let base = next_port_base();
    let mut settings = cluster_mixed::options(base);
    // The Java container is a probe class (`ClusterServiceContainerProbe`),
    // and it loads the business by name from the same compiled directory.
    settings.probes = vec![
        "ClusterServiceContainerProbe.java",
        "ClusterAuctionServiceProbe.java",
    ];
    let settings = cluster_mixed::allowing_every_command(cluster_mixed::options_with(
        settings,
        Leader::Everyone(appointed),
    ));
    let Some((mut cluster, leader)) = cluster_mixed::start(name, members, settings) else {
        cluster_mixed::skip(name);
        return;
    };
    assert_eq!(
        leader,
        appointed,
        "{name}: the appointed member must lead\n{}",
        node_logs(&cluster)
    );

    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    for (index, (customer, price)) in [(1_i64, 100_i64), (2, 200), (3, 150), (1, 250), (2, 120)]
        .into_iter()
        .enumerate()
    {
        bid(
            &mut session,
            i64::try_from(index).expect("a small index"),
            customer,
            price,
        );
    }

    // The business must have seen the bids: a snapshot of a business nothing
    // ever reached would compare equal and prove nothing (an instrument that
    // cannot fail is not an instrument).
    let bids_landed = session.poll_until(Instant::now() + DEADLINE, |session| {
        session.messages().len() >= 5
    });
    assert!(
        bids_landed,
        "{name}: the auction answered {} of five bids\n{}",
        session.messages().len(),
        node_logs(&cluster)
    );

    // The toggle is what puts every member's snapshot at one log position.
    cluster_mixed::take_a_snapshot(&cluster, &mut session, &[0, 1, 2]);

    let found: Vec<MemberRecordings> = (0..MEMBERS)
        .map(|member| recordings(&cluster, member))
        .collect();

    // (1) The log recording: the bytes the leader decided and the followers
    // copied. Replayed to the **committed** length, because a live log
    // recording has no stop position to replay to.
    let committed = cluster_mixed::commit_position(&cluster, leader)
        .unwrap_or_else(|| panic!("{name}: the leader publishes no commit position"));
    let logs: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|member| {
            read_recording(
                cluster.aeron_dir(member),
                found[usize::try_from(member).expect("a member id")]
                    .log
                    .expect("checked above")
                    .id,
                committed,
            )
        })
        .collect();
    // A ruler that compares two empty vectors would pass and prove nothing.
    assert!(
        !logs[0].is_empty(),
        "{name}: the leader's log recording replayed to nothing — the ruler is not measuring \
         anything\n{}",
        node_logs(&cluster)
    );
    for member in 1..MEMBERS {
        assert_same_bytes(
            &format!("{name}: member {member}'s log recording"),
            &logs[0],
            &logs[usize::try_from(member).expect("a member id")],
        );
    }

    // (2) The consensus module's snapshot, at one log position.
    let module_picks: Vec<(i32, Recording)> = (0..MEMBERS)
        .map(|member| {
            (
                member,
                found[usize::try_from(member).expect("a member id")]
                    .module_snapshot
                    .expect("checked above"),
            )
        })
        .collect();
    same_log_position(
        &cluster,
        &format!("{name}: the module snapshots"),
        &module_picks,
    );
    let modules: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|member| {
            read_recording(
                cluster.aeron_dir(member),
                found[usize::try_from(member).expect("a member id")]
                    .module_snapshot
                    .expect("checked above")
                    .id,
                -2,
            )
        })
        .collect();
    assert!(
        !modules[0].is_empty(),
        "{name}: the module snapshot replayed to nothing\n{}",
        node_logs(&cluster)
    );
    if std::env::var("LC3_DUMP").is_ok() {
        eprintln!(
            "{name}: roles {:?} (LEADER=2), the appointed member is {appointed}",
            cluster_mixed::roles(&cluster)
        );
        // The authoritative decode: this build's own tool, on each member's
        // directory, prints the session fields the snapshot adapter reads.
        for member in 0..MEMBERS {
            let output = std::process::Command::new("../target/debug/deepmsg-cluster-tool")
                .arg(cluster.cluster_dir(member))
                .arg("describe-latest-cm-snapshot")
                .output();
            if let Ok(output) = output {
                let text = String::from_utf8_lossy(&output.stdout);
                for line in text.lines() {
                    if line.contains("openedLogPosition") || line.contains("clusterSessionId") {
                        eprintln!("{name}: member {member} tool: {line}");
                    }
                }
            }
        }
        for (member, found) in found.iter().enumerate() {
            eprintln!("{name}: member {member} recordings: {found:?}");
        }
        for (member, bytes) in modules.iter().enumerate() {
            eprintln!(
                "{name}: member {member} module snapshot ({} bytes): {bytes:02x?}",
                bytes.len()
            );
        }
    }
    for member in 1..MEMBERS {
        assert_same_bytes(
            &format!("{name}: member {member}'s consensus module snapshot"),
            &modules[0],
            &modules[usize::try_from(member).expect("a member id")],
        );
    }

    // (3) The business snapshots — the twin on both sides.
    let service_picks: Vec<(i32, Recording)> = (0..MEMBERS)
        .map(|member| {
            (
                member,
                found[usize::try_from(member).expect("a member id")]
                    .service_snapshot
                    .expect("checked above"),
            )
        })
        .collect();
    same_log_position(
        &cluster,
        &format!("{name}: the business snapshots"),
        &service_picks,
    );
    let services: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|member| {
            read_recording(
                cluster.aeron_dir(member),
                found[usize::try_from(member).expect("a member id")]
                    .service_snapshot
                    .expect("checked above")
                    .id,
                -2,
            )
        })
        .collect();
    assert!(
        !services[0].is_empty(),
        "{name}: the business snapshot replayed to nothing\n{}",
        node_logs(&cluster)
    );
    for member in 1..MEMBERS {
        assert_same_bytes(
            &format!("{name}: member {member}'s business snapshot"),
            &services[0],
            &services[usize::try_from(member).expect("a member id")],
        );
    }

    cluster.shutdown();
}

/// **The control**: three reference members, the same steps, the same three
/// rulers. Red here means the rulers are wrong, not the mixed cluster.
#[test]
fn the_rulers_hold_on_an_all_reference_cluster() {
    let _serial = serial();
    the_three_rulers("bytes-control", &reference_members(), 0);
}

/// Our member leads, the reference's follow.
#[test]
fn the_rulers_hold_with_ours_leading_and_the_references_following() {
    let _serial = serial();
    let mode = ProcessMode::InProcess;
    the_three_rulers(
        "bytes-ours-lead",
        &twin_members(Topology::OneRustTwoJava, mode),
        0,
    );
}

/// The reference leads, ours follows.
///
/// **This is the case that found the divergence.** Before the fix, the two
/// implementations wrote different `ClusterSession.openedLogPosition` values —
/// 96 on our follower, 256 everywhere else — because our replay path handed the
/// session the frame's own start where the reference hands it the frame after
/// (`aeron-client/.../logbuffer/Header.java:240-248`). See
/// `analysis/cluster/deepmsg-p3-7-plan-mixed-membership.md` §12.
#[test]
fn the_rulers_hold_with_the_references_leading_and_ours_following() {
    let _serial = serial();
    let mode = ProcessMode::InProcess;
    the_three_rulers(
        "bytes-theirs-lead",
        &twin_members(Topology::OneRustTwoJava, mode),
        1,
    );
}

/// Two of ours, one of the reference's, and ours leads.
///
/// The second case that found the divergence; see
/// [`the_rulers_hold_with_the_references_leading_and_ours_following`].
#[test]
fn the_rulers_hold_in_two_of_ours_one_of_theirs() {
    let _serial = serial();
    let mode = ProcessMode::InProcess;
    the_three_rulers(
        "bytes-two-ours",
        &twin_members(Topology::TwoRustOneJava, mode),
        0,
    );
}

/// And the other startup path for our members: the `deepmsg-cluster-node`
/// binary rather than a thread.
#[test]
fn the_rulers_hold_with_our_members_as_binaries() {
    let _serial = serial();
    let mode = ProcessMode::External;
    the_three_rulers(
        "bytes-as-binaries",
        &twin_members(Topology::OneRustTwoJava, mode),
        0,
    );
}
