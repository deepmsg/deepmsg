//! **P3-7d**: LC-5's rolling replacement — the same directories, the other
//! implementation.
//!
//! The reference's `ClusterTool` reads our cluster directory
//! (`cluster_tool_reference.rs`), and the reference's test harness can start a
//! member over a backup's directories (`TestCluster.startStaticNodeFromBackup`).
//! What **nobody has run** is the deployment the card is about: a live cluster
//! whose members are replaced one at a time by the other implementation, each
//! starting from the `recording.log`, `node-state.dat`, mark files and archive
//! recordings the previous implementation left in the same directories
//! (plan §3.6).
//!
//! # The shape
//!
//! Three members of the reference's — Java `ClusteredMediaDriver` nodes, each
//! with the reference's `BasicAuctionClusteredService` in a Java container — then
//! `3J → 3R → 3J`: every member is stopped and started again as ours (our driver,
//! `deepmsg-cluster-node`, and our `basic-auction`), and then the whole cycle is
//! run backwards. Only `External` is exercised: rolling replacement's real shape
//! is "stop a process, start a process", and the in-process module has no
//! counterpart to it (plan §3.6).
//!
//! The same steps run once more on a cluster that is the reference's on **both**
//! sides of every replacement ([`the_same_rolling_steps_on_a_cluster_of_the_references`]).
//! That case cannot judge the crossing; it is what tells the fixture and the
//! client's own behaviour apart from a difference between the implementations —
//! the way LC-3's all-reference control does for its byte rulers, and it earned
//! its place the first time it was run: the client-side behaviour this file
//! documents around a leader's replacement reproduced on a cluster with no code
//! of ours in it.
//!
//! The business is the **twin** (`basic-auction` ↔ `BasicAuctionClusteredService`,
//! the same 16-byte snapshot record), so a replacement's business state has to
//! come out of the other implementation's snapshot bytes. That is the strongest
//! thing this card can assert — a different business on the two sides would
//! differ for reasons that are not the cluster's (LC-3's §3.4 precondition).
//!
//! # The four judgements, per step
//!
//! 1. **The cluster keeps committing.** Bids go in after the replacement is
//!    started, and the business answers them.
//! 2. **The replacement came up from the other implementation's state.** Its
//!    `recording.log` still names every entry the stopped implementation wrote —
//!    same leadership terms, same recording ids — which is only true if the
//!    module read and continued that file rather than rebuilding a history of
//!    its own. (A module that cannot parse the file does not start at all; one
//!    that decides the history is unusable rewrites it, and the entries go.)
//! 3. **Its log recording is byte for byte the leader's**, replayed out of its
//!    own archive.
//! 4. **The other implementation's tool reads the directory it left.** At every
//!    stop, the reference's `ClusterTool` is run on ours' directory and this
//!    build's `deepmsg-cluster-tool` on the reference's; `describe` has to find
//!    a `CONSENSUS_MODULE` mark file in both.
//!
//! # What this cannot separate
//!
//! Whether the business **loaded the snapshot** or rebuilt the same state by
//! replaying the log is not judged: the replay starts at the snapshot's log
//! position (so the snapshot is on the path either way), but the log is never
//! truncated in this cluster, so a module that ignored the snapshot could reach
//! the same state by replay and the next snapshot's bytes would not tell. What
//! *is* judged is that the business state after the swap is the same one the
//! other members have, byte for byte (the snapshot comparison at the end of each
//! pass).
//!
//! # The client
//!
//! A member that is replaced while it leads takes the client's session with it:
//! a node that starts up and goes on to lead closes the sessions it did not open
//! (`SessionManager.prepareSessionsForNewTerm(true)`), and a client whose
//! session the cluster closed opens a new one. That is the client's own liveness
//! contract, not the cluster's implementation — the all-reference control
//! behaves the same, in both of the shapes the outage's length can produce (see
//! [`reconnect`] and [`bids`]). What the cases judge is that the traffic goes on
//! and the cluster's state did not depend on the session.
//!
//! # One cluster at a time
//!
//! [`SERIAL`] holds the cases apart and the binary runs with
//! `--test-threads=1`; each case takes a block of its own from this binary's
//! band ([`bands::LC5_ROLLING`]), below
//! `net.ipv4.ip_local_port_range`.

use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use deepmsg_cluster::persist::recording_log::{self, RecordingLog};
use deepmsg_tests::cluster::{MemberSpec, ProcessMode, TestCluster, bands};
use deepmsg_tests::cluster_bytes::{
    MemberRecordings, assert_same_bytes, read_recording, recordings_of,
};
use deepmsg_tests::cluster_client::Session;
use deepmsg_tests::cluster_mixed::{self, DEADLINE, MEMBERS, commit_position, connect, node_logs};
use deepmsg_tests::cluster_tool;
use deepmsg_tests::java;

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

/// The reference's auction business, as its container loads it by name
/// (`ClusterAuctionServiceProbe.java`'s inner class).
const JAVA_AUCTION: &str = "ClusterAuctionServiceProbe$BasicAuctionClusteredService";

/// Our twin's name in this build's registry.
const RUST_AUCTION: &str = "basic-auction";

/// The service id every member of these clusters runs.
const SERVICE_ID: i32 = 0;

/// The next cluster's port base, from this binary's band ([`bands::LC5_ROLLING`]).
fn next_port_base() -> u16 {
    bands::LC5_ROLLING.next_base()
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The reference's implementation of a member: its node and its container.
fn java_spec(member: i32) -> MemberSpec {
    MemberSpec::java_container(member, JAVA_AUCTION)
}

/// Ours: our driver, our module as a process, our twin business as a process.
fn rust_spec(member: i32) -> MemberSpec {
    MemberSpec::rust_node(member, RUST_AUCTION, ProcessMode::External)
}

/// The cluster-wide settings both clusters start from.
///
/// Two of them are not defaults: the probes are the Java container the
/// reference's business runs in (`ClusterServiceContainerProbe`) and the twin
/// business itself, and the authorisation service allows the SNAPSHOT admin
/// request (`ClusterTest.shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot`).
///
/// **No appointment.** A rolling replacement has to reach the leader as well,
/// and a member appointed to somebody else never nominates (`Election.java:715-718`),
/// so an appointed cluster cannot survive its own leader's stop. Without an
/// appointment the leader is whichever member won the election, and a case that
/// replaces it follows a failover — the matrix's kill-leader shape.
fn settings(base: u16) -> deepmsg_tests::cluster::Options {
    let mut options = cluster_mixed::options(base);
    options.probes = vec![
        "ClusterServiceContainerProbe.java",
        "ClusterAuctionServiceProbe.java",
    ];
    cluster_mixed::allowing_every_command(options)
}

/// One bid: `correlationId @0`, `customerId @8`, `price @16`
/// (`basic_auction.rs`'s module note). `false` when the session could not take
/// it, which is the client's cue that it has no leader to publish to.
fn bid(session: &mut Session, correlation_id: i64, customer_id: i64, price: i64) -> bool {
    let mut payload = [0_u8; 24];
    payload[0..8].copy_from_slice(&correlation_id.to_le_bytes());
    payload[8..16].copy_from_slice(&customer_id.to_le_bytes());
    payload[16..24].copy_from_slice(&price.to_le_bytes());
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    session.offer(&payload, deadline)
}

/// `count` bids, and wait until the business has answered every one.
///
/// The egress reply is the evidence the cluster committed them: a bid the
/// cluster did not commit is a bid no business saw.
///
/// **A session the cluster closes is the client's to replace**, and this loop
/// does that: a rolling replacement that takes the leader away closes the
/// sessions that were open before the restarted node started
/// (`SessionManager.prepareSessionsForNewTerm(true)`), and one that arrives in
/// the window between "this node says LEADER" and that call closes the new one
/// too. What it does **not** tolerate is a session that stays open and a cluster
/// that silently drops what it sends — that is a failure, not a reconnect.
fn bids(
    name: &str,
    cluster: &TestCluster,
    base: u16,
    session: &mut Option<Session>,
    from: u64,
    count: u64,
) {
    for attempt in 0..3 {
        let live = session.as_mut().expect("a session");
        let before = live.messages().len();
        let mut sent = true;
        for index in from..from + count {
            #[allow(clippy::cast_possible_wrap)] // test-sized indices
            let id = index as i64;
            sent &= bid(live, id, id % 3 + 1, 100 + id);
            if !sent {
                break;
            }
        }

        if sent {
            let wanted = before + usize::try_from(count).expect("a test-sized count");
            let deadline = Instant::now() + DEADLINE;
            // Stop waiting the moment the cluster closes the session: a closed
            // session's answers are never coming.
            let answered = live.poll_until(deadline, |session| {
                session.messages().len() >= wanted || session.session.is_closed()
            });
            if live.messages().len() >= wanted && answered {
                return;
            }
            assert!(
                live.session.is_closed(),
                "{name}: the business answered {} of the {count} bids the client sent on an open \
                 session (it has {wanted} messages; the session is {:?}, says member {} in term \
                 {}, events {:?}, new leaders {:?}), the roles are {:?}, member terms {:?}, commit \
                 positions {:?}\n{}\n{}",
                live.messages().len() - before,
                live.session.state(),
                live.session.leader_member_id(),
                live.session.leadership_term_id(),
                live.received.borrow().events,
                live.received.borrow().new_leaders,
                cluster_mixed::roles(cluster),
                (0..MEMBERS)
                    .map(|member| cluster_mixed::leadership_term(cluster, member))
                    .collect::<Vec<_>>(),
                (0..MEMBERS)
                    .map(|member| commit_position(cluster, member))
                    .collect::<Vec<_>>(),
                node_logs(cluster),
                cluster.container_logs(),
            );
        }

        eprintln!(
            "{name}: the client's session could not carry the bids (attempt {attempt}: state \
             {:?}, events {:?}); it opens a new one",
            live.session.state(),
            live.received.borrow().events,
        );
        *session = None;
        *session = connect(name, base, cluster);
        assert!(session.is_some(), "{name}: our client's driver is gone");
    }
    panic!("{name}: the client could not carry {count} bids in three sessions");
}

/// Every entry a member's `recording.log` holds, as
/// `(leadership term, recording id, entry type, service id)`.
///
/// The in-place fields — the invalid bit, the commit position — are left out
/// deliberately: a member that comes back behind the leader marks its own
/// uncommitted entry invalid, which is the protocol working, not the history
/// being lost.
fn history_of(cluster_dir: &Path) -> Vec<(i64, i64, i32, i32)> {
    let path = cluster_dir.join(recording_log::FILENAME);
    let Ok(log) = RecordingLog::open(&path) else {
        return Vec::new();
    };
    log.entries()
        .iter()
        .map(|entry| {
            (
                entry.leadership_term_id,
                entry.recording_id,
                entry.entry_type,
                entry.service_id,
            )
        })
        .collect()
}

/// Assert that `after` still holds every entry `before` had.
fn assert_history_kept(
    name: &str,
    member: i32,
    before: &[(i64, i64, i32, i32)],
    after: &[(i64, i64, i32, i32)],
) {
    for entry in before {
        assert!(
            after.contains(entry),
            "{name}: member {member}'s recording.log lost the entry the other implementation \
             wrote, {entry:?} (term, recording, type, service) — before: {before:?}, after: \
             {after:?}"
        );
    }
}

/// The replaced member's log recording, replayed to the leader's commit
/// position, beside the leader's own.
fn assert_logs_are_the_same(
    name: &str,
    cluster: &TestCluster,
    member: i32,
    leader: i32,
    position: i64,
) {
    let leader_log = recordings_of(cluster.cluster_dir(leader), SERVICE_ID)
        .log
        .unwrap_or_else(|| {
            panic!(
                "{name}: the leader's recording.log names no log recording\n{}",
                node_logs(cluster)
            )
        });
    let member_log = recordings_of(cluster.cluster_dir(member), SERVICE_ID)
        .log
        .unwrap_or_else(|| {
            panic!(
                "{name}: member {member}'s recording.log names no log recording\n{}",
                node_logs(cluster)
            )
        });
    assert_eq!(
        member_log.leadership_term_id,
        leader_log.leadership_term_id,
        "{name}: member {member} is in term {} and the leader in {}: the log bytes are not \
         comparable\n{}",
        member_log.leadership_term_id,
        leader_log.leadership_term_id,
        node_logs(cluster)
    );
    let leader_bytes = read_recording(
        cluster.aeron_dir(leader),
        leader_log.id,
        position - leader_log.term_base_log_position,
    );
    assert!(
        !leader_bytes.is_empty(),
        "{name}: the leader's log recording replayed to nothing — the ruler is not measuring \
         anything\n{}",
        node_logs(cluster)
    );
    let member_bytes = read_recording(
        cluster.aeron_dir(member),
        member_log.id,
        position - member_log.term_base_log_position,
    );
    assert_same_bytes(
        &format!("{name}: member {member}'s log recording"),
        &leader_bytes,
        &member_bytes,
    );
}

/// The other implementation's tool reads the directory this member just left.
fn the_other_tool_reads_it(cluster: &TestCluster, member: i32, was_java: bool) -> String {
    let directory = cluster.cluster_dir(member);
    if was_java {
        // The reference's implementation wrote it; ours has to read it.
        cluster_tool::cluster_tool(directory, &["describe"])
    } else {
        // Ours wrote it; the reference's has to read it.
        java::cluster_tool(cluster.jar(), directory, &["describe"])
    }
}

/// One rolling step: stop `member`, judge what it left, start `replacement` over
/// the same directories, and judge what it wrote.
///
/// Answers the leader after the step — the same one, unless this step took the
/// leader away.
#[allow(clippy::too_many_arguments)]
fn roll(
    name: &str,
    cluster: &mut TestCluster,
    session: &mut Option<Session>,
    base: u16,
    member: i32,
    replacement: MemberSpec,
    leader: i32,
    bids_sent: u64,
) -> i32 {
    let was_the_leader = leader == member;
    let was_java = cluster
        .members()
        .find(|spec| spec.id == member && spec.service_id == SERVICE_ID)
        .is_some_and(|spec| matches!(spec.node, deepmsg_tests::cluster::NodeImpl::Java));

    cluster_mixed::stop_member(cluster, member);

    // (4) The other implementation's tool reads the directory it left.
    let described = the_other_tool_reads_it(cluster, member, was_java);
    assert!(
        described.contains("Type: CONSENSUS_MODULE"),
        "{name}: the {} tool did not read member {member}'s cluster directory (the {} \
         implementation wrote it): {described}",
        if was_java { "ours" } else { "reference's" },
        if was_java { "reference's" } else { "ours" },
    );

    // (2) What it left, before the replacement touches it.
    let history_before = history_of(cluster.cluster_dir(member));
    assert!(
        !history_before.is_empty(),
        "{name}: member {member}'s recording.log is empty before the replacement\n{}",
        node_logs(cluster)
    );

    cluster.replace_member(replacement);
    eprintln!(
        "{name}: member {member} ({}, {}) was replaced by {}",
        if was_the_leader { "leader" } else { "follower" },
        if was_java { "the reference's" } else { "ours" },
        if was_java { "ours" } else { "the reference's" },
    );

    // A replaced **leader** leaves an election behind. Two things follow: the
    // cluster has to settle before the next step replaces anybody (a member
    // replaced mid-election is what §13.2 of the plan's landing record is
    // about), and the client's session is now the restarted node's to close.
    // The waits drive the session, or the client's own driver reaps it
    // (LC-1's lesson).
    let leader = if was_the_leader {
        let leader = await_settled(name, cluster, session.as_mut().expect("a session"));
        reconnect(name, cluster, base, session);
        leader
    } else {
        leader
    };
    // (1) The cluster is still committing, and the replacement catches up.
    bids(name, cluster, base, session, bids_sent, 3);
    let live = session.as_mut().expect("a session");
    let position = commit_position(cluster, leader).unwrap_or_else(|| {
        panic!(
            "{name}: the leader publishes no commit position\n{}",
            node_logs(cluster)
        )
    });
    await_position(name, cluster, member, position, live);

    // (2) It came up from the other implementation's state.
    let history_after = history_of(cluster.cluster_dir(member));
    assert_history_kept(name, member, &history_before, &history_after);

    // (3) And its log is the leader's, byte for byte.
    assert_logs_are_the_same(name, cluster, member, leader, position);

    leader
}

/// Wait until the cluster a replaced leader left is **settled**, and answer the
/// leader it settled on.
///
/// Settled means what [`cluster_mixed::await_all_joined`] means for a fresh
/// start: every member publishes a role, exactly one of them is LEADER, and
/// every member is in the same leadership term. Waiting for anything less —
/// the first member that says LEADER, as an earlier draft of this case did —
/// catches a cluster **mid-election**, and the next rolling step then replaces a
/// member of a cluster that has not agreed who leads: the log said
/// `member terms [2, 0, 0]` with a candidate among them, and the member
/// replaced in that window came up with `appendPosition=0` against a term base
/// of 832 and never caught up.
///
/// The wait drives the session: a client nobody polls through a whole election
/// is a client its own driver reaps, and the failure that produces looks like a
/// cluster that never elected (P3-7b §11.2's lesson, learned twice).
///
/// Two outcomes are both correct, and this is not the place to prefer one:
///
/// * the survivors elected a leader of a **later** term, or
/// * the replaced member came up with the state the other implementation left
///   and leads the term again.
fn await_settled(name: &str, cluster: &TestCluster, session: &mut Session) -> i32 {
    let deadline = Instant::now() + DEADLINE;
    let mut found = None;
    let settled = session.poll_until(deadline, |_| {
        let roles = cluster_mixed::roles(cluster);
        let terms: Vec<Option<i64>> = (0..MEMBERS)
            .map(|member| cluster_mixed::leadership_term(cluster, member))
            .collect();
        if !roles.iter().all(Option::is_some) || !terms.iter().all(Option::is_some) {
            return false;
        }
        if terms.windows(2).any(|pair| pair[0] != pair[1]) {
            return false;
        }
        // One LEADER and every other member a FOLLOWER: a candidate is a
        // member that has not agreed yet (`Cluster.java:51-68`).
        let leaders: Vec<i32> = (0..MEMBERS)
            .filter(|member| {
                roles[usize::try_from(*member).expect("a member id")]
                    == Some(cluster_mixed::ROLE_LEADER)
            })
            .collect();
        let followers = roles
            .iter()
            .filter(|role| **role == Some(cluster_mixed::ROLE_FOLLOWER))
            .count();
        if let [leader] = leaders.as_slice() {
            if followers == usize::try_from(MEMBERS).expect("a member count") - 1 {
                found = Some(*leader);
                return true;
            }
        }
        false
    });
    assert!(
        settled,
        "{name}: the cluster never settled on one leader after the replacement: roles {:?}, \
         member terms {:?}\n{}",
        cluster_mixed::roles(cluster),
        (0..MEMBERS)
            .map(|member| cluster_mixed::leadership_term(cluster, member))
            .collect::<Vec<_>>(),
        node_logs(cluster)
    );
    let leader = found.expect("a leader the cluster settled on");
    eprintln!(
        "{name}: the cluster settled on member {leader} in term {:?} (roles {:?})",
        cluster_mixed::leadership_term(cluster, leader),
        cluster_mixed::roles(cluster),
    );
    leader
}

/// Wait until `member`'s commit position reaches `position`, driving the
/// session.
///
/// [`cluster_mixed::await_caught_up`] is the same wait with a follower
/// assertion on top, and it is the right one for a member that comes back while
/// somebody else leads. A member that was the leader and **leads again** is not
/// a follower, so the assertion would be false of a cluster that did exactly
/// what it should.
fn await_position(
    name: &str,
    cluster: &TestCluster,
    member: i32,
    position: i64,
    session: &mut Session,
) {
    let deadline = Instant::now() + DEADLINE;
    let caught_up = session.poll_until(deadline, |_| {
        commit_position(cluster, member).is_some_and(|reached| reached >= position)
    });
    assert!(
        caught_up,
        "{name}: member {member} never reached {position}: it is at {:?}, the roles are {:?}, \
         member terms {:?}\nits recording.log: {:?}\nour tool's recovery plan:\n{}\n{}",
        commit_position(cluster, member),
        cluster_mixed::roles(cluster),
        (0..MEMBERS)
            .map(|member| cluster_mixed::leadership_term(cluster, member))
            .collect::<Vec<_>>(),
        history_of(cluster.cluster_dir(member)),
        cluster_tool::try_cluster_tool(cluster.cluster_dir(member), &["recovery-plan", "1"]),
        node_logs(cluster)
    );
}

/// The client's half of a leader's replacement: a new session.
///
/// A node that starts up and goes on to lead **closes the sessions it did not
/// open** (`SessionManager.prepareSessionsForNewTerm(true)`, `:932-945`): the
/// restarted module has no reason to trust a session it was not the one to
/// open. The client's half is to open another one.
///
/// Where the old session ends up depends on how long the member was away, and
/// **that is not a difference between the implementations** — the
/// all-reference control reproduces both outcomes with the same client:
///
/// * away for longer than its egress image's liveness: the client falls to
///   `AWAIT_NEW_LEADER`, and a leader that resumed its own term sends no
///   `NewLeaderEvent`, so it cannot recover by itself;
/// * away for less: the client stays `CONNECTED`, keeps sending into the
///   session the cluster has meanwhile marked CLOSING, and its messages are
///   dropped in silence — the reference drops them at the same line
///   (`ConsensusModuleAgent.onIngressMessage` appends only for `session.isOpen()`).
///
/// So the case does not judge the race. It judges what the card is about: a
/// client that opens a new session goes on, and nothing the cluster kept
/// depended on the old one. A **follower's** replacement touches no session and
/// does not come here.
fn reconnect(name: &str, cluster: &TestCluster, base: u16, session: &mut Option<Session>) {
    eprintln!(
        "{name}: the leader was replaced, so the client's session (term {}, member {}) is the \
         restarted node's to close; it opens a new one",
        session
            .as_ref()
            .map_or(0, |session| session.session.leadership_term_id()),
        session
            .as_ref()
            .map_or(0, |session| session.session.leader_member_id()),
    );
    *session = None;
    *session = connect(name, base, cluster);
    assert!(session.is_some(), "{name}: our client's driver is gone");
}

/// Take a snapshot and compare every member's consensus module snapshot and
/// business snapshot with the others'.
///
/// This is LC-3's ruler (§3.4) pointed at a different question: the members on
/// the two sides of a rolling replacement have to hold the **same state**, as
/// bytes — including the business, whose state a replacement has to have carried
/// across implementations.
fn snapshot_and_compare(name: &str, cluster: &TestCluster, session: &mut Session) {
    cluster_mixed::take_a_snapshot(cluster, session, &[0, 1, 2]);

    let found: Vec<MemberRecordings> = (0..MEMBERS)
        .map(|member| recordings_of(cluster.cluster_dir(member), SERVICE_ID))
        .collect();

    compare_the_snapshots(
        name,
        cluster,
        "consensus module snapshot",
        &found
            .iter()
            .map(|recordings| {
                recordings
                    .module_snapshot
                    .expect("every member took a snapshot")
            })
            .collect::<Vec<_>>(),
    );
    compare_the_snapshots(
        name,
        cluster,
        "business snapshot",
        &found
            .iter()
            .map(|recordings| {
                recordings
                    .service_snapshot
                    .expect("every member took a snapshot")
            })
            .collect::<Vec<_>>(),
    );
}

/// One kind of snapshot, at one log position, byte for byte equal across the
/// members.
fn compare_the_snapshots(
    name: &str,
    cluster: &TestCluster,
    what: &str,
    picks: &[deepmsg_tests::cluster_bytes::Recording],
) {
    assert!(
        picks
            .iter()
            .all(|recording| recording.log_position == picks[0].log_position),
        "{name}: the {what}s are not at one log position: {:?}\n{}",
        picks
            .iter()
            .map(|recording| recording.log_position)
            .collect::<Vec<_>>(),
        node_logs(cluster)
    );
    let bytes: Vec<Vec<u8>> = picks
        .iter()
        .enumerate()
        .map(|(member, recording)| {
            read_recording(
                cluster.aeron_dir(i32::try_from(member).expect("a member id")),
                recording.id,
                -2,
            )
        })
        .collect();
    assert!(
        !bytes[0].is_empty(),
        "{name}: the {what}s replayed to nothing — the ruler is not measuring anything\n{}",
        node_logs(cluster)
    );
    for member in 1..MEMBERS {
        assert_same_bytes(
            &format!("{name}: member {member}'s {what}"),
            &bytes[0],
            &bytes[usize::try_from(member).expect("a member id")],
        );
    }
}

/// Run the whole cycle on three members: `3J → 3R → 3J`, one member at a time,
/// over the directories each implementation left.
///
/// `order` is the member ids in the order the cycle replaces them. The leader is
/// whichever member won the election — there is no appointment, because a
/// rolling replacement has to reach the leader too (see [`settings`]) — so the
/// two cases run the cycle in opposite orders and between them take the leader
/// away at different points in the cluster's life.
fn the_cycle(
    name: &str,
    order: &[i32],
    first: fn(i32) -> MemberSpec,
    second: fn(i32) -> MemberSpec,
    what: (&str, &str),
) {
    let base = next_port_base();
    let members: Vec<MemberSpec> = (0..MEMBERS).map(java_spec).collect();
    let Some((mut cluster, mut leader)) = cluster_mixed::start(name, &members, settings(base))
    else {
        cluster_mixed::skip(name);
        return;
    };
    let mut session = connect(name, base, &cluster);
    if session.is_none() {
        cluster_mixed::skip("our client's driver");
        return;
    }

    // A snapshot before the first swap: every member's directory now holds a
    // consensus module snapshot and a business snapshot the other implementation
    // wrote, and a replacement has to read them out of its own archive.
    let mut bids_sent = 0;
    bids(name, &cluster, base, &mut session, bids_sent, 5);
    bids_sent += 5;
    snapshot_and_compare(name, &cluster, session.as_mut().expect("a session"));

    for member in order {
        leader = roll(
            name,
            &mut cluster,
            &mut session,
            base,
            *member,
            first(*member),
            leader,
            bids_sent,
        );
        bids_sent += 3;
    }
    eprintln!("{name}: the cluster is three of {}", what.0);

    // The state ours carries is the state the reference's left: a snapshot taken
    // now, on every member, has to be the same bytes everywhere.
    snapshot_and_compare(name, &cluster, session.as_mut().expect("a session"));

    for member in order {
        leader = roll(
            name,
            &mut cluster,
            &mut session,
            base,
            *member,
            second(*member),
            leader,
            bids_sent,
        );
        bids_sent += 3;
    }
    eprintln!("{name}: the cluster is three of {} again", what.1);

    snapshot_and_compare(name, &cluster, session.as_mut().expect("a session"));

    bids(name, &cluster, base, &mut session, bids_sent, 3);
    let session = session.as_mut().expect("a session");
    let position = commit_position(&cluster, leader)
        .unwrap_or_else(|| panic!("{name}: the leader publishes no commit position"));
    cluster_mixed::await_everyone_at_the_leaders_position(&cluster, leader, session);
    eprintln!("{name}: every member is at {position}");

    cluster.shutdown();
}

/// The cycle, `3J → 3R → 3J`, on one cluster: every step judged by the four
/// criteria in the module note.
#[test]
fn the_implementation_rolls_from_the_references_to_ours_and_back() {
    let _serial = serial();
    the_cycle(
        "lc5-rolling",
        &[0, 1, 2],
        rust_spec,
        java_spec,
        ("ours", "the reference's"),
    );
}

/// The same cycle with the members replaced in the other order.
///
/// The leader is whichever member won the election, so this case does not
/// *choose* to replace the leader first; running the cycle in both orders is
/// what puts a leader's replacement at two different points in the cluster's
/// life rather than always at the same one.
#[test]
fn the_implementation_rolls_in_reverse_member_order() {
    let _serial = serial();
    the_cycle(
        "lc5-rolling-reversed",
        &[2, 1, 0],
        rust_spec,
        java_spec,
        ("ours", "the reference's"),
    );
}

/// **The control**: the same rolling steps on a cluster of the reference's,
/// restarting each member as the reference's.
///
/// Every judgement the mixed cycle makes about *the cluster* has to hold here
/// first. A rolling restart of a reference node is a deployment the reference
/// supports, and it is the only way to tell "our implementation read the other's
/// state" from "every implementation behaves this way, including the awkward
/// parts". What this case cannot judge is the crossing — that is the two cases
/// above — but a red here means the fixture or the client is broken.
#[test]
fn the_same_rolling_steps_on_a_cluster_of_the_references() {
    let _serial = serial();
    the_cycle(
        "lc5-control",
        &[0, 1, 2],
        java_spec,
        java_spec,
        ("the reference's", "the reference's"),
    );
}
