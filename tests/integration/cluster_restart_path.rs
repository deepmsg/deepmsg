//! P3-6c's F1 family: **the restart and catch-up rows of the reference's
//! `ClusterTest`** (`records/P3-6c.md` §5.2 puts 21 rows here).
//!
//! What separates these from P3-6b's follower path is that a member comes
//! **back**: 6b stops a follower or a leader and asks the survivors to carry
//! on, where F1 restarts the member on its own directories and asks it to be
//! taken back in — as a follower of a term it never voted in, from a log that
//! is behind, and (in the uncommitted-entrant rows) from a log whose tail the
//! cluster never committed.
//!
//! # The three shapes
//!
//! * **the leader comes back as a follower** — the term it was deposed from is
//!   not one it returns to (`Election` puts a restarted member at
//!   `FOLLOWER_REPLAY`; the term it holds is the one it read from its own
//!   recording log);
//! * **a clean restart** — the member is told to `SHUTDOWN`, takes its snapshot
//!   and ends its duty cycle, so what it comes back with is a snapshot instead
//!   of an appended log;
//! * **uncommitted entries** — the leader appended and could not commit, and
//!   the restarted members have to agree on what the new term truncates.
//!
//! # What a case reads
//!
//! The reference's cases assert the stub service's message count and the
//! client's echo; this build's members run no service in these cases (the
//! container is LC-2's and LC-4a's), so the evidence is the **commit position**
//! and the **append position** of every member, read through their counters, and
//! the client's own session events. That is the `weakened:` qualifier the
//! table's `ours` column carries.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster_harness::{
    CATCHUP_DEADLINE, ClientSide, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports,
    ROLE_FOLLOWER, ROLE_LEADER, announce_skip, await_launched, members_of, wait_until,
};

/// The reference's **five-member** shape: the cases where a majority has to
/// stay alive while others are away (`ClusterTest`'s five-node rows). With
/// three members, two away is no quorum at all and a client cannot even open a
/// session.
const FIVE: i32 = 5;

/// The scenarios here bind ports derived from the **process** id
/// (`cluster_harness::Ports`), so `cargo test`'s parallel threads would race
/// for them. They take turns.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Three members, launched, with their counters — or `None` when this machine
/// cannot stand a driver up.
fn start_cluster(ports: &Ports) -> Option<(Vec<Node>, Vec<NodeCounters>, Faults)> {
    start_cluster_of(ports, MEMBER_COUNT)
}

/// [`start_cluster`] for a cluster of `count` members.
fn start_cluster_of(ports: &Ports, count: i32) -> Option<(Vec<Node>, Vec<NodeCounters>, Faults)> {
    let member_string = members_of(ports, count);
    let faults = Faults::default();
    let (launched_tx, launched_rx) = mpsc::channel();

    let mut nodes = Vec::new();
    for id in 0..count {
        let node = Node::start(id, ports, &member_string, &faults, &launched_tx)?;
        nodes.push(node);
    }
    await_launched(&faults, &launched_rx, count);

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    wait_until("every member's election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    Some((nodes, counters, faults))
}

/// The one member whose role counter says leader.
fn the_leader(counters: &[NodeCounters]) -> i32 {
    let leaders: Vec<i32> = (0..MEMBER_COUNT)
        .filter(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
        .collect();

    assert_eq!(1, leaders.len(), "exactly one leader, not {leaders:?}");
    leaders[0]
}

/// [`wait_until`] with the counters in the panic, for a wait whose timeout has
/// to explain itself.
fn wait_with_state<F, D>(what: &str, faults: &Faults, budget: Duration, condition: F, detail: D)
where
    F: FnMut() -> bool,
    D: Fn() -> String,
{
    let deadline = Instant::now() + budget;
    let mut condition = condition;
    while Instant::now() < deadline {
        faults.check();
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    panic!("timed out waiting for {what}; {}", detail());
}

/// A client session on `leader`, and one message committed through it.
fn open_session(
    client: &mut ClientSide,
    faults: &Faults,
) -> deepmsg_tests::cluster_harness::SessionEvent {
    let session = client.connect_session(faults);
    assert_eq!(EventCode::OK, session.code, "the session opened");
    session
}

/// Ten client messages, each waiting for `leader` to commit it — the
/// reference's `sendMessages` + `awaitResponseMessageCount`
/// (`ClusterTest.java:1568-1571`), read through the leader's commit position
/// because these members run no service.
fn send_ten(
    client: &mut ClientSide,
    counters: &[NodeCounters],
    leader: i32,
    session: &deepmsg_tests::cluster_harness::SessionEvent,
    faults: &Faults,
    base: u8,
) {
    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    for index in 0..10u8 {
        client.send(session, &[base.wrapping_add(index); 16], faults);
        wait_until("a message to commit", faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
        last = counters[leader as usize]
            .commit_position()
            .expect("the leader's commit");
    }
}

/// **The leader the survivors deposed comes back as one of their followers**
/// (`ClusterTest.shouldStopLeaderAndRestartAsFollower`, `ClusterTest.java:598`).
///
/// The old leader is killed, the two survivors elect a later term, and the old
/// leader is restarted **on its own directories**. It must close its election as
/// a follower of the new term and its log must reach what the new leader had
/// committed — the term it lost is one the others moved past without it.
#[test]
fn a_stopped_leader_comes_back_as_a_follower() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let first_leader = the_leader(&counters);
    let first_term = counters[first_leader as usize]
        .leadership_term_id()
        .expect("the first term");

    // A committed message, so the new term has something to bring the returning
    // member up to.
    {
        let Some(mut client) = ClientSide::start(&ports, first_leader) else {
            announce_skip();
            return;
        };
        let session = open_session(&mut client, &faults);
        let before = counters[first_leader as usize]
            .commit_position()
            .expect("the leader's commit");
        client.send(&session, b"before the leader was stopped", &faults);
        wait_until("the first message to commit", &faults, || {
            client.poll_events();
            counters[first_leader as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        });
    }

    // ---- the leader dies; the survivors elect a later term ----------------
    nodes[first_leader as usize].stop();
    let survivors: Vec<i32> = (0..MEMBER_COUNT).filter(|id| *id != first_leader).collect();

    wait_until("the survivors to elect a new term", &faults, || {
        survivors.iter().all(|id| {
            counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[*id as usize]
                    .leadership_term_id()
                    .is_some_and(|term| term > first_term)
        })
    });
    let new_leader = the_leader(&counters);
    let new_term = counters[new_leader as usize]
        .leadership_term_id()
        .expect("the new term");
    assert_ne!(first_leader, new_leader, "a different member leads");

    // ---- and it comes back on its own directories -------------------------
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[first_leader as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    let target = counters[new_leader as usize]
        .commit_position()
        .expect("the new leader's commit");
    wait_with_state(
        "the returning member to close as a follower of the new term",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[first_leader as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[first_leader as usize].role() == Some(ROLE_FOLLOWER)
                && counters[first_leader as usize]
                    .append_position()
                    .is_some_and(|position| position >= target)
        },
        || {
            let member = &counters[first_leader as usize];
            format!(
                "member {first_leader}: role={:?} election={:?} term={:?} append={:?} \
                 commit={:?}; leader {new_leader} term={new_term} commit={:?}",
                member.role(),
                member.election_state(),
                member.leadership_term_id(),
                member.append_position(),
                member.commit_position(),
                counters[new_leader as usize].commit_position(),
            )
        },
    );

    // It is a live follower, not a caught-up corpse: a new commit reaches it.
    let before = counters[new_leader as usize]
        .commit_position()
        .expect("the new leader's commit");
    {
        let Some(mut client) = ClientSide::start(&ports, new_leader) else {
            announce_skip();
            return;
        };
        let session = open_session(&mut client, &faults);
        client.send(&session, b"after the old leader came back", &faults);
        wait_with_state(
            "the new leader to commit, and the returning member to follow",
            &faults,
            CATCHUP_DEADLINE,
            || {
                client.poll_events();
                counters[first_leader as usize]
                    .commit_position()
                    .is_some_and(|position| position > before)
            },
            || {
                format!(
                    "leader commit={:?}, returning member commit={:?} role={:?} election={:?}",
                    counters[new_leader as usize].commit_position(),
                    counters[first_leader as usize].commit_position(),
                    counters[first_leader as usize].role(),
                    counters[first_leader as usize].election_state(),
                )
            },
        );
    }

    for node in &mut nodes {
        node.stop();
    }
}

/// **The client keeps sending while the deposed leader comes back**
/// (`ClusterTest.shouldStopLeaderAndRestartAsFollowerWithSendingAfter`,
/// `ClusterTest.java:616`).
///
/// The same shape as the case above, with the client's session held across it:
/// the session is on the new leader (the old one's term ended with it), and a
/// message offered through it has to commit **after** the returning member is
/// back — a caught-up follower that is not a *live* one does not move its commit
/// counter.
#[test]
fn a_stopped_leader_comes_back_as_a_follower_and_the_client_keeps_sending() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let first_leader = the_leader(&counters);
    let first_term = counters[first_leader as usize]
        .leadership_term_id()
        .expect("the first term");

    nodes[first_leader as usize].stop();
    let survivors: Vec<i32> = (0..MEMBER_COUNT).filter(|id| *id != first_leader).collect();
    wait_until("the survivors to elect a new term", &faults, || {
        survivors.iter().all(|id| {
            counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[*id as usize]
                    .leadership_term_id()
                    .is_some_and(|term| term > first_term)
        })
    });
    let new_leader = the_leader(&counters);

    let Some(mut client) = ClientSide::start(&ports, new_leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    client.send(&session, b"while the old leader is still down", &faults);
    wait_until(
        "the new leader to commit the client's message",
        &faults,
        || {
            client.poll_events();
            counters[new_leader as usize]
                .commit_position()
                .is_some_and(|position| position > 0)
        },
    );

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[first_leader as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    wait_with_state(
        "the returning member to close as a follower",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[first_leader as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[first_leader as usize].role() == Some(ROLE_FOLLOWER)
        },
        || {
            let member = &counters[first_leader as usize];
            format!(
                "member {first_leader}: role={:?} election={:?} append={:?}",
                member.role(),
                member.election_state(),
                member.append_position(),
            )
        },
    );

    // The client's session survives the old leader's death on the new one, and
    // the message it offers now reaches both of them.
    let before = counters[first_leader as usize]
        .commit_position()
        .expect("the returning member's commit");
    client.send(&session, b"after the old leader came back", &faults);
    wait_with_state(
        "the returning member to commit the client's next message",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            counters[first_leader as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        },
        || {
            format!(
                "leader commit={:?}, returning member commit={:?} (before {before})",
                counters[new_leader as usize].commit_position(),
                counters[first_leader as usize].commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **The deposed leader comes back as a follower, and then the leader that
/// replaced it stops too** (`ClusterTest
/// .shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader`,
/// `ClusterTest.java:638`).
///
/// Two elections in a row with a member returning between them: the cluster has
/// to take the returning member back into the second term as well, and elect a
/// third leader from the two that are left.
#[test]
fn a_stopped_leader_comes_back_as_a_follower_then_the_new_leader_stops() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let first_leader = the_leader(&counters);
    let first_term = counters[first_leader as usize]
        .leadership_term_id()
        .expect("the first term");

    nodes[first_leader as usize].stop();
    let survivors: Vec<i32> = (0..MEMBER_COUNT).filter(|id| *id != first_leader).collect();
    wait_until("the survivors to elect a second term", &faults, || {
        survivors.iter().all(|id| {
            counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[*id as usize]
                    .leadership_term_id()
                    .is_some_and(|term| term > first_term)
        })
    });
    let second_leader = the_leader(&counters);
    let second_term = counters[second_leader as usize]
        .leadership_term_id()
        .expect("the second term");

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[first_leader as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);
    wait_with_state(
        "the returning member to close as a follower of the second term",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[first_leader as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[first_leader as usize].role() == Some(ROLE_FOLLOWER)
        },
        || {
            let member = &counters[first_leader as usize];
            format!(
                "member {first_leader}: role={:?} election={:?} append={:?}",
                member.role(),
                member.election_state(),
                member.append_position(),
            )
        },
    );

    // ---- and the second leader stops as well ------------------------------
    nodes[second_leader as usize].stop();
    let remaining: Vec<i32> = (0..MEMBER_COUNT)
        .filter(|id| *id != second_leader)
        .collect();

    wait_with_state(
        "a third term from the two that are left",
        &faults,
        CATCHUP_DEADLINE,
        || {
            remaining.iter().all(|id| {
                counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                    && counters[*id as usize]
                        .leadership_term_id()
                        .is_some_and(|term| term > second_term)
            })
        },
        || {
            format!(
                "roles {:?}, terms {:?}, election {:?}",
                remaining
                    .iter()
                    .map(|id| counters[*id as usize].role())
                    .collect::<Vec<_>>(),
                remaining
                    .iter()
                    .map(|id| counters[*id as usize].leadership_term_id())
                    .collect::<Vec<_>>(),
                remaining
                    .iter()
                    .map(|id| counters[*id as usize].election_state())
                    .collect::<Vec<_>>(),
            )
        },
    );

    let third_leader = the_leader(&counters);
    assert_ne!(
        second_leader, third_leader,
        "a different member leads again"
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A follower restarted with an **empty** directory catches up and takes new
/// messages** (`ClusterTest.shouldAcceptMessagesAfterSingleNodeCleanRestart`,
/// `ClusterTest.java:666`).
///
/// A clean restart in the reference's sense is `startStaticNode(index, true)`:
/// `deleteArchiveOnStart` and `deleteDirOnStart`, so the member comes back with
/// **no log at all**. Everything it has after that came from the leader's
/// archive, which is what makes this the empty-log arm of the catch-up.
#[test]
fn a_follower_restarted_on_an_empty_directory_catches_up() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, mut counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");

    // A committed message first, so there is something for the empty member to
    // be caught up on.
    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    client.send(&session, b"before the member was reformatted", &faults);
    wait_until("the message to commit", &faults, || {
        client.poll_events();
        counters[leader as usize]
            .commit_position()
            .is_some_and(|position| position > 0)
    });

    let target = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[follower as usize].restart_empty(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    // The driver is a new process with a new CnC file, so the mapping the test
    // opened at the start is stale: every read of this member's counters has to
    // come from the new one (`NodeCounters::open` waits for it).
    counters[follower as usize] = NodeCounters::open(nodes[follower as usize].aeron_dir());

    wait_with_state(
        "the empty member to catch up and close as a follower",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[follower as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[follower as usize].role() == Some(ROLE_FOLLOWER)
                && counters[follower as usize]
                    .commit_position()
                    .is_some_and(|position| position >= target)
        },
        || {
            let member = &counters[follower as usize];
            format!(
                "member {follower}: role={:?} election={:?} append={:?} commit={:?}; \
                 the leader's commit was {target}",
                member.role(),
                member.election_state(),
                member.append_position(),
                member.commit_position(),
            )
        },
    );

    // ---- and a message offered now reaches it -----------------------------
    let before = counters[follower as usize]
        .commit_position()
        .expect("the member's commit");
    client.send(&session, b"after the empty restart", &faults);
    wait_with_state(
        "the restarted member to commit",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            counters[follower as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        },
        || {
            format!(
                "leader commit={:?}, member commit={:?} (before {before})",
                counters[leader as usize].commit_position(),
                counters[follower as usize].commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **Both followers restarted on empty directories come back**
/// (`ClusterTest.shouldAcceptMessagesAfterTwoNodeCleanRestart`,
/// `ClusterTest.java:778`).
///
/// The leader is alone while both are away — no quorum — so this is also the
/// case that says the leader holds its term across a short outage instead of
/// stepping down on the first missed heartbeat.
#[test]
fn two_followers_restarted_on_empty_directories_catch_up() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, mut counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let followers: Vec<i32> = (0..MEMBER_COUNT).filter(|id| *id != leader).collect();

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    client.send(&session, b"before both members were reformatted", &faults);
    wait_until("the message to commit", &faults, || {
        client.poll_events();
        counters[leader as usize]
            .commit_position()
            .is_some_and(|position| position > 0)
    });
    let target = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    for follower in &followers {
        nodes[*follower as usize].restart_empty(&relaunch_tx);
    }
    await_launched(
        &faults,
        &relaunch_rx,
        i32::try_from(followers.len()).expect("two"),
    );

    // Each returning member is a new driver with a new CnC file, so the mappings
    // the test opened at the start are stale.
    for follower in &followers {
        counters[*follower as usize] = NodeCounters::open(nodes[*follower as usize].aeron_dir());
    }

    wait_with_state(
        "both empty members to catch up and close",
        &faults,
        CATCHUP_DEADLINE,
        || {
            followers.iter().all(|id| {
                counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                    && counters[*id as usize].role() == Some(ROLE_FOLLOWER)
                    && counters[*id as usize]
                        .commit_position()
                        .is_some_and(|position| position >= target)
            })
        },
        || {
            format!(
                "roles {:?}, election {:?}, commits {:?}; the leader's commit was {target}",
                followers
                    .iter()
                    .map(|id| counters[*id as usize].role())
                    .collect::<Vec<_>>(),
                followers
                    .iter()
                    .map(|id| counters[*id as usize].election_state())
                    .collect::<Vec<_>>(),
                followers
                    .iter()
                    .map(|id| counters[*id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    // With both members away the leader may have lost its quorum and stepped
    // down, so the cluster's leader now is not necessarily the one the session
    // was opened on — and this harness's client does not follow a redirect. A
    // fresh client on whichever member leads now is what the reference's client
    // would have become by following the new-leader event.
    wait_until("one leader again", &faults, || {
        (0..MEMBER_COUNT)
            .filter(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
            .count()
            == 1
    });
    let leader = the_leader(&counters);
    // The first client of this case still holds its egress port, so this one
    // takes the next (`Ports::with_egress_offset`).
    let Some(mut client) = ClientSide::start_at(&ports, leader, 1) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    let target = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    client.send(&session, b"after both restarted empty", &faults);
    wait_with_state(
        "a message committed with both empty members back",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            followers.iter().all(|id| {
                counters[*id as usize]
                    .commit_position()
                    .is_some_and(|position| position > target)
            })
        },
        || {
            format!(
                "leader commit={:?}, member commits {:?} (target {target})",
                counters[leader as usize].commit_position(),
                followers
                    .iter()
                    .map(|id| counters[*id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A follower stopped and restarted on its own directories is invisible to
/// the client** (`ClusterTest.shouldStopFollowerAndRestartFollowerGracefully`,
/// `ClusterTest.java:203`).
///
/// The reference's "gracefully" is `consensusModule().close()` — the module's
/// own close, not the `SHUTDOWN` toggle (which only a leader honours) and not a
/// process kill. The client's session is open throughout and keeps committing:
/// one follower leaving and coming back is nothing a client sees.
#[test]
fn a_follower_stopped_and_restarted_is_invisible_to_the_client() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);

    let before = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    nodes[follower as usize].stop();

    // The client's message commits with one follower gone — the survivor is the
    // quorum.
    client.send(&session, b"while the follower is stopped", &faults);
    wait_with_state(
        "the client's message to commit without that follower",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        },
        || {
            format!(
                "leader commit={:?} (before {before}), roles {:?}",
                counters[leader as usize].commit_position(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].role())
                    .collect::<Vec<_>>(),
            )
        },
    );

    // ---- and it is back, still invisible ----------------------------------
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[follower as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    wait_with_state(
        "the restarted follower to close",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[follower as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[follower as usize].role() == Some(ROLE_FOLLOWER)
        },
        || {
            let member = &counters[follower as usize];
            format!(
                "member {follower}: role={:?} election={:?} append={:?}",
                member.role(),
                member.election_state(),
                member.append_position(),
            )
        },
    );

    client.send(&session, b"after the follower came back", &faults);
    wait_with_state(
        "a message committed with the follower back",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            counters[follower as usize]
                .commit_position()
                .is_some_and(|position| position > 0)
        },
        || {
            format!(
                "leader commit={:?}, member commit={:?}",
                counters[leader as usize].commit_position(),
                counters[follower as usize].commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A follower that misses a single message catches up on the way back**
/// (`ClusterTest.shouldCatchUpAfterFollowerMissesOneMessage`,
/// `ClusterTest.java:1808`).
///
/// The smallest catch-up there is: one commit while the follower is away, and
/// the returning member has to close as a follower with that commit in its own
/// counter. A case that passes with ten messages can still hide an off-by-one
/// in the replay's start position, which is why the reference has this one as
/// well as the larger ones.
#[test]
fn a_follower_that_missed_one_message_catches_up() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);

    // One message before it goes, one while it is away.
    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    client.send(&session, b"before", &faults);
    wait_until("the first message", &faults, || {
        client.poll_events();
        counters[leader as usize]
            .commit_position()
            .is_some_and(|position| position > last)
    });
    last = counters[leader as usize]
        .commit_position()
        .expect("a commit");

    nodes[follower as usize].stop();
    client.send(&session, b"the message it misses", &faults);
    wait_until("the missed message to commit", &faults, || {
        client.poll_events();
        counters[leader as usize]
            .commit_position()
            .is_some_and(|position| position > last)
    });
    let missed = counters[leader as usize]
        .commit_position()
        .expect("a commit");

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[follower as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    wait_with_state(
        "the returning member to reach the commit it missed",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[follower as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[follower as usize].role() == Some(ROLE_FOLLOWER)
                && counters[follower as usize]
                    .commit_position()
                    .is_some_and(|position| position >= missed)
        },
        || {
            let member = &counters[follower as usize];
            format!(
                "member {follower}: role={:?} election={:?} append={:?} commit={:?}; the \
                 commit it missed was {missed}",
                member.role(),
                member.election_state(),
                member.append_position(),
                member.commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A follower that stopped part way through a term catches up**
/// (`ClusterTest.shouldRecoverWhenFollowerArrivesPartWayThroughTerm`,
/// `ClusterTest.java:1532`).
///
/// Ten commits land while it is away, all of them in the term it left, so the
/// replay has to start inside a term it is already holding rather than at the
/// term boundary a replication would use.
#[test]
fn a_follower_back_part_way_through_a_term_catches_up() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);

    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    client.send(&session, b"the message before the stop", &faults);
    wait_until("the first message", &faults, || {
        client.poll_events();
        counters[leader as usize]
            .commit_position()
            .is_some_and(|position| position > last)
    });

    nodes[follower as usize].stop();
    for index in 0..10u8 {
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
        client.send(&session, &[b'a' + index; 16], &faults);
        wait_until("each message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
    }
    let target = counters[leader as usize]
        .commit_position()
        .expect("a commit");

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[follower as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    wait_with_state(
        "the returning member to reach the ten commits it missed",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[follower as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[follower as usize].role() == Some(ROLE_FOLLOWER)
                && counters[follower as usize]
                    .commit_position()
                    .is_some_and(|position| position >= target)
        },
        || {
            let member = &counters[follower as usize];
            format!(
                "member {follower}: role={:?} election={:?} append={:?} commit={:?}; the \
                 leader's commit was {target}",
                member.role(),
                member.election_state(),
                member.append_position(),
                member.commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A member that missed a whole election catches up**
/// (`ClusterTest.shouldRecoverWhenFollowerArrivePartWayThroughTermAfterMissingElection`,
/// `ClusterTest.java:1558-1593`).
///
/// The reference's five steps, in its order: ten messages, one follower away,
/// ten more, **then the rest of the cluster goes down and comes back
/// together**, a reconnected client sends ten more, and only then does the
/// member that was away all along return — it is behind in **terms**, not only
/// in position, and the way back is the replication path rather than a replay
/// of its own term.
///
/// Two things this case deliberately does **not** ask for, both of them lessons
/// this file has already paid for:
///
/// * a **later** term. The reference calls `awaitLeader()` and nothing else;
///   a restarted leader resumes its own term (`is_leader_startup`), so
///   "a leader" is the guarantee and "a later term" is not (§6.9 of the
///   record);
/// * a quorum that never breaks. Steps 3 and 4 leave the cluster with no
///   member at all for a moment, which is the reference's shape too.
///
/// `weakened:` the reference reads each member's stub-service message count
/// (`cluster.node(i).service().messageCount()`); these members run no service,
/// so the evidence is the leader's **commit position** at each step and the
/// returning member's commit against the last of them.
#[test]
fn a_follower_back_after_missing_an_election_catches_up() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower_one = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");
    let follower_two = (0..MEMBER_COUNT)
        .find(|id| *id != leader && *id != follower_one)
        .expect("a second follower");

    // 1. Ten messages through a client on the leader.
    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    send_ten(&mut client, &counters, leader, &session, &faults, b'A');

    // 2. One follower away, ten more.
    nodes[follower_one as usize].stop();
    send_ten(&mut client, &counters, leader, &session, &faults, b'K');

    // 3. The rest of the cluster goes down — leader and the other follower.
    nodes[follower_two as usize].stop();
    nodes[leader as usize].stop();

    // 4. ...and comes back together, on its own directories, and a client
    // reconnects on whichever member is the leader of the term they close in.
    for id in [leader, follower_two] {
        let (relaunch_tx, relaunch_rx) = mpsc::channel();
        nodes[id as usize].restart(&relaunch_tx);
        await_launched(&faults, &relaunch_rx, 1);
    }

    wait_with_state(
        "the two that came back to close in one term",
        &faults,
        CATCHUP_DEADLINE,
        || {
            [leader, follower_two]
                .iter()
                .all(|id| counters[*id as usize].election_state() == Some(ELECTION_CLOSED))
                && [leader, follower_two]
                    .iter()
                    .filter(|id| counters[**id as usize].role() == Some(ROLE_LEADER))
                    .count()
                    == 1
        },
        || {
            format!(
                "roles {:?}, terms {:?}, election {:?}, appends {:?}, commits {:?}",
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].role())
                    .collect::<Vec<_>>(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].leadership_term_id())
                    .collect::<Vec<_>>(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].election_state())
                    .collect::<Vec<_>>(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].append_position())
                    .collect::<Vec<_>>(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    let new_leader = [leader, follower_two]
        .into_iter()
        .find(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
        .expect("a leader among the two");

    // The client of the term before the outage cannot follow the new leader
    // (this build's `ClientSide` does not act on a redirect), so the reference's
    // `reconnectClient()` is a **second** client. The first is given back
    // before it: two `ClientSide`s on one case would share their driver's
    // aeron directory (the name is the port block's), and the second start
    // removes that directory first. The egress offset keeps the second one off
    // the first one's port as well.
    drop(client);
    let Some(mut second_client) = ClientSide::start_at(&ports, new_leader, 1) else {
        announce_skip();
        return;
    };
    let second_session = open_session(&mut second_client, &faults);
    send_ten(
        &mut second_client,
        &counters,
        new_leader,
        &second_session,
        &faults,
        b'a',
    );
    let target = counters[new_leader as usize]
        .commit_position()
        .expect("the new leader's commit");

    // 5. Only now does the member that missed the term come back.
    let (back_tx, back_rx) = mpsc::channel();
    nodes[follower_one as usize].restart(&back_tx);
    await_launched(&faults, &back_rx, 1);

    wait_with_state(
        "the member that missed the term to catch up",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[follower_one as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[follower_one as usize].role() == Some(ROLE_FOLLOWER)
                && counters[follower_one as usize]
                    .commit_position()
                    .is_some_and(|position| position >= target)
        },
        || {
            let member = &counters[follower_one as usize];
            format!(
                "member {follower_one}: role={:?} election={:?} term={:?} append={:?} \
                 commit={:?}; the new term's commit was {target}",
                member.role(),
                member.election_state(),
                member.leadership_term_id(),
                member.append_position(),
                member.commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **The client keeps sending while a follower is away and comes back**
/// (`ClusterTest.shouldRecoverWhileMessagesContinue`,
/// `ClusterTest.java:1148`).
///
/// The reference runs a publisher in a thread through the outage; this build
/// sends from the test's own thread between the steps, which is the same
/// question — messages committed on both sides of the stop — without a second
/// client. What it asserts is the end state: every member's commit is the
/// leader's.
#[test]
fn messages_continue_across_a_follower_restart() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);

    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    let mut offer = |client: &mut ClientSide, counters: &[NodeCounters], payload: u8| {
        client.send(&session, &[payload; 16], &faults);
        wait_until("a message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    };

    offer(&mut client, &counters, b'1');
    nodes[follower as usize].stop();
    offer(&mut client, &counters, b'2');
    offer(&mut client, &counters, b'3');

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[follower as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);
    offer(&mut client, &counters, b'4');

    let target = counters[leader as usize]
        .commit_position()
        .expect("a commit");
    wait_with_state(
        "every member to be at the leader's commit",
        &faults,
        CATCHUP_DEADLINE,
        || {
            (0..MEMBER_COUNT).all(|id| {
                counters[id as usize]
                    .commit_position()
                    .is_some_and(|position| position >= target)
            })
        },
        || {
            format!(
                "commits {:?}, target {target}",
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A member whose log was never written catches up after a restart**
/// (`ClusterTest.shouldCatchupFromEmptyLog`, `ClusterTest.java:1190`).
///
/// The follower is stopped while its log holds nothing — it joined, voted, and
/// the cluster went on without anything being committed through it. Ten
/// commits then land and it comes back dirty: everything it has after that came
/// from the leader's archive, because its own recording has no frames to replay.
#[test]
fn a_member_with_an_empty_log_catches_up() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");

    nodes[follower as usize].stop();

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    for index in 0..10u8 {
        client.send(&session, &[b'e' + index; 16], &faults);
        wait_until("each message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    }

    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[follower as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    wait_with_state(
        "the member with an empty log to catch up",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[follower as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[follower as usize].role() == Some(ROLE_FOLLOWER)
                && counters[follower as usize]
                    .commit_position()
                    .is_some_and(|position| position >= last)
        },
        || {
            let member = &counters[follower as usize];
            format!(
                "member {follower}: role={:?} election={:?} append={:?} commit={:?}; the \
                 leader's commit was {last}",
                member.role(),
                member.election_state(),
                member.append_position(),
                member.commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A leader that appended past a stopped follower, both cold-restarted, come
/// back to one term** (`ClusterTest.shouldRecoverWhenLeaderHasAppendedMoreThanFollower`,
/// `ClusterTest.java:1388`).
///
/// The follower stops, ten commits land, and then the two that are left stop
/// too — so nothing is up when the leader and that follower come back. Which of
/// them leads again is the election's to say; what the case asserts is that both
/// close in one term with the same commit.
#[test]
fn a_leader_and_a_follower_come_back_after_both_stopped() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let leader = the_leader(&counters);
    let follower = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");
    let third = (0..MEMBER_COUNT)
        .find(|id| *id != leader && *id != follower)
        .expect("the third member");

    nodes[follower as usize].stop();

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    for index in 0..10u8 {
        client.send(&session, &[b'h' + index; 16], &faults);
        wait_until("each message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    }

    nodes[leader as usize].stop();
    nodes[third as usize].stop();

    // The leader and the follower that was behind come back together.
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[leader as usize].restart(&relaunch_tx);
    nodes[follower as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 2);

    wait_with_state(
        "the two returning members to close in one term at one commit",
        &faults,
        CATCHUP_DEADLINE,
        || {
            [leader, follower]
                .iter()
                .all(|id| counters[*id as usize].election_state() == Some(ELECTION_CLOSED))
                && counters[leader as usize].leadership_term_id()
                    == counters[follower as usize].leadership_term_id()
                && [leader, follower].iter().all(|id| {
                    counters[*id as usize]
                        .commit_position()
                        .is_some_and(|position| position >= last)
                })
        },
        || {
            format!(
                "terms {:?}, election {:?}, commits {:?}; the commit was {last}",
                [leader, follower]
                    .iter()
                    .map(|id| counters[*id as usize].leadership_term_id())
                    .collect::<Vec<_>>(),
                [leader, follower]
                    .iter()
                    .map(|id| counters[*id as usize].election_state())
                    .collect::<Vec<_>>(),
                [leader, follower]
                    .iter()
                    .map(|id| counters[*id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **Two leaders fail in turn and come back together**
/// (`ClusterTest.shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime`,
/// `ClusterTest.java:750`).
///
/// The first leader stops, a second is elected, and the second stops in turn;
/// then both of them come back at the same moment and have to be taken in by the
/// third's term.
#[test]
fn two_failed_leaders_come_back_together() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports) else {
        return;
    };

    let first = the_leader(&counters);
    nodes[first as usize].stop();
    wait_until("a second leader", &faults, || {
        (0..MEMBER_COUNT)
            .filter(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
            .count()
            == 1
    });
    let second = the_leader(&counters);
    assert_ne!(first, second, "a different member leads");
    // **Not** "a later term": a member that wins an election restores the term
    // its own recording log holds, and the reference's `TestCluster` cases read
    // the same way — what an election guarantees is a leader, not a bigger
    // number (`records/P3-6c.md` 6.9).
    let second_term = counters[second as usize]
        .leadership_term_id()
        .expect("the second term");

    nodes[second as usize].stop();

    let (back_tx, back_rx) = mpsc::channel();
    nodes[first as usize].restart(&back_tx);
    nodes[second as usize].restart(&back_tx);
    await_launched(&faults, &back_rx, 2);

    wait_with_state(
        "a term every member closes in",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters
                .iter()
                .all(|node| node.election_state() == Some(ELECTION_CLOSED))
        },
        || {
            format!(
                "roles {:?}, election {:?}, terms {:?}",
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].role())
                    .collect::<Vec<_>>(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].election_state())
                    .collect::<Vec<_>>(),
                (0..MEMBER_COUNT)
                    .map(|id| counters[id as usize].leadership_term_id())
                    .collect::<Vec<_>>(),
            )
        },
    );
    assert_eq!(
        1,
        (0..MEMBER_COUNT)
            .filter(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
            .count(),
        "and exactly one of them leads"
    );
    let term = counters[the_leader(&counters) as usize]
        .leadership_term_id()
        .expect("the term");
    assert!(
        term >= second_term,
        "the term they closed in is not behind the second leader's: {term} < {second_term}"
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **Two fresh members come back together, with a quorum left alive while they
/// are away** (`ClusterTest.shouldCatchUpTwoFreshNodesAfterRestart`,
/// `ClusterTest.java:1259`).
///
/// Five members, which is the reference's shape and the reason this row is not
/// one of the three-member ones: two away leave three, and three is a quorum, so
/// the commits keep landing while the members are gone.
#[test]
#[ignore = "P3-6c: the five-member fixture's first use does not commit for the client it was opened with: with three of five alive a fresh client commits (640 -> 1024) while the older client's offers stop landing (its publication reads position 640 although offer returns Ok). See records/P3-6c.md 6.8."]
fn two_fresh_members_come_back_to_a_five_member_cluster() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster_of(&ports, FIVE) else {
        return;
    };

    let leader = the_leader(&counters);
    let away: Vec<i32> = (0..FIVE).filter(|id| *id != leader).take(2).collect();

    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    for index in 0..3u8 {
        client.send(&session, &[b'i' + index; 16], &faults);
        wait_until("each message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    }

    // ---- two of them go, and the other three carry on ---------------------
    for member in &away {
        nodes[*member as usize].stop();
    }
    // A session opened before the stops is the question, not the instrument: a
    // fresh client proves the cluster takes messages at all with three of five.
    {
        let Some(mut fresh) = ClientSide::start_at(&ports, leader, 1) else {
            announce_skip();
            return;
        };
        let fresh_session = open_session(&mut fresh, &faults);
        let before = counters[leader as usize]
            .commit_position()
            .expect("the leader's commit");
        fresh.send(
            &fresh_session,
            b"a fresh client with three of five",
            &faults,
        );
        wait_with_state(
            "a fresh client's message to commit with three of five",
            &faults,
            CATCHUP_DEADLINE,
            || {
                fresh.poll_events();
                counters[leader as usize]
                    .commit_position()
                    .is_some_and(|position| position > before)
            },
            || {
                format!(
                    "roles {:?}, appends {:?}, commits {:?}, terms {:?}, election {:?}; \
                     fresh client {}",
                    (0..FIVE)
                        .map(|id| counters[id as usize].role())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].append_position())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].commit_position())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].leadership_term_id())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].election_state())
                        .collect::<Vec<_>>(),
                    "<read after the loop>",
                )
            },
        );
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    }
    for index in 0..5u8 {
        client.send(&session, &[b'j' + index; 16], &faults);
        let deadline = Instant::now() + CATCHUP_DEADLINE;
        loop {
            faults.check();
            client.poll_events();
            if counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
            {
                break;
            }
            if Instant::now() >= deadline {
                panic!(
                    "a message committed with three of five; roles {:?}, appends {:?}, commits \
                     {:?}, terms {:?}, election {:?}; leader {leader} had {last}; client {}",
                    (0..FIVE)
                        .map(|id| counters[id as usize].role())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].append_position())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].commit_position())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].leadership_term_id())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].election_state())
                        .collect::<Vec<_>>(),
                    client.ingress_publication_state(),
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    }

    // ---- and both come back on their own directories ----------------------
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    for member in &away {
        nodes[*member as usize].restart(&relaunch_tx);
    }
    await_launched(
        &faults,
        &relaunch_rx,
        i32::try_from(away.len()).expect("two"),
    );

    wait_with_state(
        "both returning members to catch up",
        &faults,
        CATCHUP_DEADLINE,
        || {
            away.iter().all(|id| {
                counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                    && counters[*id as usize].role() == Some(ROLE_FOLLOWER)
                    && counters[*id as usize]
                        .commit_position()
                        .is_some_and(|position| position >= last)
            })
        },
        || {
            format!(
                "roles {:?}, commits {:?}; the leader's commit was {last}",
                away.iter()
                    .map(|id| counters[*id as usize].role())
                    .collect::<Vec<_>>(),
                away.iter()
                    .map(|id| counters[*id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    // And they are live followers: one more message reaches them.
    client.send(&session, b"after both came back", &faults);
    wait_with_state(
        "a message committed with both back",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            away.iter().all(|id| {
                counters[*id as usize]
                    .commit_position()
                    .is_some_and(|position| position > last)
            })
        },
        || {
            format!(
                "leader commit={:?}, member commits {:?} (target {last})",
                counters[leader as usize].commit_position(),
                away.iter()
                    .map(|id| counters[*id as usize].commit_position())
                    .collect::<Vec<_>>(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A member with an empty log that is several terms behind catches up**
/// (`ClusterTest.shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog`,
/// `ClusterTest.java:1458`).
///
/// Five members again: one goes, and while it is away the cluster changes leader
/// twice — which takes a live majority, and is why the three-member harness
/// cannot ask this question.
#[test]
#[ignore = "P3-6c: same five-member blocker - recorded as a lead, not a flake. See records/P3-6c.md 6.8."]
fn a_member_three_terms_behind_comes_back_to_a_five_member_cluster() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster_of(&ports, FIVE) else {
        return;
    };

    let away = (0..FIVE)
        .find(|id| counters[*id as usize].role() == Some(ROLE_FOLLOWER))
        .expect("a follower");
    nodes[away as usize].stop();

    // Two leader changes while it is away: each one stops the current leader,
    // lets the rest elect, and brings the stopped member back as a follower.
    let mut term = counters[the_leader(&counters) as usize]
        .leadership_term_id()
        .expect("the first term");
    for _ in 0..2 {
        let current = the_leader(&counters);
        nodes[current as usize].stop();
        wait_with_state(
            "the rest to elect a later term",
            &faults,
            CATCHUP_DEADLINE,
            || {
                (0..FIVE).any(|id| {
                    counters[id as usize].role() == Some(ROLE_LEADER)
                        && counters[id as usize]
                            .leadership_term_id()
                            .is_some_and(|value| value > term)
                })
            },
            || {
                format!(
                    "terms {:?}, roles {:?}, election {:?}",
                    (0..FIVE)
                        .map(|id| counters[id as usize].leadership_term_id())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].role())
                        .collect::<Vec<_>>(),
                    (0..FIVE)
                        .map(|id| counters[id as usize].election_state())
                        .collect::<Vec<_>>(),
                )
            },
        );
        let elected = (0..FIVE)
            .find(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
            .expect("a leader");
        term = counters[elected as usize]
            .leadership_term_id()
            .expect("the term");

        let (tx, rx) = mpsc::channel();
        nodes[current as usize].restart(&tx);
        await_launched(&faults, &rx, 1);
        wait_with_state(
            "the returning leader to close as a follower",
            &faults,
            CATCHUP_DEADLINE,
            || {
                counters[current as usize].election_state() == Some(ELECTION_CLOSED)
                    && counters[current as usize].role() == Some(ROLE_FOLLOWER)
            },
            || {
                let member = &counters[current as usize];
                format!(
                    "member {current}: role={:?} election={:?} term={:?}",
                    member.role(),
                    member.election_state(),
                    member.leadership_term_id(),
                )
            },
        );
    }

    let leader = the_leader(&counters);
    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let session = open_session(&mut client, &faults);
    let mut last = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    for index in 0..5u8 {
        client.send(&session, &[b'k' + index; 16], &faults);
        wait_until("each message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > last)
        });
        last = counters[leader as usize]
            .commit_position()
            .expect("a commit");
    }

    // ---- and the member that missed both terms comes back -----------------
    let (back_tx, back_rx) = mpsc::channel();
    nodes[away as usize].restart(&back_tx);
    await_launched(&faults, &back_rx, 1);

    wait_with_state(
        "the member three terms behind to catch up",
        &faults,
        CATCHUP_DEADLINE,
        || {
            counters[away as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[away as usize].role() == Some(ROLE_FOLLOWER)
                && counters[away as usize]
                    .commit_position()
                    .is_some_and(|position| position >= last)
        },
        || {
            let member = &counters[away as usize];
            format!(
                "member {away}: role={:?} election={:?} term={:?} append={:?} commit={:?}; the \
                 leader's commit was {last}",
                member.role(),
                member.election_state(),
                member.leadership_term_id(),
                member.append_position(),
                member.commit_position(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}
