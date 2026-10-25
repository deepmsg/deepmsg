//! LC-3's kit: a member table with **both** consensus modules in it, the
//! ports, the waits, and the judgements that say what a mixed cluster has to
//! do.
//!
//! The two halves a case varies are [`Topology`] (which members are ours) and
//! [`LeaderImpl`] (which implementation the cluster is told to lead). Every
//! member's container is this build's business, which LC-1 and LC-4a already
//! proved on both kinds of node — a red here is about the consensus modules
//! meeting each other.
//!
//! # The appointment
//!
//! Leadership is pinned with `aeron.cluster.appointed.leader.id` rather than
//! left to whichever member wins a race. There are two ways to do it and both
//! are here:
//!
//! * [`Leader::Everyone`] — every member is told the same id. That is the
//!   deployment shape, and it is what the scenarios that never kill a leader
//!   use.
//! * [`Leader::OnlyThatMember`] — only the member that is meant to lead is
//!   told. It is the only way a *kill-the-leader* case can work: the reference
//!   rules that a member appointed to somebody else never nominates
//!   (`Election.java:715-718`), so with every member told the same dead id the
//!   survivors canvass forever. Telling just the intended leader keeps the
//!   startup deterministic **and** leaves the survivors an election.
//!
//! # What the judgements can and cannot separate yet
//!
//! Every scenario is judged by what a client sees (messages in, messages out),
//! by the role and commit-position counters read on each member's own driver,
//! and by whether the members that were away come back to the leader's
//! position. **Which path a returning member took — log replication, or a
//! catch-up from the leader's archive — is not judged here**: that needs the
//! recordings' bytes, and those are P3-7c's rulers. The catch-up and
//! replication scenarios differ in what happens while the member is away (a
//! snapshot in between, or not), so the paths are exercised; they are proven
//! later.

use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::{
    CLUSTER_COMMIT_POSITION_TYPE_ID, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID,
    CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
};
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::admin_response_code::AdminResponseCode;

use crate::cluster::{ClusterPorts, MemberSpec, Options, ProcessMode, TestCluster};
use crate::cluster_client::{Session, base_context, send_and_await_echo};

/// How many members every topology here has.
pub const MEMBERS: i32 = 3;
/// How long a start, a stop or a wait may take before the case is a failure.
pub const DEADLINE: Duration = Duration::from_secs(60);

/// `Cluster.Role.LEADER` (`Cluster.java:51-68`).
pub const ROLE_LEADER: i64 = 2;
/// `Cluster.Role.FOLLOWER` (`Cluster.java:51-68`). `CANDIDATE` is 1 and is
/// never a settled state.
pub const ROLE_FOLLOWER: i64 = 0;

/// Which implementations a cluster's members are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topology {
    /// One member of ours, two of the reference's.
    OneRustTwoJava,
    /// Two of ours, one of the reference's.
    TwoRustOneJava,
}

impl Topology {
    /// Every topology, in the order the matrix runs them.
    pub const ALL: [Self; 2] = [Self::OneRustTwoJava, Self::TwoRustOneJava];

    /// The member table: one node per member, every container this build's
    /// `echo` business.
    #[must_use]
    pub fn members(self, mode: ProcessMode) -> Vec<MemberSpec> {
        match self {
            Self::OneRustTwoJava => vec![
                MemberSpec::rust_node(0, "echo", mode),
                MemberSpec::our_container(1, "echo", mode),
                MemberSpec::our_container(2, "echo", mode),
            ],
            Self::TwoRustOneJava => vec![
                MemberSpec::rust_node(0, "echo", mode),
                MemberSpec::rust_node(1, "echo", mode),
                MemberSpec::our_container(2, "echo", mode),
            ],
        }
    }

    /// The members this topology runs with our consensus module.
    #[must_use]
    pub fn rust_members(self) -> &'static [i32] {
        match self {
            Self::OneRustTwoJava => &[0],
            Self::TwoRustOneJava => &[0, 1],
        }
    }

    /// The members it runs with the reference's.
    #[must_use]
    pub fn java_members(self) -> &'static [i32] {
        match self {
            Self::OneRustTwoJava => &[1, 2],
            Self::TwoRustOneJava => &[2],
        }
    }

    /// A member of `implementation`, for the cases that need one.
    #[must_use]
    pub fn member_of(self, implementation: LeaderImpl) -> i32 {
        match implementation {
            LeaderImpl::Rust => self.rust_members()[0],
            LeaderImpl::Java => self.java_members()[0],
        }
    }
}

/// Which implementation the matrix asks to lead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaderImpl {
    /// Ours.
    Rust,
    /// The reference's.
    Java,
}

impl LeaderImpl {
    /// Both directions, in the order the matrix runs them.
    pub const ALL: [Self; 2] = [Self::Rust, Self::Java];

    /// The other one — the implementation a case expects to take over.
    #[must_use]
    pub fn other(self) -> Self {
        match self {
            Self::Rust => Self::Java,
            Self::Java => Self::Rust,
        }
    }
}

/// Who is told who the leader is, which decides what a leader's death does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leader {
    /// Every member is told, the way a deployment configures it. Leadership is
    /// then deterministic — and the cluster **stalls** if that member dies, by
    /// the reference's design: a member appointed to somebody else never
    /// nominates (`Election.java:715-718`).
    Everyone(i32),
    /// Nobody is told, so leadership is whoever wins the election. The only
    /// shape in which a leader's death leaves a failover.
    Nobody,
}

/// The cluster-wide settings every case here starts from.
///
/// The timer supplier is written down rather than left to the default: it is a
/// **byte contract**, not a performance knob (plan §7.4 point 3), and the
/// snapshot comparison this card exists for is only meaningful when both sides
/// iterate the same container.
#[must_use]
pub fn options(base: u16) -> Options {
    Options {
        cluster_id: CLUSTER_ID,
        members: Some(ClusterPorts::new(base).members_string(MEMBERS)),
        ingress_channel: "aeron:udp?term-length=128k".to_owned(),
        consensus_channel: "aeron:udp?term-length=64k".to_owned(),
        log_channel: "aeron:udp?term-length=64k".to_owned(),
        node_properties: vec![(
            "aeron.cluster.timer.service.supplier".to_owned(),
            "io.aeron.cluster.WheelTimerServiceSupplier".to_owned(),
        )],
        ..Options::default()
    }
}

/// The cluster id every case here uses.
pub const CLUSTER_ID: i32 = 21;

/// [`options`], with the leadership pinned the way `leader` says.
#[must_use]
pub fn options_with(options: Options, leader: Leader) -> Options {
    let mut options = options;
    if let Leader::Everyone(id) = leader {
        options.node_properties.push(appointment(id));
    }
    options
}

/// What the reference's default authorisation service refuses and
/// `SNAPSHOT`'s case needs: an admin request only reaches the cluster when the
/// service allows it (`ClusterTest.shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot`).
#[must_use]
pub fn allowing_every_command(mut options: Options) -> Options {
    options.node_properties.push((
        "aeron.cluster.authorisation.service.supplier".to_owned(),
        "ALLOW_ALL".to_owned(),
    ));
    options
}

fn appointment(id: i32) -> (String, String) {
    (
        "aeron.cluster.appointed.leader.id".to_owned(),
        id.to_string(),
    )
}

/// Every member's own log, for a failure message.
#[must_use]
pub fn node_logs(cluster: &TestCluster) -> String {
    (0..MEMBERS)
        .map(|member| format!("member {member}:\n{}", cluster.node_log(member)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The skip this suite prints when the reference or this build's half is not
/// there.
pub fn skip(what: &str) {
    eprintln!(
        "SKIPPED: {what}: the reference jar, java, our driver or our consensus module binary is \
         missing"
    );
}

/// Start the cluster and wait until every member has a role and exactly one is
/// the leader.
pub fn start(
    test_name: &str,
    members: &[MemberSpec],
    options: Options,
) -> Option<(TestCluster, i32)> {
    let cluster = TestCluster::start(test_name, members, options)?;
    let leader = await_all_joined(&cluster);
    Some((cluster, leader))
}

/// Every member's role counter, or `None` for a member whose module has not
/// published one.
#[must_use]
pub fn roles(cluster: &TestCluster) -> Vec<Option<i64>> {
    (0..MEMBERS)
        .map(|member| cluster.counter_on(member, CLUSTER_NODE_ROLE_TYPE_ID))
        .collect()
}

/// Wait until every member has a role and exactly one is the leader.
///
/// [`TestCluster::await_leader`] answers as soon as *one* member says LEADER,
/// which on a cluster that is still joining can be a member the others have not
/// been canvassed by yet.
pub fn await_all_joined(cluster: &TestCluster) -> i32 {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let roles = roles(cluster);
        // `Some(0)` is a **follower**, which is a role of its own
        // (`Cluster.java:51-68`: FOLLOWER=0, CANDIDATE=1, LEADER=2).
        if roles.iter().all(Option::is_some)
            && roles
                .iter()
                .filter(|role| **role == Some(ROLE_LEADER))
                .count()
                == 1
        {
            return member_with_role(&roles, ROLE_LEADER);
        }
        assert!(
            Instant::now() < deadline,
            "the members never agreed on one leader\n{}",
            node_logs(cluster)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// One leader and two followers, read on each member's own driver.
pub fn assert_one_leader(cluster: &TestCluster, leader: i32) {
    let roles = roles(cluster);
    let leaders = roles
        .iter()
        .filter(|role| **role == Some(ROLE_LEADER))
        .count();
    assert_eq!(
        leaders,
        1,
        "member {leader} is the leader and {leaders} members say LEADER: {roles:?}\n{}",
        node_logs(cluster)
    );
    for member in 0..MEMBERS {
        if member != leader {
            assert_eq!(
                roles[usize::try_from(member).expect("a member id")],
                Some(ROLE_FOLLOWER),
                "member {member} is not the leader, so it is a follower\n{}",
                node_logs(cluster)
            );
        }
    }
}

fn member_with_role(roles: &[Option<i64>], role: i64) -> i32 {
    i32::try_from(
        roles
            .iter()
            .position(|candidate| *candidate == Some(role))
            .expect("a member with that role"),
    )
    .expect("a member id")
}

/// Our client, on its own driver, pointed at every member's ingress.
pub fn connect(label: &str, base: u16, cluster: &TestCluster) -> Option<Session> {
    let ports = ClusterPorts::new(base);
    let mut context = base_context(label, DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));
    // An election on this machine takes well under a second, and a case that
    // follows a new leader should measure the cluster, not the client's
    // patience.
    context.new_leader_timeout_ns =
        Some(i64::try_from(DEADLINE.as_nanos()).expect("a timeout that fits an i64"));
    Session::connect(label, context, &[], || node_logs(cluster))
}

/// Offer `from..to` and wait for each echo.
pub fn echo(session: &mut Session, from: u64, to: u64) {
    let deadline = Instant::now() + DEADLINE;
    for sequence in from..to {
        send_and_await_echo(session, sequence, deadline);
    }
}

/// Stop one member: its containers first, then its node.
///
/// The order matters: a container outliving its node ends by itself, and a case
/// that wants to see the end watches it.
pub fn stop_member(cluster: &mut TestCluster, member: i32) {
    for service_id in service_ids(cluster, member) {
        cluster.stop_container(member, service_id);
    }
    cluster.stop_node(member);
}

/// Start one member again over the same directories: its node, then its
/// containers.
pub fn start_member(cluster: &mut TestCluster, member: i32) {
    cluster.start_node_by_id(member);
    for service_id in service_ids(cluster, member) {
        cluster.start_container(member, service_id);
    }
}

fn service_ids(cluster: &TestCluster, member: i32) -> Vec<i32> {
    cluster
        .members()
        .filter(|spec| spec.id == member)
        .map(|spec| spec.service_id)
        .collect()
}

/// The member's commit position, as its own driver publishes it.
#[must_use]
pub fn commit_position(cluster: &TestCluster, member: i32) -> Option<i64> {
    cluster.counter_on(member, CLUSTER_COMMIT_POSITION_TYPE_ID)
}

/// The member's leadership term, as its own driver publishes it.
#[must_use]
pub fn leadership_term(cluster: &TestCluster, member: i32) -> Option<i64> {
    cluster.counter_on(member, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID)
}

/// How many snapshots the member has taken.
#[must_use]
pub fn snapshots_taken(cluster: &TestCluster, member: i32) -> Option<i64> {
    cluster.counter_on(member, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)
}

/// Ask the cluster for a snapshot and wait until every member in `expected`
/// has taken one.
///
/// `expected` is not always every member: a case that takes a snapshot while a
/// member is away has one whose counter can never move.
pub fn take_a_snapshot(cluster: &TestCluster, session: &mut Session, expected: &[i32]) {
    assert!(
        session
            .session
            .send_admin_request_to_take_a_snapshot(&mut session.client, 42),
        "the session has an ingress publication to send the request on"
    );

    let deadline = Instant::now() + DEADLINE;
    assert!(
        session.poll_until(deadline, |session| {
            !session.received.borrow().admin_responses.is_empty()
        }),
        "the cluster never answered the admin request\n{}",
        node_logs(cluster)
    );
    let responses = session.received.borrow().admin_responses.clone();
    assert!(
        responses.iter().any(
            |response| response.request_type == AdminRequestType::SNAPSHOT
                && response.response_code == AdminResponseCode::OK
                && response.message.is_empty()
        ),
        "the cluster accepted the snapshot request: {responses:?}"
    );

    // The toggle is in the log, so every member takes its own snapshot when it
    // reaches that position — the leader first, the followers on their way
    // through.
    let taken = session.poll_until(deadline, |_| {
        expected
            .iter()
            .all(|member| snapshots_taken(cluster, *member).is_some_and(|count| count >= 1))
    });
    assert!(
        taken,
        "not every member took the snapshot: {:?}\n{}",
        expected
            .iter()
            .map(|member| snapshots_taken(cluster, *member))
            .collect::<Vec<_>>(),
        node_logs(cluster)
    );
}

/// Wait until every member's commit position is the leader's, and hand it back.
///
/// **The wait drives the client.** A wait that blocks without polling the
/// session is a wait that lets the client's own driver reap it — LC-1's
/// failover case learned that the hard way, and a catch-up takes long enough
/// to be the same mistake.
pub fn await_everyone_at_the_leaders_position(
    cluster: &TestCluster,
    leader: i32,
    session: &mut Session,
) -> i64 {
    let deadline = Instant::now() + DEADLINE;
    let mut answer = None;
    let agreed = session.poll_until(deadline, |_| {
        let positions: Vec<Option<i64>> = (0..MEMBERS)
            .map(|member| commit_position(cluster, member))
            .collect();
        let position = commit_position(cluster, leader);
        if position.is_some() && positions.iter().all(|other| *other == position) {
            answer = position;
            return true;
        }
        false
    });
    assert!(
        agreed,
        "the members never agreed on a commit position\n{}",
        node_logs(cluster)
    );
    answer.expect("a commit position every member agreed on")
}

/// Wait until `member`'s commit position reaches `position`, as a follower.
///
/// Driven by the client, like [`await_everyone_at_the_leaders_position`].
pub fn await_caught_up(cluster: &TestCluster, member: i32, position: i64, session: &mut Session) {
    let deadline = Instant::now() + DEADLINE;
    let caught_up = session.poll_until(deadline, |_| {
        commit_position(cluster, member).is_some_and(|reached| reached >= position)
    });
    assert!(
        caught_up,
        "member {member} never caught up to {position}: it is at {:?}\n{}",
        commit_position(cluster, member),
        node_logs(cluster)
    );
    assert_eq!(
        cluster.counter_on(member, CLUSTER_NODE_ROLE_TYPE_ID),
        Some(ROLE_FOLLOWER),
        "the member that caught up is a follower\n{}",
        node_logs(cluster)
    );
}
