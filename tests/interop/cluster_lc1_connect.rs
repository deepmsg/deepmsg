//! LC-1's first step: **our client connects to the reference's cluster**.
//!
//! Everything else in this workspace's cluster suite has the reference's client
//! on the near end (`cluster_lc4a_*`) or our consensus module on the far end
//! (`cluster_lc4b`). Here the near end is `deepmsg-cluster-client`: its
//! `AsyncConnect` adds an egress subscription and an ingress publication to the
//! **reference Java node's own driver**, sends a `SessionConnectRequest` over
//! IPC, and reads the `SessionEvent` the reference's `ConsensusModuleAgent`
//! answers with. A green test means the connect request's bytes, the answer's
//! bytes and the state machine between them all agree with the reference.
//!
//! # What is ours and what is the reference's
//!
//! * node — the reference's `ClusteredMediaDriver` (Java), started by
//!   [`TestCluster`];
//! * container — our own `echo` business on a thread of this process, so the
//!   node has a service to ack its log and become ACTIVE;
//! * client — ours, the subject;
//! * channels — `aeron:ipc` both ways, which is the reference's own test setup
//!   (`ClusterEchoClientProbe.java`).
//!
//! # One at a time
//!
//! The test owns a whole cluster on `/dev/shm` names and a Java process. The
//! binary has one test today; [`SERIAL`] is there for the second.

use std::sync::Mutex;
use std::time::Duration;

use deepmsg_cluster_client::{AeronCluster, AeronClusterContext};
use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

/// How long the connect may take. The node is ACTIVE before this starts; the
/// budget is for the request's round trip, not for a cluster coming up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Our client opens a session on a single-node Java cluster.
#[test]
fn our_client_connects_to_a_java_single_node() {
    let _serial = SERIAL.lock().expect("the serial lock");

    let member = MemberSpec::our_container(0, "echo", ProcessMode::InProcess);
    let Some(mut cluster) = TestCluster::start("lc1-connect", &[member], Options::default()) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    // ACTIVE means the module is leading and the container has acked its log —
    // the state in which it serves a connect.
    cluster.await_active();

    let mut client = cluster.client();

    let context = AeronClusterContext::default()
        .ingress_channel(Some("aeron:ipc"))
        .egress_channel(Some("aeron:ipc"))
        .client_name("deepmsg-lc1-connect")
        .message_timeout_ns(
            i64::try_from(CONNECT_TIMEOUT.as_nanos()).expect("a timeout that fits an i64"),
        );

    let mut session = AeronCluster::connect(context, &mut client).unwrap_or_else(|error| {
        panic!(
            "our client did not connect: {error}\nnode log:\n{}",
            cluster.node_log(0)
        )
    });

    assert_eq!(
        0,
        session.leader_member_id(),
        "a single-member cluster's leader is member 0\nnode log:\n{}",
        cluster.node_log(0)
    );
    assert!(
        session.cluster_session_id() > 0,
        "the cluster assigned a session id, got {}\nnode log:\n{}",
        session.cluster_session_id(),
        cluster.node_log(0)
    );
    assert!(
        session.leadership_term_id() >= 0,
        "the answer named a leadership term, got {}",
        session.leadership_term_id()
    );
    assert!(
        session.egress_image().is_some(),
        "the answer arrived on an egress image, and the session remembers which"
    );
    assert_eq!(
        Some(2 * 10 * 1_000 * 1_000_000),
        session.new_leader_timeout_ns(),
        "no heartbeat in the answer, so the reference's ten-second default doubled"
    );

    session.close(&mut client);

    cluster.shutdown();
}
