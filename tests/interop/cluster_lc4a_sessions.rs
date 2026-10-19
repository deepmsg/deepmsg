//! LC-4a's session scenario: **a session opens, closes on the client's word,
//! and times out when the client vanishes**.
//!
//! The reference's cluster suite asserts session lifecycle through
//! `TestNode.TestService`'s counters and close reasons — a stub-service
//! assertion, readable only while the business is in the test's own process.
//! This is the in-process arm, and it is the only arm: an external container's
//! business state cannot be read, and the client-visible half (a session
//! exists, then does not) is what every other scenario already exercises. The
//! loss is recorded in `records/P3-2c.md` as `n/a:external-injection`.
//!
//! Two paths, and they are different on purpose:
//!
//! * a client that closes normally sends a close-session request, and the
//!   business sees `on_session_close(CLIENT_ACTION)`;
//! * a client that **vanishes** sends nothing, and the consensus module closes
//!   the session after `aeron.cluster.session.timeout` — the business sees
//!   `TIMEOUT`. The node is started with a two-second timeout so the test does
//!   not wait ten.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_tests::cluster::{MemberSpec, Options, TestCluster};

const CLUSTER_ID: i32 = 11;
const SESSION_TIMEOUT: &str = "2s";

static SERIAL: Mutex<()> = Mutex::new(());

fn start(test_name: &str) -> Option<TestCluster> {
    let options = Options {
        cluster_id: CLUSTER_ID,
        probes: vec![
            "ClusterEchoClientProbe.java",
            "ClusterAbandonSessionProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterEchoTimerService.java",
        ],
        node_properties: vec![(
            "aeron.cluster.session.timeout".to_owned(),
            SESSION_TIMEOUT.to_owned(),
        )],
        ..Options::default()
    };
    TestCluster::start(
        test_name,
        &[MemberSpec::test_container(0, "test-service")],
        options,
    )
}

fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn a_session_opens_closes_and_times_out() {
    let _serial = SERIAL.lock().expect("the serial lock");
    let Some(mut cluster) = start("lc4a-sessions") else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };
    let aeron_dir = cluster.aeron_dir(0).display().to_string();

    cluster.await_active();

    let state = cluster
        .test_service(0, 0)
        .expect("the test business's state");

    // A client that connects, sends one message and closes: one open, one
    // `CLIENT_ACTION` close.
    let reading = cluster.run_probe("ClusterEchoClientProbe", &[aeron_dir.as_str(), "1", "20"]);
    assert!(
        reading.contains("RECEIVED 1"),
        "the client was answered, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );
    wait_for("the client's session to open", || {
        state.session_opens() >= 1
    });
    wait_for("the client's session to close", || {
        state.session_closes() >= 1
    });
    assert_eq!(
        state.close_reasons(),
        vec![CloseReason::CLIENT_ACTION],
        "a client that closes is a CLIENT_ACTION, not a timeout"
    );

    // A client that joins, is answered, and **halts**: nothing is sent, so the
    // only thing that can close the session is the session timeout.
    let reading = cluster.run_probe("ClusterAbandonSessionProbe", &[aeron_dir.as_str(), "20"]);
    assert!(
        reading.contains("ABANDONED"),
        "the second client's session was open before it vanished, client said: {reading}\n\
         node log:\n{}",
        cluster.node_log(0)
    );

    wait_for("the abandoned session to open", || {
        state.session_opens() >= 2
    });
    wait_for("the abandoned session to time out", || {
        state.session_closes() >= 2
    });
    assert_eq!(
        state.close_reasons(),
        vec![CloseReason::CLIENT_ACTION, CloseReason::TIMEOUT],
        "the session whose client vanished was a TIMEOUT"
    );

    cluster.shutdown();
}
