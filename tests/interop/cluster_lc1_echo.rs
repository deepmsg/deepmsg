//! LC-1's second step: a message goes round a Java cluster and the session
//! closes.
//!
//! `cluster_lc1_connect` proved the connect. This proves the two directions and
//! the end of the session: our `AeronCluster` offers a message on the ingress,
//! the reference's consensus module sequences it, our `echo` business answers
//! it, and the answer comes back on the egress and reaches a listener. Then
//! `close` sends a `SessionCloseRequest`, and the business is asked to close the
//! session — which the fixture's test business records, so the assertion is the
//! far end's, not ours.
//!
//! # Why the fixture's own business
//!
//! The product `echo` answers the same way but has no observables: an external
//! process's service state is not readable. [`MemberSpec::test_container`] is
//! the in-process business that records `on_session_close` and its reason
//! (`CloseReason::CLIENT_ACTION` here), which is the only way to tell a close
//! request that arrived from one that did not.
//!
//! # One at a time
//!
//! The test owns a whole cluster and a Java process; [`SERIAL`] is there for
//! the second test this binary gains.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster::{MemberSpec, Options, TestCluster};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

const DEADLINE: Duration = Duration::from_secs(30);
const PAYLOAD: &[u8] = b"hello cluster";

/// What the egress delivered.
#[derive(Default)]
struct Received {
    messages: Vec<Vec<u8>>,
}

struct SharedListener(Rc<RefCell<Received>>);

impl EgressListener for SharedListener {
    fn on_message(
        &mut self,
        _cluster_session_id: i64,
        _timestamp: i64,
        payload: &[u8],
        _header: &MessageHeader,
    ) {
        self.0.borrow_mut().messages.push(payload.to_vec());
    }

    fn on_session_event(
        &mut self,
        _correlation_id: i64,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _code: EventCode,
        _detail: &str,
    ) {
    }
}

/// Our client sends a message, reads the echo and closes the session.
#[test]
fn our_client_offers_a_message_and_closes_the_session() {
    let _serial = SERIAL.lock().expect("the serial lock");

    let member = MemberSpec::test_container(0, "test-service");
    let Some(mut cluster) = TestCluster::start("lc1-echo", &[member], Options::default()) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();

    let received = Rc::new(RefCell::new(Received::default()));
    let mut client = cluster.client();

    let context = AeronClusterContext::default()
        .ingress_channel(Some("aeron:ipc"))
        .egress_channel(Some("aeron:ipc"))
        .client_name("deepmsg-lc1-echo")
        .message_timeout_ns(i64::try_from(DEADLINE.as_nanos()).expect("a timeout that fits an i64"))
        .egress_listener(Box::new(SharedListener(Rc::clone(&received))));

    let mut session = AeronCluster::connect(context, &mut client).unwrap_or_else(|error| {
        panic!(
            "our client did not connect: {error}\nnode log:\n{}",
            cluster.node_log(0)
        )
    });

    let offered = session.offer(&mut client, PAYLOAD);
    assert!(
        offered.is_some(),
        "the connect request was accepted, so the session has an ingress publication"
    );

    // Read until the echo comes back: the client has to be driven, and the
    // session polled, in the same loop.
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        let _ = client.poll();
        session.poll_egress(&mut client).unwrap_or_else(|error| {
            panic!(
                "our client failed to poll egress: {error}\nnode log:\n{}",
                cluster.node_log(0)
            )
        });

        if received
            .borrow()
            .messages
            .iter()
            .any(|bytes| bytes == PAYLOAD)
        {
            break;
        }

        std::thread::yield_now();
    }

    assert!(
        received
            .borrow()
            .messages
            .iter()
            .any(|bytes| bytes == PAYLOAD),
        "the echo came back; got {:?}\nnode log:\n{}",
        received.borrow().messages,
        cluster.node_log(0)
    );

    session.close(&mut client);

    // The service records the close and its reason; the request has to reach
    // the cluster and be sequenced for the reason to appear.
    let service = cluster
        .test_service(0, 0)
        .expect("the fixture's test business is in process");
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        let _ = client.poll();
        if service
            .close_reasons()
            .contains(&CloseReason::CLIENT_ACTION)
        {
            break;
        }
        std::thread::yield_now();
    }

    assert!(
        service
            .close_reasons()
            .contains(&CloseReason::CLIENT_ACTION),
        "the service saw the session close because the client asked for it; \
         reasons: {:?}\nnode log:\n{}",
        service.close_reasons(),
        cluster.node_log(0)
    );

    cluster.shutdown();
}
