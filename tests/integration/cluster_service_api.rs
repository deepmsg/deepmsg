//! P3-12's acceptance: the service container's public API, end to end.
//!
//! One member, one in-process `api-service` (see `deepmsg_tests::cluster`),
//! one client. The business is told what to do by the first byte of a session
//! message and records what each door answered; the client reads what came out
//! of the cluster. Every door of `Cluster.java:130-387` that this card added is
//! exercised here, and the two that were already there (`offer`,
//! `close_client_session`) stay untouched so a change to them shows up as a
//! failure somewhere else.
//!
//! # What each message asks for
//!
//! | Byte | Door |
//! |---|---|
//! | `v` | `ClientSession.offer_vectored` and `Cluster.offer_vectored` |
//! | `k` | `ClientSession.try_claim` and `Cluster.try_claim` |
//! | `f` | `Cluster.for_each_client_session` |
//! | `m` | `Cluster.aeron()` — a publication the container makes itself |
//! | `x` | `ClientSession.close()` |
//!
//! `Cluster.context()` is read in `on_start`, before any of them.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster::{API_MARKET_STREAM_ID, SNAPSHOT_MARKET_MARKER};
use deepmsg_tests::cluster_harness::{
    CLUSTER_ID, ClientSide, Faults, Node, NodeCounters, Ports, SessionEvent, announce_skip,
    await_launched, await_snapshot_count, ingress_message, members_of, offer_message,
    take_snapshot, wait_until,
};

/// The harness's port blocks are per process and `cargo test` runs threads in
/// one, so these cases take turns.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A port block of this case's own.
fn ports() -> Ports {
    static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    let base = *BASE.get_or_init(|| Ports::new().base());
    Ports::with_base(base + 64 * 7)
}

/// One member running the API business, or `None` when this machine cannot
/// stand a driver up.
fn start(faults: &Faults) -> Option<Node> {
    let ports = ports();
    let member_string = members_of(&ports, 1);
    let (launched_tx, launched_rx) = mpsc::channel();

    let node = Node::start_with_service(
        0,
        &ports,
        &member_string,
        faults,
        &launched_tx,
        "api-service",
    )?;
    await_launched(faults, &launched_rx, 1);

    Some(node)
}

/// `Cluster.context()`: the facts a business reads about its own container.
#[test]
fn the_business_reads_its_containers_configuration() {
    let _serial = serial();
    let faults = Faults::default();
    let Some(node) = start(&faults) else {
        announce_skip();
        return;
    };

    let state = node.api_state().expect("the API business is running");
    wait_until("the business to read its context", &faults, || {
        state.cluster_id.load(std::sync::atomic::Ordering::Acquire) != 0
    });

    assert_eq!(
        0,
        state.service_id.load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        i64::from(CLUSTER_ID),
        state.cluster_id.load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        node.aeron_dir().display().to_string(),
        *state.aeron_dir.lock().expect("the directory lock"),
        "aeronDirectoryName is the container's client directory"
    );
    assert!(state.faults().is_empty(), "{:?}", state.faults());
}

/// The four message doors, over one session.
#[test]
fn every_door_of_the_containers_api_answers() {
    let _serial = serial();
    let faults = Faults::default();
    let Some(node) = start(&faults) else {
        announce_skip();
        return;
    };
    let state = node.api_state().expect("the API business is running");

    let ports = ports();
    let mut client = match ClientSide::start(&ports, 0) {
        Some(client) => client,
        None => {
            announce_skip();
            return;
        }
    };
    let session = client.connect_session(&faults);
    assert_eq!(EventCode::OK, session.code, "{session:?}");

    // A subscription of the case's own, before the business is asked to make
    // its publication: a publication with no subscriber is not connected, and
    // the first offer would be refused.
    let market = client.add_subscription(&ports.client_egress_channel(), API_MARKET_STREAM_ID);

    // `v` — parts on both doors.
    send(&client, &session, b"v", &faults);
    let vec = wait_for_egress(&mut client, 2, &faults);
    assert_eq!(
        vec.len(),
        2,
        "an egress message each from offer_vectored and offer"
    );
    assert_eq!(
        vec[0], vec[1],
        "the vectored message and the single-buffer one are the same bytes — \
         the header and the two parts joined"
    );
    assert_eq!(
        b"vec-onevec-two",
        &vec[0][SESSION_HEADER_LENGTH..],
        "the parts arrive in order, after the sticky header"
    );

    // `k` — a claim on both doors.
    send(&client, &session, b"k", &faults);
    let claimed = wait_for_egress(&mut client, 1, &faults);
    assert_eq!(1, claimed.len(), "one egress claim");
    assert_eq!(SESSION_HEADER_LENGTH + 5, claimed[0].len());
    assert_eq!(b"claim", &claimed[0][SESSION_HEADER_LENGTH..]);

    // `f` — every session, once.
    send(&client, &session, b"f", &faults);
    let fanned = wait_for_egress(&mut client, 1, &faults);
    assert_eq!(1, fanned.len());
    assert_eq!(b"fan", &fanned[0][SESSION_HEADER_LENGTH..]);
    assert_eq!(
        1,
        state.visits.load(std::sync::atomic::Ordering::Acquire),
        "one session was visited"
    );
    assert_eq!(
        1,
        state.fanned_out.load(std::sync::atomic::Ordering::Acquire),
        "and it took the offer"
    );

    // `m` — a publication the container made through `aeron()`.
    //
    // Asked for more than once: a publication is created before it is
    // connected, so the business's first offer can be refused while its SM is
    // still finding the client's subscription. Every `m` also answers with an
    // egress ack, which is drained and not asserted on.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut market_payloads = Vec::new();
    while Instant::now() < deadline && market_payloads.is_empty() {
        send(&client, &session, b"m", &faults);
        market_payloads.extend(poll_payloads_for(
            &mut client,
            market,
            Duration::from_millis(500),
        ));
        let _ = client.poll_egress();
    }
    assert_eq!(
        vec![b"market".to_vec()],
        market_payloads,
        "the client's own driver received what the container published"
    );
    assert!(
        state
            .market_publication
            .load(std::sync::atomic::Ordering::Acquire)
            > 0,
        "and it answered with the publication's registration id"
    );

    // The claims, on both doors, committed.
    assert_eq!(2, state.claimed.load(std::sync::atomic::Ordering::Acquire));

    // `x` — the session's own close, which is `Cluster.close_client_session`'s
    // path (`ContainerClientSession.java:73-79`).
    send(&client, &session, b"x", &faults);

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && state.closes.load(std::sync::atomic::Ordering::Acquire) == 0
    {
        let _ = client.poll_egress();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(1, state.closes.load(std::sync::atomic::Ordering::Acquire));

    let mut close = None;
    while Instant::now() < deadline && close.is_none() {
        let (events, _) = client.poll_egress();
        close = events
            .into_iter()
            .find(|event| event.code == EventCode::CLOSED);
    }
    let close = close.expect("the client was told the session closed");
    assert_eq!(
        "SERVICE_ACTION", close.detail,
        "a session a business closed says so, not CLIENT_ACTION \
         (`SessionManager.java:459`, `:469`)"
    );

    assert!(state.faults().is_empty(), "{:?}", state.faults());
}

/// **The snapshot callback publishes on the business's own publication**, which
/// is the door `Cluster::aeron()` cannot give it: that borrow ends with the
/// callback that took it, and `on_take_snapshot` is handed no `Cluster`.
///
/// This is the counterpart of the reference's business holding the `Aeron` it
/// took out of `onStart` (`Cluster.java:151`); `SnapshotWriter::aeron()` answers
/// the same client, read-only, so a publication can be offered on from the
/// snapshot and cannot be created or removed there.
///
/// Two judgements, and the second is what makes the first mean something:
///
/// 1. the marker reaches the client's subscriber **byte for byte**, once, and
///    the business's own counter says it was the snapshot callback that
///    offered it;
/// 2. the snapshot the same callback wrote **still recovers**, to the state the
///    business put in it through the writer's own door — a frame that leaked
///    into the recording, or a recording the extra offer interrupted, fails
///    here.
///
/// The instrument can fail: comment out the `writer.aeron().offer(..)` in the
/// business and the first judgement is red; stop writing the record and the
/// second is.
#[test]
fn a_snapshot_publishes_on_the_containers_own_publication() {
    let _serial = serial();
    let faults = Faults::default();
    let Some(mut node) = start(&faults) else {
        announce_skip();
        return;
    };
    let state = node.api_state().expect("the API business is running");

    let ports = ports();
    let mut client = match ClientSide::start(&ports, 0) {
        Some(client) => client,
        None => {
            announce_skip();
            return;
        }
    };
    let session = client.connect_session(&faults);
    assert_eq!(EventCode::OK, session.code, "{session:?}");

    // A subscription of the case's own, before the publication exists: a
    // publication with no subscriber is not connected and its first offer is
    // refused.
    let market = client.add_subscription(&ports.client_egress_channel(), API_MARKET_STREAM_ID);

    // `m`: the publication is made from `Cluster::aeron()` — P3-12's door, in a
    // callback that does get a `Cluster`. Asked for more than once because a
    // publication is created before it is connected.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    while Instant::now() < deadline && seen.is_empty() {
        send(&client, &session, b"m", &faults);
        seen.extend(poll_payloads_for(
            &mut client,
            market,
            Duration::from_millis(500),
        ));
        let _ = client.poll_egress();
    }
    assert_eq!(
        vec![b"market".to_vec()],
        seen,
        "the business's own publication reaches the client's driver"
    );
    assert!(
        state
            .market_publication
            .load(std::sync::atomic::Ordering::Acquire)
            > 0,
        "and it answered with the registration id"
    );

    // The SNAPSHOT toggle: the cluster takes one at this log position, and the
    // business's `on_take_snapshot` runs inside it.
    let counters = NodeCounters::open(node.aeron_dir());
    take_snapshot(&counters);
    await_snapshot_count(&counters, &faults, 1);

    // (1) The marker crossed, once, byte for byte.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut markers = 0;
    while Instant::now() < deadline && markers == 0 {
        faults.check();
        for payload in client.poll_payloads(market) {
            if payload == SNAPSHOT_MARKET_MARKER {
                markers += 1;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        1, markers,
        "the subscriber received the frame the snapshot callback published on the business's \
         own publication, exactly once"
    );
    assert_eq!(
        1,
        state
            .snapshot_market_offers
            .load(std::sync::atomic::Ordering::Acquire),
        "and it was the snapshot callback that offered it"
    );
    assert!(state.faults().is_empty(), "{:?}", state.faults());

    // (2) The snapshot recovers, to the state the callback wrote beside the
    //     marker through the writer's own door.
    drop(client);
    let (launched_tx, launched_rx) = mpsc::channel();
    node.stop();
    node.restart(&launched_tx);
    await_launched(&faults, &launched_rx, 1);

    // The harness builds a **new** business for the restarted container, and with
    // it a new `ApiState`: the handle taken before the restart is not the one
    // that just started.
    let reloaded = node.api_state().expect("the API business is running again");
    wait_until("the business to load its snapshot", &faults, || {
        reloaded
            .started_with_snapshot
            .load(std::sync::atomic::Ordering::Acquire)
    });
    assert_eq!(
        1,
        reloaded
            .loaded_state
            .load(std::sync::atomic::Ordering::Acquire),
        "the record the snapshot callback wrote through the writer is what `on_start` read \
         back: the business's own publication and the snapshot's recording are different \
         streams"
    );
    assert_eq!(
        1,
        reloaded
            .snapshot_records
            .load(std::sync::atomic::Ordering::Acquire),
        "the snapshot image carried exactly the business's own record: the frame the callback \
         published went to the business's publication, not into the recording"
    );
    assert!(reloaded.faults().is_empty(), "{:?}", reloaded.faults());

    node.stop();
}

/// The service messages the business offered in parts and by claim, as the log
/// handed them back to it.
///
/// These never reach the client — a service message is `clusterSessionId =
/// serviceId` and the container reads it out of the log — so the business
/// pairs what it offered with what arrived, and that pairing is the assertion.
#[test]
fn the_log_hands_back_the_parted_and_claimed_service_messages() {
    let _serial = serial();
    let faults = Faults::default();
    let Some(node) = start(&faults) else {
        announce_skip();
        return;
    };
    let state = node.api_state().expect("the API business is running");

    let ports = ports();
    let Some(mut client) = ClientSide::start(&ports, 0) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&faults);
    assert_eq!(EventCode::OK, session.code, "{session:?}");

    send(&client, &session, b"v", &faults);
    send(&client, &session, b"k", &faults);

    // A service message is offered by the business and comes back on the next
    // log pass; two offers means two round trips.
    wait_until("both service messages to come back", &faults, || {
        state
            .service_messages
            .lock()
            .expect("the service-message lock")
            .iter()
            .filter(|(_, delivered)| !delivered.is_empty())
            .count()
            == 2
    });

    let paired = state
        .service_messages
        .lock()
        .expect("the service-message lock")
        .clone();

    assert_eq!(2, paired.len());
    for (offered, delivered) in &paired {
        assert_eq!(
            offered, delivered,
            "the service message the log delivered is the one that was offered"
        );
    }
    assert_eq!(b"vec-onevec-two".to_vec(), paired[0].1);
    assert_eq!(b"claim".to_vec(), paired[1].1);

    assert!(state.faults().is_empty(), "{:?}", state.faults());
}

/// `Cluster.time()`'s session header is what an egress message carries: the
/// claim's header is written by the container, not by the caller.
///
/// A separate case because it is the one thing the two claims could get wrong
/// without any payload changing: `Cluster::try_claim` stamps the **service**
/// id and `ClientSession::try_claim` stamps the **session** id.
#[test]
fn a_claimed_egress_message_carries_the_session_id_not_the_service_id() {
    let _serial = serial();
    let faults = Faults::default();
    let Some(node) = start(&faults) else {
        announce_skip();
        return;
    };

    let ports = ports();
    let Some(mut client) = ClientSide::start(&ports, 0) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&faults);
    assert_eq!(EventCode::OK, session.code, "{session:?}");

    send(&client, &session, b"k", &faults);
    let claimed = wait_for_egress(&mut client, 1, &faults);
    assert_eq!(1, claimed.len());

    let session_id = i64::from_le_bytes(
        claimed[0][16..24]
            .try_into()
            .expect("the header's clusterSessionId"),
    );
    assert_eq!(
        session.cluster_session_id, session_id,
        "an egress message names the session it is for"
    );
    let service_id = node
        .api_state()
        .expect("the API business")
        .service_id
        .load(std::sync::atomic::Ordering::Acquire);
    assert_ne!(
        service_id, session_id,
        "and not the service, which is what a service message names"
    );
}

/// The one-message session header, in bytes: `MessageHeader` and
/// `SessionMessageHeader` (`AeronCluster.SESSION_HEADER_LENGTH`).
const SESSION_HEADER_LENGTH: usize = 32;

/// One session message: the sticky header the client must write, then the
/// command byte the business reads.
fn send(client: &ClientSide, session: &SessionEvent, command: &[u8], faults: &Faults) {
    let message = ingress_message(
        session.leadership_term_id,
        session.cluster_session_id,
        command,
    );
    offer_message(client, faults, &message);
}

fn wait_for_egress(client: &mut ClientSide, count: usize, faults: &Faults) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut payloads = Vec::new();

    while Instant::now() < deadline {
        faults.check();
        payloads.extend(client.poll_egress_messages());
        if payloads.len() >= count {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    payloads
}

/// Poll one subscription for a budget, and answer with what it delivered.
fn poll_payloads_for(
    client: &mut ClientSide,
    registration_id: i64,
    budget: Duration,
) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + budget;
    let mut payloads = Vec::new();

    while Instant::now() < deadline {
        payloads.extend(client.poll_payloads(registration_id));
        if !payloads.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    payloads
}
