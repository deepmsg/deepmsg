//! The client side of the cluster interop tests: our session on its **own**
//! driver, and the egress it records.
//!
//! The reference's harness gives its client a driver of its own
//! (`TestCluster.java:960-990` builds a second `MediaDriver.Context` for it),
//! and that is the shape a deployment has: our `AeronCluster` is the client,
//! the nodes are somebody else's, and the medium between them is UDP. Both
//! LC-1 test binaries use this kit so that a scenario and a reliability case
//! differ in what they assert, not in how they connect.
//!
//! # The driver's settings are the test's, not the client's
//!
//! Loss injection lives in the driver (`debug.send.data.loss.…`,
//! `debug.receive.data.loss.…`), and a driver is configured before it starts.
//! [`Session::connect`] therefore takes the `-D` properties to start it with,
//! and the reliability cases are the ones that pass any.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::driver::OwnDriver;
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::admin_response_code::AdminResponseCode;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_core::logbuffer::append::Appended;

/// The fixture's client-facing stream ids (`Options::default`).
pub const INGRESS_STREAM_ID: i32 = 101;
/// The fixture's client-facing egress stream id.
pub const EGRESS_STREAM_ID: i32 = 102;

/// An admin response, as the client saw it.
#[derive(Clone, Debug)]
pub struct AdminResponse {
    /// What was asked.
    pub request_type: AdminRequestType,
    /// What the cluster answered.
    pub response_code: AdminResponseCode,
    /// The cluster's words, empty when the request was accepted.
    pub message: String,
}

/// What the egress delivered, as a test reads it.
#[derive(Default)]
pub struct Received {
    /// Session messages, in arrival order.
    pub messages: Vec<Vec<u8>>,
    /// Session events, in arrival order.
    pub events: Vec<(EventCode, String)>,
    /// Admin responses, in arrival order.
    pub admin_responses: Vec<AdminResponse>,
    /// The leader member id of every `NewLeaderEvent`, in arrival order.
    pub new_leaders: Vec<i32>,
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
        code: EventCode,
        detail: &str,
    ) {
        self.0.borrow_mut().events.push((code, detail.to_owned()));
    }

    fn on_new_leader(
        &mut self,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        leader_member_id: i32,
        _ingress_endpoints: &str,
    ) {
        self.0.borrow_mut().new_leaders.push(leader_member_id);
    }

    fn on_admin_response(
        &mut self,
        _cluster_session_id: i64,
        _correlation_id: i64,
        request_type: AdminRequestType,
        response_code: AdminResponseCode,
        message: &str,
        _payload: &[u8],
    ) {
        self.0.borrow_mut().admin_responses.push(AdminResponse {
            request_type,
            response_code,
            message: message.to_owned(),
        });
    }
}

/// A connected session, its client, and the driver the two of them own.
pub struct Session {
    driver: OwnDriver,
    /// The client the session runs on.
    pub client: Client,
    /// The session.
    pub session: AeronCluster,
    /// Everything the egress delivered.
    pub received: Rc<RefCell<Received>>,
}

impl Session {
    /// Connect on our own driver, started with `driver_properties`.
    ///
    /// `None` when our driver binary is not built, which is the suite's skip.
    /// A connect that fails is a failure: `diagnostics` is called for the
    /// message, because the reason is usually on the cluster's side and only the
    /// caller knows where its logs are.
    ///
    /// # Panics
    ///
    /// When the client cannot connect to our driver, or the session cannot
    /// connect to the cluster.
    pub fn connect(
        label: &str,
        mut context: AeronClusterContext,
        driver_properties: &[&str],
        diagnostics: impl Fn() -> String,
    ) -> Option<Self> {
        let received = Rc::new(RefCell::new(Received::default()));
        context.egress_listener = Some(Box::new(SharedListener(Rc::clone(&received))));

        let driver = OwnDriver::start_with(label, driver_properties)?;
        let mut client = Client::connect(driver.aeron_dir()).unwrap_or_else(|error| {
            panic!("{label}: the client did not connect to our driver: {error}")
        });

        let session = AeronCluster::connect(context, &mut client).unwrap_or_else(|error| {
            panic!(
                "{label}: our client did not connect: {error}\n{}\nour driver:\n{}",
                diagnostics(),
                driver.log_tail(40)
            )
        });

        Some(Self {
            driver,
            client,
            session,
            received,
        })
    }

    /// One poll of the client and the session, which any wait must repeat.
    ///
    /// The client is the session's way to hear from its driver: a loop that
    /// polls one and not the other reads nothing.
    pub fn drive(&mut self) {
        let _ = self.client.poll();
        self.session
            .poll_egress(&mut self.client)
            .unwrap_or_else(|error| panic!("the session failed to poll egress: {error}"));
    }

    /// Offer `payload` until the session takes it, or `false` at the deadline.
    pub fn offer(&mut self, payload: &[u8], deadline: Instant) -> bool {
        while Instant::now() < deadline {
            if matches!(
                self.session.offer(&mut self.client, payload),
                Some(Appended::Ok { .. })
            ) {
                return true;
            }

            self.drive();
            std::thread::sleep(Duration::from_millis(1));
        }

        false
    }

    /// Drive until `condition` holds or the deadline passes.
    pub fn poll_until(
        &mut self,
        deadline: Instant,
        mut condition: impl FnMut(&Self) -> bool,
    ) -> bool {
        while Instant::now() < deadline {
            self.drive();

            if condition(self) {
                return true;
            }

            std::thread::sleep(Duration::from_millis(1));
        }

        self.drive();
        condition(self)
    }

    /// The session messages the listener has, in arrival order.
    #[must_use]
    pub fn messages(&self) -> Vec<Vec<u8>> {
        self.received.borrow().messages.clone()
    }

    /// The last lines our own driver wrote, for a failure's message.
    #[must_use]
    pub fn driver_log(&self) -> String {
        self.driver.log_tail(40)
    }
}

/// The client-side settings the LC-1 cases share: the fixture's stream ids, a
/// message timeout equal to the test's own deadline, and a UDP egress channel
/// whose port the driver picks.
#[must_use]
pub fn base_context(client_name: &str, message_timeout: Duration) -> AeronClusterContext {
    let mut context = AeronClusterContext::default();
    context.message_timeout_ns =
        i64::try_from(message_timeout.as_nanos()).expect("a timeout that fits an i64");
    context.ingress_stream_id = INGRESS_STREAM_ID;
    context.egress_stream_id = EGRESS_STREAM_ID;
    context.client_name = client_name.to_owned();
    context.egress_channel = Some("aeron:udp?term-length=128k|endpoint=localhost:0".to_owned());
    context
}

/// Offer `sequence` as eight little-endian bytes and wait for the echo.
///
/// # Panics
///
/// When the session will not take the message, or the echo never arrives.
pub fn send_and_await_echo(session: &mut Session, sequence: u64, deadline: Instant) {
    let payload = sequence.to_le_bytes();
    let before = session.messages().len();

    assert!(
        session.offer(&payload, deadline),
        "the session never accepted sequence {sequence}"
    );

    assert!(
        session.poll_until(deadline, |session| session.messages().len() > before),
        "the echo of sequence {sequence} never arrived"
    );
}
