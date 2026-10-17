//! The session table's decisions that do not need a driver: the four branches
//! of a connect request, the authenticator's connect callback, the challenge
//! routing, and the state changes an authenticator can ask for.
//!
//! `AuthenticationTest` (`AuthenticationTest.java:86-416`) asserts the same
//! five cases end to end through a real client. What is asserted here is the
//! decision half; the client-visible half is the integration test's
//! (`tests/integration/cluster_session.rs`). The `ours` column of
//! `reference-cases-consensus.tsv` records which assertions were weakened.

use std::cell::RefCell;
use std::rc::Rc;

use deepmsg_cluster::config::{
    SESSION_INVALID_VERSION_MSG, SESSION_LIMIT_MSG, SESSION_REJECTED_MSG,
};
use deepmsg_cluster::session::authenticator::{ALLOW_ALL_NAME, Authenticator, SessionProxy};
use deepmsg_cluster::session::cluster_session::{ClusterSession, ClusterSessionState, NULL_VALUE};
use deepmsg_cluster::session::manager::{SessionConnectRequest, SessionManager};
use deepmsg_cluster::state::Role;
use deepmsg_codec_cluster::event_code::EventCode;

#[derive(Default)]
struct Recorded {
    connect_requests: Vec<(i64, Vec<u8>)>,
    challenge_responses: Vec<(i64, Vec<u8>)>,
}

struct RecordingAuthenticator {
    recorded: Rc<RefCell<Recorded>>,
}

impl RecordingAuthenticator {
    fn new() -> (Self, Rc<RefCell<Recorded>>) {
        let recorded = Rc::new(RefCell::new(Recorded::default()));
        (
            Self {
                recorded: Rc::clone(&recorded),
            },
            recorded,
        )
    }
}

impl Authenticator for RecordingAuthenticator {
    fn on_connect_request(&mut self, session_id: i64, encoded_credentials: &[u8], _now_ms: i64) {
        self.recorded
            .borrow_mut()
            .connect_requests
            .push((session_id, encoded_credentials.to_vec()));
    }

    fn on_challenge_response(&mut self, session_id: i64, encoded_credentials: &[u8], _now_ms: i64) {
        self.recorded
            .borrow_mut()
            .challenge_responses
            .push((session_id, encoded_credentials.to_vec()));
    }

    fn on_connected_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        session.authenticate(&[])?;
        Ok(())
    }

    fn on_challenged_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        session.authenticate(b"principal")?;
        Ok(())
    }
}

fn manager_with(authenticator: RecordingAuthenticator) -> (SessionManager, Rc<RefCell<Recorded>>) {
    let recorded = Rc::clone(&authenticator.recorded);
    let manager = SessionManager::new(
        0,
        7,
        10_000_000_000,
        10,
        Box::new(authenticator),
        deepmsg_cluster::session::authenticator::authorisation_service_for(Some(ALLOW_ALL_NAME))
            .expect("ALLOW_ALL exists"),
    );
    (manager, recorded)
}

fn request(correlation_id: i64, version: Option<i32>) -> SessionConnectRequest {
    SessionConnectRequest {
        correlation_id,
        response_stream_id: 102,
        version,
        response_channel: "aeron:ipc".to_owned(),
        encoded_credentials: Vec::new(),
        client_info: "test".to_owned(),
        source_identity: "(ipc)".to_owned(),
        image_session_id: 1,
    }
}

#[test]
fn should_redirect_a_connect_request_on_a_follower() {
    let (mut manager, _) = manager_with(RecordingAuthenticator::new().0);
    manager.on_session_connect(
        request(1, Some(768)),
        Role::Follower,
        "0=localhost:20123",
        0,
    );

    assert_eq!(1, manager.redirect_sessions().len());
    assert_eq!(NULL_VALUE, manager.redirect_sessions()[0].id());
    assert_eq!(
        Some(EventCode::REDIRECT),
        manager.redirect_sessions()[0].event_code()
    );
    assert_eq!(
        Some("0=localhost:20123"),
        manager.redirect_sessions()[0].response_detail()
    );
    assert!(manager.pending_sessions().is_empty());
}

#[test]
fn should_reject_a_client_with_the_wrong_protocol_major_version() {
    let (mut manager, _) = manager_with(RecordingAuthenticator::new().0);
    // 1.0.0: major 1, where the cluster is 0.3.0.
    manager.on_session_connect(
        request(1, Some(1 << 16)),
        Role::Leader,
        "0=localhost:20123",
        0,
    );

    let rejected = &manager.rejected_sessions()[0];
    assert_eq!(ClusterSessionState::Rejected, rejected.state());
    assert_eq!(Some(EventCode::ERROR), rejected.event_code());
    let detail = rejected.response_detail().expect("a detail");
    assert!(detail.starts_with(SESSION_INVALID_VERSION_MSG), "{detail}");
    assert!(detail.contains("1.0.0"), "{detail}");
    assert!(detail.contains("0.3.0"), "{detail}");
}

#[test]
fn should_reject_a_connect_request_over_the_concurrent_session_limit() {
    let authenticator = RecordingAuthenticator::new().0;
    let mut manager = SessionManager::new(
        0,
        7,
        10_000_000_000,
        0,
        Box::new(authenticator),
        deepmsg_cluster::session::authenticator::authorisation_service_for(None).expect("default"),
    );
    manager.on_session_connect(request(1, Some(768)), Role::Leader, "", 0);

    let rejected = &manager.rejected_sessions()[0];
    assert_eq!(Some(EventCode::ERROR), rejected.event_code());
    assert_eq!(Some(SESSION_LIMIT_MSG), rejected.response_detail());
}

#[test]
fn should_hand_an_accepted_connect_request_to_the_authenticator() {
    let (authenticator, recorded) = RecordingAuthenticator::new();
    let (mut manager, _) = manager_with(authenticator);

    let mut request = request(23, Some(768));
    request.encoded_credentials = b"secret".to_vec();
    manager.on_session_connect(request, Role::Leader, "", 1_000_000);

    assert_eq!(1, manager.pending_sessions().len());
    let session = &manager.pending_sessions()[0];
    assert_eq!(1, session.id(), "session ids start at one");
    assert_eq!(ClusterSessionState::Init, session.state());
    assert_eq!(
        vec![(1, b"secret".to_vec())],
        recorded.borrow().connect_requests
    );
}

#[test]
fn should_route_a_challenge_response_to_the_authenticator_only_when_challenged() {
    let (authenticator, recorded) = RecordingAuthenticator::new();
    let (mut manager, _) = manager_with(authenticator);
    manager.on_session_connect(request(1, Some(768)), Role::Leader, "", 0);

    // An open/pending session that is not challenged does not hear it.
    manager.on_challenge_response_for_user_session(7, 1, b"early", 0);
    assert!(recorded.borrow().challenge_responses.is_empty());

    manager.pending_sessions_mut()[0].set_state(ClusterSessionState::Challenged, "test");
    manager.on_challenge_response_for_user_session(7, 1, b"answer", 0);
    assert_eq!(
        vec![(1, b"answer".to_vec())],
        recorded.borrow().challenge_responses
    );
}

#[test]
fn should_authenticate_and_reject_a_session_the_authenticator_asks_for() {
    let mut session = ClusterSession::new(0, 1, 102, "aeron:ipc", String::new());

    session
        .authenticate(b"principal")
        .expect("the principal fits");
    assert_eq!(ClusterSessionState::Authenticated, session.state());
    assert_eq!(b"principal", session.encoded_principal());

    let mut rejected = ClusterSession::new(0, 1, 102, "aeron:ipc", String::new());
    rejected.reject(EventCode::AUTHENTICATION_REJECTED, SESSION_REJECTED_MSG);
    assert_eq!(ClusterSessionState::Rejected, rejected.state());
    assert_eq!(
        Some(EventCode::AUTHENTICATION_REJECTED),
        rejected.event_code()
    );
    assert_eq!(Some(SESSION_REJECTED_MSG), rejected.response_detail());
}

#[test]
fn a_challenge_that_could_not_be_sent_leaves_the_session_where_it_was() {
    // The send needs a publication, and a session without one answers `false`;
    // the state moves only when the challenge went out
    // (`ClusterSessionProxy.java:47-56`).
    let session = ClusterSession::new(0, 1, 102, "aeron:ipc", String::new());
    assert_eq!(ClusterSessionState::Init, session.state());
    assert_eq!(None, session.event_code());
}
