//! P3-4b's acceptance: a client the cluster has never met connects, is
//! authenticated, opens a session, has a message written into the log, and
//! closes — all over the real ingress and egress channels.
//!
//! The client here is a raw [`Client`], not the cluster client (that is P3-3):
//! it publishes the two SBE messages the reference's `AeronCluster` would and
//! reads the `SessionEvent` the cluster answers with. That is the same
//! exchange, with the handshake written out in the test instead of in a
//! library.
//!
//! The driver and archive are the workspace's own binaries, as in
//! `cluster_cm_launch.rs`; the consensus module runs in this process so the
//! test can drive its duty cycle.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::session::authenticator::{Authenticator, SessionProxy};
use deepmsg_cluster::{
    ClusterError, ConsensusModuleAgent, ConsensusModuleConfig, LaunchComponents,
};
use deepmsg_cluster_common::config::Properties;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::session_close_request_codec::SessionCloseRequestEncoder;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(30);
const INGRESS_STREAM_ID: i32 = 101;
const EGRESS_STREAM_ID: i32 = 102;
const PROTOCOL_VERSION: i32 = 3 << 8; // 0.3.0, `SemanticVersion.compose(0, 3, 0)`

fn properties(aeron_dir: &std::path::Path, cluster_dir: &std::path::Path) -> Properties {
    let mut properties = Properties::new();
    properties
        .apply_args([
            format!("-Daeron.dir={}", aeron_dir.display()),
            format!("-Daeron.cluster.dir={}", cluster_dir.display()),
            "-Daeron.cluster.id=7".to_owned(),
            "-Daeron.cluster.member.id=0".to_owned(),
            "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0"
                .to_owned(),
            "-Daeron.cluster.ingress.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.consensus.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.log.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k".to_owned(),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            "-Daeron.cluster.service.count=0".to_owned(),
        ])
        .expect("the properties parse");
    properties
}

struct Harness {
    // Held so the directories outlive the driver and the module.
    _archive_dir: TempDir,
    _cluster_dir: TempDir,
    driver: OwnArchivingMediaDriver,
    agent: ConsensusModuleAgent,
    client: Client,
    ingress_publication_id: i64,
    egress_subscription_id: i64,
}

impl Harness {
    fn start(name: &str, components: LaunchComponents) -> Option<Self> {
        let archive_dir = TempDir::new(&format!("{name}-archive"));
        let cluster_dir = TempDir::new(&format!("{name}-cluster"));
        let mut driver =
            OwnArchivingMediaDriver::start(name, archive_dir.path(), &archive::PROPERTIES)?;
        driver.await_ready(DEADLINE).expect("the driver is ready");

        let mut config = ConsensusModuleConfig::from_properties(&properties(
            driver.aeron_dir(),
            cluster_dir.path(),
        ))
        .expect("the configuration parses");
        // This build has no class loader; the flag is the programmatic
        // extension seam a zero-service cluster needs (`ConsensusModule.java:2158`).
        config.consensus_module_extension = true;
        let agent = ConsensusModuleAgent::launch_with_components(config, components)
            .expect("the module launches");

        let mut client = Client::connect(driver.aeron_dir()).expect("the test client connects");
        let ingress_publication_id = client
            .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
            .expect("ingress publication");
        let egress_subscription_id = client
            .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
            .expect("egress subscription");

        Some(Self {
            _archive_dir: archive_dir,
            _cluster_dir: cluster_dir,
            driver,
            agent,
            client,
            ingress_publication_id,
            egress_subscription_id,
        })
    }

    fn offer(&self, bytes: &[u8]) -> bool {
        matches!(
            self.client.offer(self.ingress_publication_id, bytes),
            Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
        )
    }

    /// One duty cycle and one client poll — the two things a node and a client
    /// do — and whatever the egress subscription delivered.
    fn turn(&mut self) -> Vec<(i64, i64, EventCode)> {
        self.agent.do_work().expect("the duty cycle is healthy");
        self.client.poll();

        let mut events = Vec::new();
        self.client
            .poll_subscription(self.egress_subscription_id, 10, |message| {
                let payload = message.payload;
                if payload.len() < MESSAGE_HEADER_LENGTH {
                    return;
                }
                let header =
                    deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder::default()
                        .wrap(ReadBuf::new(payload), 0);
                if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
                    || header.template_id()
                        != deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID
                {
                    return;
                }
                let decoder = SessionEventDecoder::default().header(header, 0);
                events.push((
                    decoder.cluster_session_id(),
                    decoder.correlation_id(),
                    decoder.code(),
                ));
            });
        events
    }

    fn until<F>(&mut self, what: &str, mut condition: F) -> Vec<(i64, i64, EventCode)>
    where
        F: FnMut(&Harness, &[(i64, i64, EventCode)]) -> bool,
    {
        let deadline = Instant::now() + DEADLINE;
        let mut events = Vec::new();
        while Instant::now() < deadline {
            events.extend(self.turn());
            if condition(self, &events) {
                return events;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out waiting for {what}; events so far: {events:?}");
    }
}

fn connect_request(correlation_id: i64) -> Vec<u8> {
    let mut buffer = vec![0_u8; 256];
    let encoder = SessionConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .response_stream_id(EGRESS_STREAM_ID)
        .version(PROTOCOL_VERSION)
        .response_channel(b"aeron:ipc")
        .encoded_credentials(b"")
        .client_info(b"integration");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

fn ingress_message(leadership_term_id: i64, cluster_session_id: i64, payload: &[u8]) -> Vec<u8> {
    let mut buffer = vec![0_u8; 64 + payload.len()];
    let encoder = SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(1);
    let header_length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer[header_length..header_length + payload.len()].copy_from_slice(payload);
    buffer.truncate(header_length + payload.len());
    buffer
}

fn close_request(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
    let mut buffer = vec![0_u8; 64];
    let encoder = SessionCloseRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id);
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

#[test]
fn a_client_connects_opens_a_session_sends_a_message_and_closes() {
    let Some(mut harness) = Harness::start("cluster-session", LaunchComponents::default()) else {
        archiving_driver::announce_skip();
        return;
    };

    // The ingress publication has to be connected before anything is offered.
    let deadline = Instant::now() + DEADLINE;
    while !harness.offer(&connect_request(1)) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication never accepted a connect request"
        );
        harness.agent.do_work().expect("the duty cycle is healthy");
        harness.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    // The open event is `SessionEvent(OK)` for session 1.
    let events = harness.until("the session open event", |_, events| {
        events
            .iter()
            .any(|(session_id, _, code)| *session_id == 1 && *code == EventCode::OK)
    });
    assert!(
        events
            .iter()
            .any(|(session_id, correlation_id, code)| *session_id == 1
                && *correlation_id == 1
                && *code == EventCode::OK),
        "{events:?}"
    );
    assert_eq!(1, harness.agent.session_count());

    // A message on the open session goes into the log.
    let leadership_term_id = harness.agent.leadership_term_id();
    let before = harness.agent.log_position();
    assert!(harness.offer(&ingress_message(leadership_term_id, 1, b"hello")));
    harness.until("the message to reach the log", |agent, _| {
        agent.agent.log_position() > before + 32
    });
    let frame_length = harness.agent.log_position() - before;
    assert_eq!(
        96, frame_length,
        "32-byte session header + 5-byte payload + data header, aligned to 32"
    );

    // A close request takes the session out and appends the close event.
    let before_close = harness.agent.log_position();
    assert!(harness.offer(&close_request(leadership_term_id, 1)));
    harness.until("the session close", |harness, _| {
        harness.agent.session_count() == 0
    });
    assert!(harness.agent.log_position() > before_close);

    harness.agent.close().expect("the module closes");
    harness.driver.stop().expect("the driver stops");
}

/// A truncated cluster message is an error the duty cycle must report, not
/// swallow: the reference's decoder throws out of `onFragment`, which aborts
/// the poll and reaches `onDoWorkError` (plan §4.4 rule 5).
#[test]
fn a_truncated_ingress_message_is_an_error_the_duty_cycle_reports() {
    let Some(mut harness) =
        Harness::start("cluster-session-truncated", LaunchComponents::default())
    else {
        archiving_driver::announce_skip();
        return;
    };

    // A header that claims a `SessionMessageHeader` with no body behind it.
    let mut truncated = [0_u8; MESSAGE_HEADER_LENGTH];
    truncated[0..2].copy_from_slice(&24_u16.to_le_bytes());
    truncated[2..4].copy_from_slice(&1_u16.to_le_bytes());
    truncated[4..6].copy_from_slice(&deepmsg_codec_cluster::SBE_SCHEMA_ID.to_le_bytes());
    truncated[6..8].copy_from_slice(&17_u16.to_le_bytes());

    let deadline = Instant::now() + DEADLINE;
    while !harness.offer(&truncated) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication never accepted the frame"
        );
        harness.agent.do_work().expect("the duty cycle is healthy");
        harness.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let mut error = None;
    while Instant::now() < deadline && error.is_none() {
        match harness.agent.do_work() {
            Ok(_) => {
                harness.client.poll();
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(reported) => error = Some(reported),
        }
    }
    let error = error.expect("the truncated message is reported");
    assert!(
        error.to_string().contains("SessionMessageHeader"),
        "{error}"
    );

    harness.agent.close().expect("the module closes");
    harness.driver.stop().expect("the driver stops");
}

/// `AuthenticationTest.shouldRejectOnConnectRequest`'s shape, end to end: an
/// authenticator that rejects produces `SessionEvent(AUTHENTICATION_REJECTED)`.
struct RejectingAuthenticator;

impl Authenticator for RejectingAuthenticator {
    fn on_connect_request(&mut self, _session_id: i64, _encoded_credentials: &[u8], _now_ms: i64) {}

    fn on_challenge_response(
        &mut self,
        _session_id: i64,
        _encoded_credentials: &[u8],
        _now_ms: i64,
    ) {
    }

    fn on_connected_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), ClusterError> {
        session.reject();
        Ok(())
    }

    fn on_challenged_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), ClusterError> {
        session.reject();
        Ok(())
    }
}

#[test]
fn a_rejecting_authenticator_produces_an_authentication_rejected_event() {
    let Some(mut harness) = Harness::start(
        "cluster-session-reject",
        LaunchComponents {
            authenticator: Some(Box::new(RejectingAuthenticator)),
            authorisation_service: None,
        },
    ) else {
        archiving_driver::announce_skip();
        return;
    };

    let deadline = Instant::now() + DEADLINE;
    while !harness.offer(&connect_request(1)) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication never accepted a connect request"
        );
        harness.agent.do_work().expect("the duty cycle is healthy");
        harness.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let events = harness.until("the rejection event", |_, events| {
        events
            .iter()
            .any(|(_, _, code)| *code == EventCode::AUTHENTICATION_REJECTED)
    });
    assert!(
        events
            .iter()
            .any(|(_, correlation_id, code)| *correlation_id == 1
                && *code == EventCode::AUTHENTICATION_REJECTED),
        "{events:?}"
    );
    assert_eq!(0, harness.agent.session_count());

    harness.agent.close().expect("the module closes");
    harness.driver.stop().expect("the driver stops");
}
