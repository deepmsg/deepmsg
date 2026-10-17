//! P3-4c's acceptance: a service on the control channel.
//!
//! There is no container in this build yet (P3-2c), so the test **is** the
//! service: a raw [`Client`] subscribes to the service stream (104), publishes
//! on the consensus-module stream (105), answers the `JoinLog` with a
//! `ServiceAck`, and then exercises the other three kinds it can send.
//!
//! The module runs on its own thread because `launch` blocks in
//! `awaitServicesReady`: the fake service has to poll while the module waits,
//! and one thread cannot do both.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig};
use deepmsg_cluster_common::config::Properties;
use deepmsg_codec_cluster::close_session_codec::CloseSessionEncoder;
use deepmsg_codec_cluster::cluster_members_query_codec::ClusterMembersQueryEncoder;
use deepmsg_codec_cluster::cluster_members_response_codec::ClusterMembersResponseDecoder;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::join_log_codec::JoinLogDecoder;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::schedule_timer_codec::ScheduleTimerEncoder;
use deepmsg_codec_cluster::service_ack_codec::ServiceAckEncoder;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(30);
const SERVICE_STREAM_ID: i32 = 104;
const CONSENSUS_MODULE_STREAM_ID: i32 = 105;
const INGRESS_STREAM_ID: i32 = 101;
const EGRESS_STREAM_ID: i32 = 102;
const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=128k";

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
            format!("-Daeron.cluster.control.channel={CONTROL_CHANNEL}"),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            // One service: the module will wait for its JoinLog ack.
            "-Daeron.cluster.service.count=1".to_owned(),
        ])
        .expect("the properties parse");
    properties
}

fn offer(client: &Client, registration_id: i64, bytes: &[u8]) -> bool {
    matches!(
        client.offer(registration_id, bytes),
        Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
    )
}

fn wait_for<F: FnMut() -> bool>(what: &str, mut condition: F) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn a_service_joins_and_then_timers_sessions_and_membership_all_work() {
    let archive_dir = TempDir::new("cluster-service-archive");
    let cluster_dir = TempDir::new("cluster-service-cluster");
    let Some(mut driver) =
        OwnArchivingMediaDriver::start("cluster-service", archive_dir.path(), &archive::PROPERTIES)
    else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration parses");

    // The service first, so its subscription exists when the module sends.
    let mut service = Client::connect(driver.aeron_dir()).expect("the service client connects");
    let service_subscription = service
        .add_subscription(CONTROL_CHANNEL, SERVICE_STREAM_ID, DEADLINE)
        .expect("the service subscription");
    let service_publication = service
        .add_publication(CONTROL_CHANNEL, CONSENSUS_MODULE_STREAM_ID, DEADLINE)
        .expect("the service publication");

    let log_position = Arc::new(AtomicI64::new(0));
    let session_count = Arc::new(AtomicUsize::new(0));
    let launched = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let failure: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));

    let (thread_log_position, thread_session_count, thread_launched, thread_stop) = (
        Arc::clone(&log_position),
        Arc::clone(&session_count),
        Arc::clone(&launched),
        Arc::clone(&stop),
    );
    let thread_failure = Arc::clone(&failure);

    let module = std::thread::spawn(move || {
        let agent = match ConsensusModuleAgent::launch(config) {
            Ok(agent) => agent,
            Err(error) => {
                *thread_failure.lock().expect("the lock") = Some(error.to_string());
                return;
            }
        };
        thread_launched.store(true, Ordering::SeqCst);

        let mut agent = agent;
        while !thread_stop.load(Ordering::SeqCst) {
            if let Err(error) = agent.do_work() {
                *thread_failure.lock().expect("the lock") = Some(error.to_string());
                break;
            }
            thread_log_position.store(agent.log_position(), Ordering::SeqCst);
            #[allow(clippy::cast_possible_truncation)] // a test's session count
            thread_session_count.store(agent.session_count(), Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1));
        }
        let _ = agent.close();
    });

    // What a real container does at startup (`ClusteredServiceAgent
    // .recoverState`): ack the recovery-state counter's position with a
    // non-null relevant id, before it ever sees a JoinLog. The module's
    // `recoverFromSnapshotAndLog` is parked on exactly that.
    let mut recovery_acked = false;
    let mut joined = false;
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline && !joined {
        service.poll();

        if !recovery_acked {
            let mut buffer = vec![0_u8; 64];
            let encoder = ServiceAckEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut ack = header.parent().expect("the encoder just wrapped");
            ack.log_position(0)
                .timestamp(0)
                .ack_id(0)
                .relevant_id(42)
                .service_id(0);
            let length = MESSAGE_HEADER_LENGTH + ack.encoded_length();
            recovery_acked = offer(&service, service_publication, &buffer[..length]);
            if recovery_acked {
                continue;
            }
        }

        let mut join_position = None;
        service.poll_subscription(service_subscription, 10, |message| {
            let payload = message.payload;
            if payload.len() < MESSAGE_HEADER_LENGTH {
                return;
            }
            let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
            if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
                || header.template_id() != deepmsg_codec_cluster::join_log_codec::SBE_TEMPLATE_ID
            {
                return;
            }
            let decoder = JoinLogDecoder::default().header(header, 0);
            join_position = Some(decoder.log_position());
        });
        if let Some(log_position) = join_position {
            let mut buffer = vec![0_u8; 64];
            let encoder = ServiceAckEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut ack = header.parent().expect("the encoder just wrapped");
            // `ackId` is 1: the recovery ack above was id 0
            // (`recoverFromSnapshotAndLog` increments it).
            ack.log_position(log_position)
                .timestamp(0)
                .ack_id(1)
                .relevant_id(42)
                .service_id(0);
            let length = MESSAGE_HEADER_LENGTH + ack.encoded_length();
            wait_for("the service publication to accept the ack", || {
                offer(&service, service_publication, &buffer[..length])
            });
            joined = true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        joined,
        "the module never sent JoinLog; failure: {:?}",
        failure.lock().expect("the lock")
    );
    wait_for("the module to finish launching", || {
        launched.load(Ordering::SeqCst)
    });
    if let Some(error) = failure.lock().expect("the lock").clone() {
        panic!("the module failed: {error}");
    }

    // A client session, so CloseSession has something to close.
    let mut client = Client::connect(driver.aeron_dir()).expect("the cluster client connects");
    let ingress_publication = client
        .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
        .expect("ingress publication");
    let egress_subscription = client
        .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
        .expect("egress subscription");

    let mut connect = vec![0_u8; 256];
    {
        let encoder = SessionConnectRequestEncoder::default()
            .wrap(WriteBuf::new(&mut connect), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut message = header.parent().expect("the encoder just wrapped");
        message
            .correlation_id(1)
            .response_stream_id(EGRESS_STREAM_ID)
            .version(3 << 8)
            .response_channel(b"aeron:ipc")
            .encoded_credentials(b"")
            .client_info(b"service-test");
        let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
        connect.truncate(length);
    }

    let mut open = false;
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline && !open {
        client.poll();
        if !offer(&client, ingress_publication, &connect) {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }
        client.poll_subscription(egress_subscription, 10, |message| {
            let payload = message.payload;
            if payload.len() < MESSAGE_HEADER_LENGTH {
                return;
            }
            let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
            if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
                || header.template_id()
                    != deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID
            {
                return;
            }
            let decoder = SessionEventDecoder::default().header(header, 0);
            if decoder.code() == EventCode::OK {
                open = true;
            }
        });
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(open, "the session did not open");
    wait_for("the module to see the open session", || {
        session_count.load(Ordering::SeqCst) == 1
    });

    // CloseSession.
    let mut close = vec![0_u8; 64];
    {
        let encoder =
            CloseSessionEncoder::default().wrap(WriteBuf::new(&mut close), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut message = header.parent().expect("the encoder just wrapped");
        message.cluster_session_id(1);
        let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
        close.truncate(length);
    }
    wait_for("the service publication to accept CloseSession", || {
        service.poll();
        offer(&service, service_publication, &close)
    });
    wait_for("the session to close", || {
        session_count.load(Ordering::SeqCst) == 0
    });

    // ScheduleTimer for a deadline in the past: the next leader poll expires
    // it and appends a TimerEvent.
    let before = log_position.load(Ordering::SeqCst);
    let mut schedule = vec![0_u8; 64];
    {
        let encoder = ScheduleTimerEncoder::default()
            .wrap(WriteBuf::new(&mut schedule), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut message = header.parent().expect("the encoder just wrapped");
        #[allow(clippy::cast_possible_wrap)] // epoch milliseconds fits an i64
        let deadline_ms = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock")
            .as_millis() as i64)
            - 1;
        message.correlation_id(5).deadline(deadline_ms);
        let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
        schedule.truncate(length);
    }
    wait_for("the service publication to accept ScheduleTimer", || {
        service.poll();
        offer(&service, service_publication, &schedule)
    });
    wait_for("the timer event to be appended", || {
        log_position.load(Ordering::SeqCst) > before
    });

    // ClusterMembersQuery and its response.
    let mut query = vec![0_u8; 64];
    {
        let encoder = ClusterMembersQueryEncoder::default()
            .wrap(WriteBuf::new(&mut query), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut message = header.parent().expect("the encoder just wrapped");
        message
            .correlation_id(9)
            .extended(deepmsg_codec_cluster::boolean_type::BooleanType::FALSE);
        let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
        query.truncate(length);
    }
    wait_for(
        "the service publication to accept ClusterMembersQuery",
        || {
            service.poll();
            offer(&service, service_publication, &query)
        },
    );

    let mut answered = false;
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline && !answered {
        service.poll();
        service.poll_subscription(service_subscription, 10, |message| {
            let payload = message.payload;
            if payload.len() < MESSAGE_HEADER_LENGTH {
                return;
            }
            let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
            if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
                || header.template_id()
                    != deepmsg_codec_cluster::cluster_members_response_codec::SBE_TEMPLATE_ID
            {
                return;
            }
            let decoder = ClusterMembersResponseDecoder::default().header(header, 0);
            if decoder.correlation_id() == 9 {
                answered = true;
            }
        });
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(answered, "the membership query went unanswered");

    stop.store(true, Ordering::SeqCst);
    module.join().expect("the module thread joins");
    if let Some(error) = failure.lock().expect("the lock").clone() {
        panic!("the module failed: {error}");
    }
    client.close();
    service.close();
    driver.stop().expect("the driver stops");
}
