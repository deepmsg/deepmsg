//! What both drivers answer and record when a subscription cannot bind.
//!
//! The largest entry a client ever sees: five layers each append a site line to
//! the reference's per-thread error buffer — the syscall that failed, the
//! transport, the destination, and two frames of the conductor — and the whole
//! thing is both the `ON_ERROR`'s message and the log's entry. One
//! composition, two readers, which is why a `RegistrationException` and an
//! error-log line look the same.
//!
//! This build recorded `Address already in use (os error 98)` — a bare
//! message, no code, no sites — until the layers were taught to append. The
//! descriptor number in the first line is masked: it is the process's, and the
//! two drivers make their sockets in different orders.

use std::net::UdpSocket;
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT, ReferenceDriver};

// A port of its own: cargo runs test *binaries* in parallel, and
// `mtu_fault_entry` is holding 9999 for the same reason. Two tests that both
// need a specific port taken are two tests that must not share it.
const CHANNEL: &str = "aeron:udp?endpoint=127.0.0.1:10099";
const STREAM_ID: i32 = 1001;

/// What the reference answers with and records, with the one field that is the
/// process's own masked out.
const EXPECTED: &str = "(98) Address already in use\n\
     [aeron_bind, aeron_socket.c:93] failed to bind(<fd>, 127.0.0.1:10099)\n\
     [aeron_udp_channel_transport_init, aeron_udp_channel_transport.c:151] unicast bind, affinity=1\n\
     [aeron_receive_destination_create, aeron_receive_destination.c:78] uri = aeron:udp?endpoint=127.0.0.1:10099\n\
     [aeron_driver_conductor_get_or_add_receive_channel_endpoint, aeron_driver_conductor.c:2110] correlation_id=2\n\
     [aeron_driver_conductor_execute_add_network_subscription, aeron_driver_conductor.c:5043] \n";

fn masked(text: &str) -> String {
    let Some(at) = text.find("failed to bind(") else {
        return text.to_string();
    };
    let head = &text[..at + "failed to bind(".len()];
    let rest = &text[at + "failed to bind(".len()..];
    let end = rest.find(',').expect("a descriptor and then an address");

    format!("{head}<fd>{}", &rest[end..])
}

/// Ask one driver for a subscription it cannot bind, and answer with the
/// message it was told and the entry it recorded.
fn ask(aeron_dir: &std::path::Path) -> (String, String) {
    let mut client = Client::connect(aeron_dir).expect("connect to the driver");

    let refused = client
        .add_subscription(CHANNEL, STREAM_ID, Duration::from_secs(10))
        .expect_err("the port is taken");
    let told = format!("{refused:?}");
    let told = told[told.find("message: \"").expect("a message") + 10..].to_string();
    let told = told[..told.rfind('"').expect("a closing quote")].replace("\\n", "\n");

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        client.poll();
        std::thread::sleep(Duration::from_millis(5));

        let Ok(cnc) = deepmsg_cnc::CncFile::open(aeron_dir, Duration::from_secs(1)) else {
            continue;
        };
        let Some(log) = cnc.error_log() else {
            continue;
        };

        let mut entries = Vec::new();
        log.read(i64::MIN, &mut entries);
        if let Some(entry) = entries.first() {
            return (told, entry.text.clone());
        }
    }

    panic!("nothing was recorded");
}

#[test]
fn both_drivers_answer_and_record_the_same_chain_for_a_bind_that_failed() {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    // The port has to be taken by somebody, or both drivers bind happily.
    let held = UdpSocket::bind("127.0.0.1:10099").expect("hold the port");

    // One at a time: two drivers on this port is the fault being tested.
    let (ours_told, ours_recorded) = {
        let Some(ours) = OwnDriver::start("bind-fault-ours") else {
            driver::announce_skip();
            return;
        };

        ask(ours.aeron_dir())
    };

    let (theirs_told, theirs_recorded) = {
        let mut reference = ReferenceDriver::start_with(&binary, "bind-fault-theirs", &[])
            .expect("start the reference");
        reference
            .await_cnc(READY_TIMEOUT)
            .expect("the reference publishes its CnC file");

        ask(reference.aeron_dir())
    };

    drop(held);

    // The reference's own words, pinned here as well as compared, so that a
    // change to *either* side is a failure of this test rather than agreement
    // between two wrong numbers.
    assert_eq!(EXPECTED, masked(&theirs_told));
    assert_eq!(EXPECTED, masked(&theirs_recorded));

    // And this build says and logs the same thing — which is the point, since
    // a client reads one and an operator reads the other.
    assert_eq!(masked(&theirs_told), masked(&ours_told));
    assert_eq!(masked(&theirs_recorded), masked(&ours_recorded));
    assert_eq!(
        ours_told, ours_recorded,
        "one composition, two readers — the message and the entry are the same text"
    );
}
