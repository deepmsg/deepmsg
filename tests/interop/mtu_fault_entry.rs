//! The entry a receiver records when a sender's MTU does not fit its window.
//!
//! `ChannelValidationTest` asks that the driver record *something* here — it
//! matches two words out of the message — and passes on a bare string. What
//! the reference records is a composition, and the composition is the part
//! that is easy to get almost right: the code is the errno `AERON_SET_ERR` was
//! given (positive, so the description is the OS's text rather than the
//! protocol table's), the file is a **basename**, the line is the `__LINE__`
//! of the macro name rather than of its arguments, and there is a **second**
//! site line for the `AERON_APPEND_ERR` at the call site, carrying the stream
//! and the session.
//!
//! So both drivers are driven through the fault and their entries compared.
//! The session id is masked: it is chosen by whichever driver is publishing,
//! and nothing about it is under test.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT, ReferenceDriver};

const STREAM_ID: i32 = 10001;
const PUB: &str = "aeron:udp?endpoint=localhost:9999|mtu=1408";
const SUB: &str = "aeron:udp?endpoint=localhost:9999|rcv-wnd=1376";

/// Drive one driver through the fault and answer with the entry it recorded.
fn entry_for(aeron_dir: &std::path::Path) -> Option<String> {
    let mut client = Client::connect(aeron_dir).expect("connect to the driver");

    let _publication = client
        .add_publication(PUB, STREAM_ID, Duration::from_secs(10))
        .expect("a publication");
    let _subscription = client
        .add_subscription(SUB, STREAM_ID, Duration::from_secs(10))
        .expect("a subscription");

    let deadline = Instant::now() + Duration::from_secs(20);

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
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.text.contains("mtuLength="))
        {
            return Some(entry.text.clone());
        }
    }

    None
}

/// The session id is the publisher's, so it is the one field two runs cannot
/// agree on.
fn masked(text: &str) -> String {
    let mut masked = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(at) = rest.find("session_id=") {
        masked.push_str(&rest[..at + "session_id=".len()]);
        masked.push_str("<masked>");
        rest = &rest[at + "session_id=".len()..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .unwrap_or(rest.len());
        rest = &rest[end..];
    }

    masked.push_str(rest);
    masked
}

#[test]
fn both_drivers_record_the_same_entry_for_an_mtu_the_window_cannot_carry() {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    // One at a time, and the first is dropped before the second starts: both
    // drivers would bind the same port otherwise, and the second one's
    // subscription would fail to `Address already in use` — which is how this
    // test first arrived.
    let ours_entry = {
        let Some(ours) = OwnDriver::start("mtu-fault-ours") else {
            driver::announce_skip();
            return;
        };

        entry_for(ours.aeron_dir()).expect("our driver records the refusal")
    };

    let theirs_entry = {
        let mut reference = ReferenceDriver::start_with(&binary, "mtu-fault-theirs", &[])
            .expect("start the reference");
        reference
            .await_cnc(READY_TIMEOUT)
            .expect("the reference publishes its CnC file");

        entry_for(reference.aeron_dir()).expect("the reference records the refusal")
    };

    // The reference's own entry, spelled out — read off a live 1.53.2 driver
    // and pinned here so that a change to *either* side shows up as a failure
    // of this test rather than as agreement between two wrong numbers.
    assert_eq!(
        "(22) Invalid argument\n\
         [aeron_receiver_channel_endpoint_validate_sender_mtu_length, \
         aeron_receive_channel_endpoint.c:1022] mtuLength=1408 > initialWindowLength=1376\n\
         [aeron_driver_conductor_execute_create_publication_image_validate, \
         aeron_driver_conductor.c:6509] stream_id=10001 session_id=<masked>\n",
        masked(&theirs_entry)
    );

    assert_eq!(masked(&theirs_entry), masked(&ours_entry));
}
