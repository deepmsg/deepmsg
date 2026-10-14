//! **The channel a live replication names, as both drivers read it.**
//!
//! `ReplicationSession.extend` puts `tags=<channelTagId>,<subscriptionTagId>` on
//! every multi-destination channel (`ReplicationSession.java:536-541`), so a
//! replication that names a live destination and no tags — the live merge, and
//! the only way to reach that branch without a tag — asks for a subscription on
//! `tags=-1,-1`. That is `Aeron.NULL_VALUE` written as a tag, and the three
//! drivers do three different things with it:
//!
//! * the reference **C** driver refuses the channel, because its parse-failure
//!   sentinel and its "no tag" sentinel are the same `-1`
//!   (`aeron_uri.h:89`, `aeron_uri.c:517-529`, `aeron_udp_channel.c:385-390`);
//! * the reference **Java** driver serves it as a **real** tag of `-1`
//!   (`UdpChannel.java:216-218`);
//! * **this** driver serves it as **no tag**, which is what the value means
//!   (`crates/driver/src/udp_channel.rs::parse_tag`).
//!
//! The third is a deliberate divergence and `docs/compat.md` records it — and a
//! divergence is only a contract if a test covers *both* sides of it. That is
//! what this is: the same channel, the same client, our driver and the
//! reference's. Neither the driver's own unit tests nor any integration test can
//! say what the reference does with it; only the reference can.
//!
//! Run with `cargo test -p deepmsg-tests --features interop`. With no reference
//! checkout the reference half prints `SKIPPED` and passes, like the rest of
//! this directory.

use std::time::Duration;

use deepmsg_client::client::{Client, CommandError};
use deepmsg_tests::driver::{self, OwnDriver, ReferenceDriver};

/// The stream the subscription asks for — nothing is published, so the number
/// is only what the request carries.
const STREAM_ID: i32 = 1001;

/// The channel `EXTEND` builds for a live-only replication, byte for byte, with
/// the two null ids the reference writes and the two parameters this build
/// adds on top of them.
const NULL_TAG_CHANNEL: &str = "aeron:udp?tags=-1,-1|rejoin=false|session-id=7|control-mode=manual";

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Our driver **serves** the channel, and the reference's **refuses** it.
///
/// One test and not two, because the claim is comparative: a row that says
/// "this driver differs from the reference here" is meaningless unless both
/// verdicts are read in the same place.
#[test]
fn the_null_tag_channel_is_one_the_reference_refuses_and_this_driver_serves() {
    let Some(mut own) = OwnDriver::start("interop-null-tag-own") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("our driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    assert!(
        client
            .add_subscription(NULL_TAG_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
            .is_ok(),
        "this driver serves the channel the reference's archive builds for a live merge"
    );

    own.stop().expect("our driver stops");

    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference =
        ReferenceDriver::start(&binary, "interop-null-tag-reference").expect("start the reference");

    let mut client = Client::connect(reference.aeron_dir()).expect("connect to the reference");

    let refusal = client
        .add_subscription(NULL_TAG_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect_err("the reference C driver refuses it");

    match refusal {
        CommandError::Driver { message, .. } => assert!(
            message.contains("could not parse channel tag string: -1"),
            "the reference's own words for the refusal: {message}"
        ),
        other => panic!("the reference answered something else: {other:?}"),
    }

    reference.stop().expect("the reference driver stops");
}
