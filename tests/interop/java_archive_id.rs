//! P2-C1's last acceptance case: **an archive that names itself**
//! (`AeronCArchiveIdTest.shouldResolveArchiveId`, `aeron_archive_test.cpp:3394-3406`).
//!
//! The one case of the seventeen that needs a **real archive**, and it needs it
//! for a reason that is easy to miss: a client that has not asked reports
//! `AERON_NULL_VALUE`, and the reference archive's own default id is `-1` — the
//! same number — so "the client asked and was answered" and "nobody asked" are
//! indistinguishable until the archive is made to answer something else. Hence
//! `-Daeron.archive.id=0x0423_6483_BEEF`, which is also **wider than 32 bits**, so
//! a client that narrowed the id anywhere on the way through would fail here.
//!
//! Run with `cargo test -p deepmsg-tests --features interop`. When the reference
//! build is not there the test prints `SKIPPED` and passes, like the rest of this
//! directory.

use std::time::Duration;

use deepmsg_archive::client::archive::{Archive, Handlers};
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::{ArchiveContext, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_tests::java_archiving_driver::{self, JavaArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// `0x0423_6483_BEEF` — the id the reference's own case sets, and wider than an
/// `int32` so that a narrower one anywhere would not survive.
const ARCHIVE_ID: i64 = 0x0423_6483_BEEF;

/// A port for this run, so that two test processes do not take the same one.
///
/// The reference's own default is `localhost:8010` and a **concrete** port is
/// required on both sides: a publication on `endpoint=localhost:0` is refused by
/// the driver outright (`endpoint has port=0 for publication`), so the wildcard
/// port the rest of this slice's channels leave to the driver cannot be used to
/// find an archive.
fn port(offset: u32) -> u32 {
    8010 + (std::process::id() % 500) * 2 + offset
}

/// Where the reference archive is told to listen for clients, and where the
/// client sends its requests.
///
/// **UDP, and it has to be**: the reference refuses anything else outright —
/// `Archive.Context.controlChannel must be UDP media`, from `Archive.java:1225`
/// by way of `Archive$Context.conclude`. That is a fact about the archive rather
/// than about this test, and it is why the client half here is not the
/// `aeron:ipc` one every other case in this slice uses. The stream is the
/// reference's own default, 10.
fn control_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(0))
}

/// What the client asks to be answered on.
fn response_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(1))
}

/// How long the reference process may take to come up.
const DEADLINE: Duration = Duration::from_secs(30);

fn context() -> ArchiveContext {
    ArchiveContext::resolve(&[
        (CONTROL_CHANNEL_ENV.to_owned(), control_channel()),
        (CONTROL_RESPONSE_CHANNEL_ENV.to_owned(), response_channel()),
    ])
}

#[test]
fn an_archive_that_names_itself_is_answered_with_that_name() {
    let archive_dir = TempDir::new("java-archive-id");
    let control_property = format!("-Daeron.archive.control.channel={}", control_channel());

    let Some(mut media_driver) = JavaArchivingMediaDriver::start(
        "java-archive-id",
        archive_dir.path(),
        Some(ARCHIVE_ID),
        &[control_property.as_str()],
    ) else {
        java_archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the reference archiving media driver comes up");

    let mut client =
        Client::connect(media_driver.aeron_dir()).expect("connect to the reference driver");

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the archive is connected");

    assert_eq!(
        ARCHIVE_ID,
        archive.archive_id(),
        "the id the archive was started with, and not the one a client that never asked has"
    );

    archive.close(&client);
    media_driver.stop().expect("the reference process stops");
}
