//! A6: the reference's own client, adding destinations, against our driver.
//!
//! Every other interop test in this tree puts the reference on one side of a
//! *stream* — a `BasicPublisher` sending, a `BasicSubscriber` receiving. This is
//! the first that asks the reference to be a client of **our driver** and to use
//! the part of the protocol nothing shipped exercises: the destination commands.
//! No sample adds a destination to anything, so there is nothing to point at
//! that already exists — hence the probe beside this file, linked against the
//! reference's own `libaeron`.
//!
//! What it proves that no test in this build could: that `ADD_DESTINATION`
//! arriving from a client that shares no code with ours is decoded, answered,
//! and answered **successfully** — through the conductor's handler, the sender
//! proxy, and the tracker that the destination actually lands in. The unit
//! tests pin each of those pieces against its own idea of the wire; this pins
//! them against the reference's.

use std::net::UdpSocket;
use std::path::PathBuf;
use std::process::Command;

use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};

/// The stream the probe publishes on.
const STREAM_ID: i32 = 1001;

/// Where the reference checkout keeps what compiling against its client needs
/// (`docs/reference.md`).
const REFERENCE_INCLUDE: &str = "../../aeron/aeron-client/src/main/c";
const REFERENCE_LIB: &str = "../../aeron/cppbuild/Release/lib";

/// A port nothing is listening on, so the probe's destination is a real address
/// rather than a name that has to resolve.
fn free_udp_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("a socket");
    socket.local_addr().expect("a bound address").port()
}

/// Compile the probe against the reference's client library, or answer `None`
/// when the checkout is not there — which is a skip, as it is for every other
/// interop test.
fn build_probe() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source = manifest.join("fixtures/destination_probe.c");
    let include = manifest.join(REFERENCE_INCLUDE);
    let lib = manifest.join(REFERENCE_LIB);
    let output = manifest.join("../target/destination_probe");

    if !include.is_dir() || !lib.is_dir() {
        return None;
    }

    let status = Command::new("cc")
        .arg("-std=gnu11")
        .arg("-Wall")
        .arg("-Werror")
        .arg("-I")
        .arg(&include)
        .arg("-o")
        .arg(&output)
        .arg(&source)
        .arg("-L")
        .arg(&lib)
        .arg("-laeron")
        // The library is not installed, so the probe has to be told where to
        // find it at run time.
        .arg(format!("-Wl,-rpath,{}", lib.display()))
        .status()
        .expect("a C compiler");

    assert!(
        status.success(),
        "the probe must compile against {}",
        lib.display()
    );

    Some(output)
}

/// A manual channel — the category a destination can be added to at all
/// (`aeron_udp_channel_is_multi_destination`, `media/aeron_udp_channel.h:147-151`).
fn manual_channel(port: u16) -> String {
    format!("aeron:udp?endpoint=localhost:{port}|term-length=65536|control-mode=manual")
}

#[test]
fn a_reference_client_adds_a_destination_to_our_driver() {
    let Some(probe) = build_probe() else {
        driver::announce_tool_skip("the reference client library");
        return;
    };

    let Some(mut own) = OwnDriver::start("destination-probe") else {
        driver::announce_own_skip();
        return;
    };

    let aeron_dir = own.aeron_dir().to_path_buf();
    let _ = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let channel = manual_channel(free_udp_port());
    let destination = format!("aeron:udp?endpoint=localhost:{}", free_udp_port());

    let output = Command::new(&probe)
        .arg("-d")
        .arg(&aeron_dir)
        .arg("-c")
        .arg(&channel)
        .arg("-s")
        .arg(STREAM_ID.to_string())
        .arg("-D")
        .arg(&destination)
        .output()
        .expect("the probe runs");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let log = own.log_tail(60);
    let _ = own.stop();

    assert!(
        output.status.success(),
        "the reference client could not add a destination to this driver.\n\
         it said:\n{stdout}\n{stderr}\nour driver said:\n{log}"
    );

    assert!(
        stdout.contains("PUBLICATION"),
        "the publication has to exist before a destination can be added to it:\n{stdout}"
    );
    assert!(
        stdout.contains("DESTINATION"),
        "and the destination has to be answered:\n{stdout}"
    );
}

/// The probe is a program, and a program that is never run is not evidence —
/// so its own arguments are checked here too. Without this, a probe that
/// refused everything would pass the test above by never being reached.
#[test]
fn the_probe_refuses_arguments_that_leave_it_nothing_to_do() {
    let Some(probe) = build_probe() else {
        driver::announce_tool_skip("the reference client library");
        return;
    };

    let output = Command::new(&probe)
        .output()
        .expect("the probe runs without arguments");

    assert_eq!(
        Some(2),
        output.status.code(),
        "no arguments at all is a usage error"
    );
}

/// Three free ports at once, so that the three addresses below are three
/// different ones — asking one at a time can hand the same port back twice.
fn three_free_ports() -> (u16, u16, u16) {
    let sockets: Vec<UdpSocket> = (0..3)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("a socket"))
        .collect();
    let ports: Vec<u16> = sockets
        .iter()
        .map(|socket| socket.local_addr().expect("a bound address").port())
        .collect();

    (ports[0], ports[1], ports[2])
}

/// The other direction: the reference client adds a **source** to its own
/// subscription, on our driver (`aeron_subscription_async_add_destination`).
///
/// This is the receive side of a multi-destination channel, and the harder one:
/// the receiver is the side that has to speak first, because a sender that does
/// not know it exists will never describe its stream. The destination's channel
/// names a `control=` for exactly that reason — it is what makes this driver
/// open the conversation (and keep opening it).
#[test]
fn a_reference_client_adds_a_source_to_our_subscription() {
    let Some(probe) = build_probe() else {
        driver::announce_tool_skip("the reference client library");
        return;
    };

    let Some(mut own) = OwnDriver::start("destination-probe-subscribe") else {
        driver::announce_own_skip();
        return;
    };

    let aeron_dir = own.aeron_dir().to_path_buf();
    let _ = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let (channel_port, endpoint_port, control_port) = three_free_ports();
    let channel = manual_channel(channel_port);
    let destination =
        format!("aeron:udp?endpoint=localhost:{endpoint_port}|control=localhost:{control_port}");

    let output = Command::new(&probe)
        .arg("-d")
        .arg(&aeron_dir)
        .arg("-c")
        .arg(&channel)
        .arg("-s")
        .arg(STREAM_ID.to_string())
        .arg("-D")
        .arg(&destination)
        .arg("-S")
        .output()
        .expect("the probe runs");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let log = own.log_tail(60);
    let _ = own.stop();

    assert!(
        output.status.success(),
        "the reference client could not add a source to its subscription on this driver.\n\
         it said:\n{stdout}\n{stderr}\nour driver said:\n{log}"
    );

    assert!(
        stdout.contains("SUBSCRIPTION") && stdout.contains("DESTINATION"),
        "the subscription has to exist and the source has to be answered:\n{stdout}"
    );
}
