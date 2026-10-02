//! A13-b: the reference's own client, spying on our driver.
//!
//! Every other test of the spy in this tree has our client on both ends, which
//! can only ever show that this build agrees with itself. This one puts a
//! client that shares no code with ours on the read side: it publishes into our
//! driver, subscribes to its own channel so that the publication has a receiver
//! to send to, and then reads the **same** messages a second time through
//! `aeron-spy:`. What it validates is the bytes it got that way — each fragment
//! the length it published and the index it published it with, in order.
//!
//! Nothing samples exercise that path: the reference's own samples subscribe to
//! channels on the wire, and no shipped program uses the scheme at all. Hence
//! the probe beside this file, linked against the reference's `libaeron` —
//! the same arrangement as `destination_probe.rs`, and for the same reason.
//!
//! It is the one direction `tests/integration/spy_subscription.rs` cannot be:
//! that test proves the link is made and the counters are right, this one
//! proves a *foreign* client can read through it.

use std::path::PathBuf;
use std::process::Command;

use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};

/// The stream the probe publishes on.
const STREAM_ID: i32 = 1001;

/// How many messages it sends, in order, through the spy.
///
/// Enough to cross term boundaries at the term length the probe asks for, so
/// that a spy reading a *position* rather than a socket has to walk one.
const MESSAGES: i64 = 200;

/// Where the reference checkout keeps what compiling against its client needs
/// (`docs/reference.md`).
const REFERENCE_INCLUDE: &str = "../../aeron/aeron-client/src/main/c";
const REFERENCE_LIB: &str = "../../aeron/cppbuild/Release/lib";

/// A UDP port nothing is listening on.
fn free_udp_port() -> u16 {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("a socket");
    socket.local_addr().expect("a bound address").port()
}

/// Compile the probe against the reference's client library, or answer `None`
/// when the checkout is not there — which is a skip, as it is for every other
/// interop test.
///
/// **Once per process**: two `cc` runs writing one probe while a test thread is
/// `exec`ing it is `ETXTBSY`, which is what `destination_probe` was losing runs
/// to before it learned the same lesson.
fn build_probe() -> Option<PathBuf> {
    static BUILT: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

    BUILT.get_or_init(compile_probe).clone()
}

/// The compile itself, which [`build_probe`] runs once.
fn compile_probe() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source = manifest.join("fixtures/spy_probe.c");
    let include = manifest.join(REFERENCE_INCLUDE);
    let lib = manifest.join(REFERENCE_LIB);
    let output = manifest.join("../target/spy_probe");

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

#[test]
fn a_reference_client_spies_on_our_driver() {
    let Some(probe) = build_probe() else {
        driver::announce_tool_skip("the reference client library");
        return;
    };

    let Some(mut own) = OwnDriver::start("spy-reference-client") else {
        driver::announce_own_skip();
        return;
    };

    let aeron_dir = own.aeron_dir().to_path_buf();
    let _ = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // A megabyte term, where 64 KiB would do for the frames: the probe reads
    // the publication twice — once through its own subscriber, once through the
    // spy — and the publication's cleaning runs behind its slowest reader,
    // which a wire subscriber is not part of. The whole of what it publishes
    // fits inside one term here, so neither reader can have the bytes the other
    // has not read yet cleaned out from under it. See the same reasoning in
    // `tests/integration/spy_subscription.rs`.
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=1m",
        free_udp_port()
    );

    let output = Command::new(&probe)
        .arg("-d")
        .arg(&aeron_dir)
        .arg("-c")
        .arg(&channel)
        .arg("-s")
        .arg(STREAM_ID.to_string())
        .arg("-n")
        .arg(MESSAGES.to_string())
        .output()
        .expect("the probe runs");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let log = own.log_tail(60);
    let _ = own.stop();

    assert!(
        output.status.success(),
        "the reference client could not read a local publication through our driver.\n\
         it said:\n{stdout}\n{stderr}\nour driver said:\n{log}"
    );

    assert!(
        stdout.contains("READY"),
        "the spy has to be given an image before anything can be read:\n{stdout}"
    );
    assert!(
        stdout.contains(&format!("DONE {MESSAGES}")),
        "and every message has to come back through it:\n{stdout}"
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
