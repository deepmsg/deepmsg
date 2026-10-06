//! The one question P2-1c starts from: does a channel carrying
//! `response-correlation-id` still reach a plain `aeron:ipc` subscription?
//!
//! P2-0c measured that both of the reference's suites fail on
//! `aeron:ipc?control-mode=response` and pass on every UDP variant of the same
//! case, and that they agree — a Java client with the archive in-process and a
//! C client with the archive in a process of its own. What the client library
//! does there is small and readable (`AeronArchive.java:4043-4048`): when the
//! response channel is a response channel it rewrites the **request** channel
//! `aeron:ipc` into `aeron:ipc?response-correlation-id=<response subscription's
//! registration id>`, and the driver pairs the two ends by that id
//! (`aeron_driver_conductor.c:1714-1729`).
//!
//! So the request publication that has to reach the archive's plain `aeron:ipc`
//! subscription carries one parameter a plain channel does not carry. If this
//! driver counts that parameter as part of the channel's identity, the two
//! never meet and the ConnectRequest is not delivered — which is what a connect
//! timeout looks like from the client's side.
//!
//! The probe beside this test asks that of whichever driver it is pointed at,
//! with a plain channel as its own control. **What this test asserts is the
//! control and the agreement**, and what it *prints* is the whole reading: a
//! delivery on its own is not a verdict, the difference between the two drivers
//! is.

use std::path::{Path, PathBuf};
use std::process::Command;

use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT, ReferenceDriver};

/// Where the reference checkout keeps what compiling against its client needs
/// (`docs/reference.md`).
const REFERENCE_INCLUDE: &str = "../../aeron/aeron-client/src/main/c";
const REFERENCE_LIB: &str = "../../aeron/cppbuild/Release/lib";

/// Compile the probe against the reference's client library, or answer `None`
/// when the checkout is not there — which is a skip, as it is for every other
/// interop test. Once per process, for the reason `spy_reference.rs` records:
/// two `cc` runs writing one probe while a test thread `exec`s it is `ETXTBSY`.
fn build_probe() -> Option<PathBuf> {
    static BUILT: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

    BUILT.get_or_init(compile_probe).clone()
}

fn compile_probe() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source = manifest.join("fixtures/ipc_response_probe.c");
    let include = manifest.join(REFERENCE_INCLUDE);
    let lib = manifest.join(REFERENCE_LIB);
    let output = manifest.join("../target/ipc_response_probe");

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

/// The probe's `CASE` lines, one per case, in the order it ran them.
fn readings(probe: &Path, aeron_dir: &Path) -> Vec<String> {
    let output = Command::new(probe)
        .arg("-d")
        .arg(aeron_dir)
        .output()
        .expect("the probe must run");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "the probe must produce readings; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let cases: Vec<String> = stdout
        .lines()
        .filter(|line| line.starts_with("CASE "))
        .map(str::to_owned)
        .collect();

    assert!(!cases.is_empty(), "the probe printed no cases:\n{stdout}");

    cases
}

/// Whether the case delivered anything at all.
///
/// The count, not the string `delivered=1`: the probe's question is whether a
/// link is made, and a reading that happened to be 2 is a yes. Reading it as an
/// equality made this test fail one run in three on the *control* case, which
/// is a test that fails for a reason that has nothing to do with the driver.
fn delivered(case: &str) -> bool {
    case.rsplit_once("delivered=")
        .and_then(|(_, count)| count.trim().parse::<i64>().ok())
        .is_some_and(|count| count > 0)
}

/// The index of the plain case, which is the control every other case is read
/// against.
const PLAIN: usize = 0;

#[test]
fn both_drivers_agree_on_what_a_response_channel_reaches() {
    let Some(probe) = build_probe() else {
        driver::announce_tool_skip("the reference client library");
        return;
    };

    let Some(mut own) = OwnDriver::start("ipc-response-own") else {
        driver::announce_own_skip();
        return;
    };

    let _ = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");
    let ours = readings(&probe, own.aeron_dir());

    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };
    let mut reference =
        ReferenceDriver::start_with(&binary, "ipc-response-reference", &[]).expect("start driver");
    let _ = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");
    let theirs = readings(&probe, reference.aeron_dir());

    println!("--- reference driver ---");
    for case in &theirs {
        println!("{case}");
    }
    println!("--- this driver ---");
    for case in &ours {
        println!("{case}");
    }

    // The control: a plain channel into a plain channel is what the rest of the
    // tree already covers, and if it did not deliver on both, the probe would
    // be measuring itself rather than the driver.
    assert!(
        delivered(&theirs[PLAIN]),
        "the control case must deliver under the reference driver: {}",
        theirs[PLAIN]
    );
    assert!(
        delivered(&ours[PLAIN]),
        "the control case must deliver under this driver: {}",
        ours[PLAIN]
    );

    let agree: Vec<(&String, &String)> = theirs
        .iter()
        .zip(ours.iter())
        .filter(|(theirs, ours)| delivered(theirs) != delivered(ours))
        .collect();

    assert!(
        agree.is_empty(),
        "the two drivers disagree about {} case(s): {:#?}",
        agree.len(),
        agree
    );

    let _ = own.stop();
}
