//! T3: the crate's own criteria (ADR-0006, slice D3).
//!
//! Every case that wants an EAL runs in a **process of its own**, because
//! `rte_eal_init` is once per process and `rte_eal_cleanup` does not undo it
//! (D0). The child is this test binary itself — re-entered with
//! `--ignored --exact <helper>` — and not a binary under `target/debug`,
//! because `cargo test` does not rebuild those: the same trap D1 hit, where a
//! stale binary makes a criterion pass while testing something else.
//!
//! A filtered test binary exits 0 with nothing run, so the parent insists on
//! seeing its child report exactly one passing test. A case that quietly stops
//! running is the one failure mode this file must not have.

#![cfg(feature = "dpdk")]

use std::process::Command;

use deepmsg_dpdk::{Eal, RegisteredThread, restore_affinity, save_affinity};

/// The EAL arguments every case here starts with.
///
/// `--no-huge` because no machine in this slice's CI needs huge pages for a
/// virtual device, and `--no-shconf` because `--no-huge` implies the legacy
/// memory layout, which is incompatible with `--in-memory` (D0 measured both).
/// `--no-telemetry` keeps EAL's telemetry thread out of the process.
const EAL_ARGS: [&str; 4] = [
    "deepmsg-dpdk-test",
    "--no-huge",
    "--no-shconf",
    "--no-telemetry",
];

/// Run one case in a child process of this same test binary.
fn in_a_child(helper: &str) {
    let exe = std::env::current_exe().expect("the test binary's own path");
    let output = Command::new(&exe)
        .args([
            "--exact",
            helper,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .output()
        .unwrap_or_else(|error| panic!("running {}: {error}", exe.display()));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "the child case `{helper}` failed ({}):\n{stdout}\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains("1 passed"),
        "the child case `{helper}` did not run. A filtered test binary exits 0 \
         with nothing run, so this is the one way a criterion could pass by not \
         being looked at:\n{stdout}\n{stderr}"
    );
}

/// EAL starts once, and the process's second `init` is the first EAL.
///
/// Runs `eal_init_is_once_per_process_in_the_child` in a child process.
#[test]
fn eal_init_is_once_per_process() {
    in_a_child("eal_init_is_once_per_process_in_the_child");
}

/// The body of the case above; not itself a criterion when run here.
#[test]
#[ignore = "runs as a child of `eal_init_is_once_per_process`: one EAL per process"]
fn eal_init_is_once_per_process_in_the_child() {
    let eal = Eal::init(EAL_ARGS).expect("EAL starts");

    let again = Eal::init(EAL_ARGS).expect("a second init answers the first");
    assert!(
        std::ptr::eq(eal, again),
        "the second `init` must answer the EAL the first one started, not start another"
    );

    // A `std::thread` EAL never started: no lcore id until it registers, and
    // the id it gets is one of EAL's (R7 — the id is what turns the pool's
    // per-lcore cache on for that thread).
    std::thread::spawn(|| {
        assert_eq!(Eal::lcore_id(), None, "a fresh std::thread has no lcore id");
        let registered = RegisteredThread::register().expect("a std::thread can register");
        let id = registered.lcore_id();
        assert_eq!(
            Eal::lcore_id(),
            Some(id),
            "the registered thread knows its lcore"
        );
        assert!(
            id < Eal::lcore_count(),
            "lcore {id} is one of EAL's {} lcores",
            Eal::lcore_count()
        );
    })
    .join()
    .expect("the thread joins");

    save_affinity().expect("the affinity can be saved");
    restore_affinity().expect("and put back");

    Eal::cleanup().expect("EAL shuts down");

    // Cleanup is final: there is no second EAL in this process.
    let refused = Eal::init(EAL_ARGS).expect_err("after cleanup, init must be refused");
    assert!(
        refused.to_string().contains("cleaned up"),
        "the refusal says why: {refused}"
    );
}
