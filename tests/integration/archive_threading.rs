//! P2-9a: the archive's threads, read from outside the process.
//!
//! `aeron.archive.threading.mode` decides how many threads an archive runs and
//! what they are called (`ArchiveThreadingMode.java:28-38`):
//!
//! * `DEDICATED` — the conductor, the recorder and the replayer, each an agent
//!   on a thread of its own (`DedicatedModeArchiveConductor.java:54-55`);
//! * `SHARED` — one agent, driven in one turn (`SharedModeArchiveConductor.java:57-66`);
//! * `INVOKER` — that same agent, driven by the caller (`Archive.java:253-256`
//!   starts an `AgentInvoker` and no runner at all).
//!
//! The reference's own acceptance for the *names* is `ArchiveThreadNamingTest`,
//! which runs in the reference's JVM against the reference's archive and cannot
//! be used here. What stands in for it is this: read `/proc/<pid>/task/*/comm`
//! from the archive process this workspace just started — the same shape
//! `driver_process_contract.rs` uses for the driver, and the only place a
//! thread's name is visible from outside.
//!
//! # The kernel keeps fifteen bytes
//!
//! `comm` is not the name the thread was given; it is the name the kernel
//! **kept**, and it keeps fifteen bytes. The reference's classic names do not
//! fit: `archive-conductor` is seventeen bytes and `archive-recorder` is
//! sixteen, so what a reader sees is `archive-conduct` and `archive-recorde`.
//! (Fifteen bytes, and not a rounder number: `TASK_COMM_LEN` is sixteen
//! *including* the terminating null.) That is not a defect in either
//! implementation — the reference's `Thread.setName` keeps the
//! whole name and the kernel's copy is cut, exactly as it is for the Java
//! process this build is compared against. It is why the names asserted below
//! are the *truncations*: this is what an outside observer sees, which is the
//! whole point of reading `comm` rather than a name the process reports about
//! itself.
//!
//! # What is not asserted here
//!
//! ★ **`SHARED`'s thread set.** The reference starts the shared agent on a
//! thread of its own (`AgentRunner.startOnThread`, `Archive.java:262` — the same
//! call `DEDICATED` uses for its conductor), so its `SHARED` archive has one
//! thread named `archive-conductor` under the classic naming. This build's
//! `SHARED` runs the shared agent on the **caller's** thread, which is where the
//! reference puts `INVOKER` — a difference of topology and not of work, since a
//! process whose archive is the only thing it does has nothing else for the
//! caller to be doing. Both are green here and neither is claimed; the naming
//! *table* is pinned cell by cell in
//! `crates/archive/src/server/threading.rs::every_role_has_the_references_name_under_both_namings`,
//! and the topology is what P2-9a's remaining steps are about.

use std::time::{Duration, Instant};

use deepmsg_tests::archive::{self, DEADLINE};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// How long the archive is given to have all of its agents running.
///
/// The archive signals its mark file ready before its runner starts its
/// workers — the reference's `signalReady` is at the end of `conclude`
/// (`Archive.java:1665`) and the runners start in `onStart`, afterwards — so a
/// reading taken the instant `await_ready` returns can legitimately be missing
/// one.
const THREAD_TIMEOUT: Duration = Duration::from_secs(10);

/// `aeron.thread.naming=new` (`CommonContext.java:534-566`), which is the
/// archive's as much as the driver's.
const NEW_NAMING: &str = "-Daeron.thread.naming=new";

#[test]
fn a_dedicated_archive_runs_the_references_agents_by_their_classic_names() {
    let names = threads_of_a_dedicated_archive("classic", &[]);

    // The reference's three agents, by the names the kernel kept.
    for name in ["archive-conduct", "archive-recorde", "archive-replaye"] {
        assert!(
            names.iter().any(|thread| thread == name),
            "{name} is not among {names:?}"
        );
    }
}

#[test]
fn a_dedicated_archive_runs_the_references_agents_by_aeron_thread_naming() {
    let names = threads_of_a_dedicated_archive("new", &[NEW_NAMING]);

    // The short spellings fit inside fifteen bytes, so these are the whole
    // names rather than truncations.
    for name in ["aeron-ar-cnd", "aeron-ar-rec", "aeron-ar-rep"] {
        assert!(
            names.iter().any(|thread| thread == name),
            "{name} is not among {names:?}"
        );
    }
}

/// Every thread the archive's process is running, by the name the kernel holds
/// for it, once the reference's three agents are all there.
///
/// All three, and not just one: the assertion is about a *set*, and a reading
/// taken before the third thread starts would fail for a reason that has nothing
/// to do with what is being asserted.
fn threads_of_a_dedicated_archive(naming: &str, extra: &[&str]) -> Vec<String> {
    let archive_dir = TempDir::new(&format!("archive-threads-{naming}"));

    let mut properties: Vec<&str> = archive::PROPERTIES.to_vec();
    properties.push("-Daeron.archive.threading.mode=DEDICATED");
    properties.extend_from_slice(extra);

    let Some(mut media_driver) = OwnArchivingMediaDriver::start(
        &format!("archive-threads-{naming}"),
        archive_dir.path(),
        &properties,
    ) else {
        archiving_driver::announce_skip();
        return Vec::new();
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    let deadline = Instant::now() + THREAD_TIMEOUT;
    let mut names = threads(media_driver.pid());

    while Instant::now() < deadline && !has_both(&names, naming) {
        std::thread::sleep(Duration::from_millis(5));
        names = threads(media_driver.pid());
    }

    let _ = media_driver.stop();
    names
}

/// Whether both of the agents this slice adds are running, under `naming`.
fn has_both(names: &[String], naming: &str) -> bool {
    let expected: [&str; 3] = if "new" == naming {
        ["aeron-ar-cnd", "aeron-ar-rec", "aeron-ar-rep"]
    } else {
        ["archive-conduct", "archive-recorde", "archive-replaye"]
    };

    expected
        .iter()
        .all(|agent| names.iter().any(|name| name == agent))
}

/// Every thread the process is running, by the name the kernel holds for it.
///
/// `/proc/<pid>/task/*/comm` is the only place a thread's name is visible from
/// outside the process, and it is truncated to fifteen characters — see the
/// module note for why the truncations are the answers rather than a nuisance.
fn threads(pid: u32) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(format!("/proc/{pid}/task"))
        .expect("the archive's own task directory")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            std::fs::read_to_string(entry.path().join("comm")).ok()
        })
        .map(|name| name.trim().to_owned())
        .collect();

    names.sort();
    names
}
