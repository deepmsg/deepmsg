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
//! # `SHARED`'s thread set, and `INVOKER`'s answer
//!
//! ★ **`SHARED` starts no thread of its own.** The reference starts the shared
//! agent on a thread of its own (`AgentRunner.startOnThread`, `Archive.java:262`
//! — the same call `DEDICATED` uses for its conductor), so its `SHARED` archive
//! has one thread named `archive-conductor` under the classic naming. This
//! build's `SHARED` runs the shared agent on the **caller's** thread, which is
//! where the reference puts `INVOKER` — a difference of topology and not of
//! work, since a process whose archive is the only thing it does has nothing
//! else for the caller to be doing. It is a decision rather than an accident, so
//! it is pinned rather than left to drift:
//! `a_shared_archive_starts_no_agent_thread_of_its_own`. The naming *table* is
//! pinned cell by cell in
//! `crates/archive/src/server/threading.rs::every_role_has_the_references_name_under_both_namings`.
//!
//! ★ **`INVOKER` is refused, by both binaries that can run an archive, in the
//! same words.** Neither has a caller to drive the agent — `launch::run` is the
//! loop — so the mode is answered by name rather than by quietly running
//! something else: `both_binaries_that_run_the_archive_refuse_invoker_by_name`.
//! This is where the two used to disagree, the standalone one refusing and the
//! archiving media driver running `INVOKER` as `SHARED` without saying so.

use std::time::{Duration, Instant};

use deepmsg_tests::archive::{self, DEADLINE};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::standalone_archive;
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

/// How long a binary asked for `INVOKER` is given to refuse it.
///
/// A refusal is answered before anything is served — milliseconds — so this is
/// only ever reached by a binary that accepted the mode and began serving.
const INVOKER_PATIENCE: Duration = Duration::from_secs(5);

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

/// Every name the archive's agents can carry, under either naming, as the
/// kernel keeps them: `server/threading.rs`'s table with `comm`'s fifteen bytes.
const AGENT_NAMES: [&str; 6] = [
    "archive-conduct",
    "archive-recorde",
    "archive-replaye",
    "aeron-ar-cnd",
    "aeron-ar-rec",
    "aeron-ar-rep",
];

/// A `SHARED` archive starts no agent thread at all — this build's one
/// deliberate difference of topology from the reference's `SHARED`.
#[test]
fn a_shared_archive_starts_no_agent_thread_of_its_own() {
    let archive_dir = TempDir::new("archive-threads-shared");

    // `archive::PROPERTIES` names `SHARED` for the driver and the archive both,
    // which is the shape the rest of this suite runs; this reads what that shape
    // looks like from outside the process.
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("archive-threads-shared", archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    // An absence needs a window rather than one reading: a thread that were
    // going to start has started by the time the mark file is ready (the archive
    // signals ready before its runner starts — see `THREAD_TIMEOUT`), so this
    // samples for as long as the presence above would have been given.
    let deadline = Instant::now() + THREAD_TIMEOUT;
    loop {
        let names = threads(media_driver.pid());
        let agents: Vec<&String> = names
            .iter()
            .filter(|name| AGENT_NAMES.contains(&name.as_str()))
            .collect();
        assert!(
            agents.is_empty(),
            "a SHARED archive ran agents on threads of its own: {agents:?} of {names:?}"
        );

        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = media_driver.stop();
}

/// Each agent idles with the strategy **it** was named, and with the archive's
/// when it was named nothing.
///
/// An idle strategy is invisible from outside the process any other way, so this
/// reads each thread's own CPU time. The archive's own is `yield`, which never
/// stops, and the agent under test is given `sleep-ms`, which parks for a
/// millisecond at a time (`SleepingMillisIdleStrategy.DEFAULT_SLEEP_PERIOD_MS`).
/// So the agent that was named parks, the one that was not spins with the
/// archive, and a wiring that handed the agent the archive's strategy instead of
/// its own shows up as a second spinner — which is what this is here to catch.
#[test]
fn each_agent_idles_with_the_strategy_it_was_named_and_not_the_archives() {
    for named in ["recorder", "replayer"] {
        let Some((named_ticks, other_ticks, conductor_ticks)) = idle_ticks_of(named) else {
            archiving_driver::announce_skip();
            return;
        };

        // A margin of four, because this is a difference of two samples taken
        // under whatever else the machine is doing. What it separates is a thread
        // that sleeps a millisecond at a time from one that never stops —
        // measured, nearer a hundred.
        assert!(
            named_ticks * 4 < conductor_ticks,
            "the {named} is not idling with its own `sleep-ms`: {named_ticks} ticks over 500 ms \
             against the conductor's {conductor_ticks}"
        );
        assert!(
            other_ticks * 4 > conductor_ticks,
            "the agent that named nothing is not idling as the archive does: {other_ticks} ticks \
             against the conductor's {conductor_ticks}"
        );
    }
}

/// A 500 ms reading of the three agent threads' own CPU time, with the archive's
/// strategy `yield` and `named`'s own `sleep-ms` — or [`None`] when the
/// archiving media driver is not built.
///
/// Returns `(named, the other agent, the conductor)`. The conductor is the
/// control rather than a fourth subject: it is the thread the archive's own
/// strategy is always left to.
fn idle_ticks_of(named: &str) -> Option<(u64, u64, u64)> {
    let archive_dir = TempDir::new(&format!("archive-threads-{named}-idle"));

    let mut properties: Vec<&str> = archive::PROPERTIES.to_vec();
    properties.push("-Daeron.archive.threading.mode=DEDICATED");
    properties.push("-Daeron.archive.idle.strategy=yield");
    let own = format!("-Daeron.archive.{named}.idle.strategy=sleep-ms");
    properties.push(&own);

    let test_name = format!("archive-threads-{named}-idle");
    let mut media_driver =
        OwnArchivingMediaDriver::start(&test_name, archive_dir.path(), &properties)?;

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    let deadline = Instant::now() + THREAD_TIMEOUT;
    while Instant::now() < deadline && !has_both(&threads(media_driver.pid()), "classic") {
        std::thread::sleep(Duration::from_millis(5));
    }

    // The three roles under the classic naming, each cut to the fifteen bytes
    // `comm` keeps (`AGENT_NAMES`).
    let in_proc = |role: &str| format!("archive-{}", &role[..7]);
    let other = if "recorder" == named {
        "replayer"
    } else {
        "recorder"
    };

    let before = cpu_ticks(media_driver.pid());
    std::thread::sleep(Duration::from_millis(500));
    let after = cpu_ticks(media_driver.pid());

    for role in [named, other, "conductor"] {
        let thread = in_proc(role);
        assert!(
            before.contains_key(&thread) && after.contains_key(&thread),
            "{thread} is not among the archive's threads: {:?}",
            after.keys().collect::<Vec<&String>>()
        );
    }

    let spent = |role: &str| after[&in_proc(role)] - before[&in_proc(role)];
    let ticks = (spent(named), spent(other), spent("conductor"));

    let _ = media_driver.stop();
    Some(ticks)
}

/// Every thread's own CPU time — `utime + stime`, in clock ticks — by the name
/// the kernel keeps for it.
///
/// `/proc/<pid>/task/<tid>/stat` carries the name as its second field, in
/// parentheses and possibly holding spaces of its own, so the fields are read
/// from after the **last** `)`: `state` is the first of the remainder, which puts
/// `utime` and `stime` — fields 14 and 15 — at 11 and 12 of it.
fn cpu_ticks(pid: u32) -> std::collections::HashMap<String, u64> {
    let mut ticks: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

    for entry in std::fs::read_dir(format!("/proc/{pid}/task")).expect("the archive's tasks") {
        let stat = std::fs::read_to_string(entry.expect("a task directory").path().join("stat"))
            .expect("a task's stat");

        let (head, rest) = stat
            .rsplit_once(')')
            .expect("the comm field is parenthesised");
        let name = head
            .split_once('(')
            .expect("the comm field is parenthesised")
            .1
            .to_owned();

        let fields: Vec<&str> = rest.split_whitespace().collect();
        let user: u64 = fields[11].parse().expect("utime");
        let system: u64 = fields[12].parse().expect("stime");

        *ticks.entry(name).or_insert(0) += user + system;
    }

    ticks
}

/// `INVOKER` is refused by name — by **both** binaries that can run an archive,
/// and in the same words.
///
/// The reference's `INVOKER` means the *caller* drives the archive's agent
/// (`Archive.launch:253-256` starts an `AgentInvoker` and no runner), and neither
/// of these binaries has a caller: `launch::run` is the loop. A build where one
/// of them refused while the other ran `INVOKER` as `SHARED` would be one no
/// configuration file could be reasoned about from — the same property would
/// serve one deployment and fail another.
#[test]
fn both_binaries_that_run_the_archive_refuse_invoker_by_name() {
    let binaries = [
        (
            "deepmsg-archive",
            standalone_archive::locate_own_standalone_archive(),
        ),
        (
            "deepmsg-archiving-media-driver",
            archiving_driver::locate_own_archiving_media_driver(),
        ),
    ];

    let mut refusals: Vec<(&str, String)> = Vec::new();
    for (name, binary) in binaries {
        let Some(binary) = binary else {
            continue;
        };

        // A configuration the *parser* accepts, with only the mode changed:
        // `ArchiveConfig::from_args` validates what it is required to have (the
        // control channel among it) before `main` reaches the mode, so an argv
        // that failed parsing would answer with the parser's complaint instead
        // of the refusal this is about.
        let mut properties: Vec<String> = archive::PROPERTIES
            .iter()
            .map(|property| (*property).to_owned())
            .collect();
        let mut named_the_mode = false;
        for property in &mut properties {
            if property.starts_with("-Daeron.archive.threading.mode=") {
                *property = "-Daeron.archive.threading.mode=INVOKER".to_owned();
                named_the_mode = true;
            }
        }
        assert!(
            named_the_mode,
            "`archive::PROPERTIES` no longer names the archive's threading mode, so this test \
             would be asking for the default instead of for INVOKER"
        );

        let aeron_dir = TempDir::new(&format!("archive-invoker-{name}"));
        let archive_dir = TempDir::new(&format!("archive-invoker-{name}-data"));
        let mut child = std::process::Command::new(&binary)
            .args(&properties)
            // The archiving media driver checks the *driver's* mode first, so
            // that half has to be one it runs — otherwise this would pass on a
            // refusal about the driver and never reach the archive's.
            .arg("-Daeron.threading.mode=SHARED")
            .arg(format!("-Daeron.dir={}", aeron_dir.path().display()))
            // Named as well, and this one is not about the configuration being
            // complete: a binary that *accepted* the mode — which is what the
            // mutation this asserts against does — would otherwise write its
            // catalog and mark file to a directory of the default name beside
            // the test binary. Measured: it did.
            .arg(format!(
                "-Daeron.archive.dir={}",
                archive_dir.path().display()
            ))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("the archive binary must be runnable");

        // Bounded, which `.output()` is not: a binary that *accepted* `INVOKER`
        // would be serving and would never exit on its own, so the mutation this
        // assertion is checked against would hang the test instead of failing
        // it. Measured: it does (5 s and counting, on the first run of this).
        let deadline = Instant::now() + INVOKER_PATIENCE;
        let status = loop {
            if let Some(status) = child.try_wait().expect("the child's status") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "{name} accepted INVOKER instead of refusing it: still running after {} ms",
                    INVOKER_PATIENCE.as_millis()
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        };

        let output = child.wait_with_output().expect("the child's output");
        assert!(
            !status.success(),
            "{name} accepted INVOKER instead of refusing it: {:?}",
            String::from_utf8_lossy(&output.stdout)
        );
        refusals.push((name, String::from_utf8_lossy(&output.stderr).into_owned()));
    }

    if refusals.is_empty() {
        standalone_archive::announce_skip();
        return;
    }

    for (name, stderr) in &refusals {
        assert!(
            stderr.contains("aeron.archive.threading.mode is INVOKER"),
            "{name} refused for some other reason: {stderr}"
        );
    }

    // Everything but the binary's own name, which is the one thing that is
    // supposed to differ between the two.
    let sentence = |stderr: &str| {
        stderr
            .split_once(": ")
            .map(|(_, rest)| rest.trim().to_owned())
            .unwrap_or_default()
    };

    let (first_name, first) = &refusals[0];
    for (name, stderr) in &refusals[1..] {
        assert_eq!(
            sentence(first),
            sentence(stderr),
            "{first_name} and {name} refuse INVOKER in different words"
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
